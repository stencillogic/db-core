/// Write data blocks from the buffer to database file


use crate::common::errors::Error;
use crate::common::defs::Sequence;
use crate::common::defs::BlockId;
use crate::common::intercom::SyncNotification;
use crate::storage::datastore::FileDesc;
use crate::buf_mgr::buf_mgr::BlockType;
use crate::block_mgr::block_mgr::BlockMgr;
use crate::block_mgr::block::BasicBlock;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::thread::JoinHandle;
use std::time::Duration;
use log::error;


const CONDVAR_WAIT_INTERVAL_MS: u64 = 1000;


#[derive(Clone)]
pub struct BufWriter {
    writer_threads: Arc<(Vec<JoinHandle<()>>, Arc<AtomicBool>)>,
    write_ready:    SyncNotification<usize>,
}


impl BufWriter {

    pub fn new(block_mgr: &BlockMgr, writer_num: usize) -> Result<Self, Error> {
        let terminate = Arc::new(AtomicBool::new(false));
        let mut writer_threads = Vec::with_capacity(writer_num);
        let write_ready = SyncNotification::new(0);

        for _ in 0..writer_num {
            let terminate2 = terminate.clone();

            let block_mgr2 = block_mgr.clone()?;

            let write_ready2 = write_ready.clone();

            let handle = std::thread::spawn(move || {
                Self::writer_thread(block_mgr2, terminate2, write_ready2);
            });

            writer_threads.push(handle);
        }

        Ok(BufWriter {
            writer_threads: Arc::new((writer_threads, terminate)),
            write_ready,
        })
    }

    pub fn wake_writers(&self) {
        self.write_ready.send(self.writer_threads.0.len(), true);
    }

    pub fn terminate(self) {
        if let Ok((mut writer_threads, terminate)) = Arc::try_unwrap(self.writer_threads) {
            terminate.store(true, Ordering::Relaxed);
            for jh in writer_threads.drain(..) {
                jh.join().unwrap();
            }
        }
    }

    fn writer_thread(block_mgr: BlockMgr, terminate: Arc<AtomicBool>, write_ready: SyncNotification<usize>) {

        let mut chkpnt_allocators = [CheckpointStoreBlockAllocator::new(&block_mgr), CheckpointStoreBlockAllocator::new(&block_mgr)];
        loop {
            if let Some(mut lock) = write_ready.wait_for_interruptable(
                &mut (|count| -> bool { *count > 0 }),
                &mut (|| -> bool { terminate.load(Ordering::Relaxed) }),
                Duration::from_millis(CONDVAR_WAIT_INTERVAL_MS)
            ) {
                if *lock > 0 {
                    *lock -= 1;
                    drop(lock);

                    if let Err(e) = Self::write_blocks(&block_mgr, &mut chkpnt_allocators) {
                        error!("Failed to perform block write: {}", e);
                    }
                }
            } else {
                break;
            }
        }
    }

    fn write_blocks(block_mgr: &BlockMgr, chkpnt_allocators: &mut [CheckpointStoreBlockAllocator]) -> Result<(), Error> {

        let mut iter = block_mgr.get_iter();

        while let Some(desc) = iter.next() {
            if desc.dirty && desc.block_type != BlockType::CheckpointBlock {
                if let Some(mut block) = block_mgr.get_block_by_idx(desc.id) {
                    let desc = block_mgr.get_block_desc(block.get_buf_idx()).unwrap();
                    match desc.block_type {
                        BlockType::DataBlock => {
                            if !desc.checkpoint_written {
                                let mut checkpoint_block = block_mgr.get_block_mut(&desc.checkpoint_block_id)?;
                                let checkpoint_csn = checkpoint_block.get_checkpoint_csn();
                                let allocator_id = checkpoint_csn as usize & 0x1;
                                let actual_block_id = chkpnt_allocators[allocator_id].get_next_block_id(checkpoint_csn);
                                checkpoint_block.set_id(actual_block_id);
                                block_mgr.write_block(&mut checkpoint_block)?;
                                block_mgr.set_checkpoint_written(desc.id, true);
                            }
                        },
                        _ => {},
                    }
                    block_mgr.write_block(&mut block)?;
                    block_mgr.set_dirty(desc.id, false);
                }
            }
        }

        Ok(())
    }
}


struct AllocatorState {
    file_info:              Vec<FileDesc>,
    parity:                 u64,
}


/// Block allocator determines to which block on disk in the checkpoint datastore a block 
/// from the buffer can be written.
#[derive(Clone)]
struct CheckpointStoreBlockAllocator<'a> {
    seqn:                   Sequence,
    lock:                   Arc<RwLock<AllocatorState>>,
    block_mgr:              &'a BlockMgr,
    cur_checkpoint_csn:     Arc<AtomicU64>,
}

impl<'a> CheckpointStoreBlockAllocator<'a> {

    pub fn new(block_mgr: &'a BlockMgr) -> Self {
        let seqn = Sequence::new(0);
        let file_info = Vec::<FileDesc>::new();
        let parity = 0;

        let allocator_state = AllocatorState {
            file_info,
            parity,
        };
        let lock = Arc::new(RwLock::new(allocator_state));

        let cur_checkpoint_csn = Arc::new(AtomicU64::new(0));

        CheckpointStoreBlockAllocator {
            seqn,
            lock,
            block_mgr,
            cur_checkpoint_csn,
        }
    }

    /// Return next block id for writing block to checkpoint store.
    pub fn get_next_block_id(&mut self, checkpoint_csn: u64) -> BlockId {

        if self.cur_checkpoint_csn.load(Ordering::Relaxed) != checkpoint_csn {
            self.next_checkpoint(checkpoint_csn);
        }

        let sl = self.lock.read().unwrap();

        let n = self.seqn.get_next();
        let file_info_id = (n % sl.file_info.len() as u64) as u16;
        let file_id = sl.file_info[file_info_id as usize].file_id;
        let n = n / sl.file_info.len() as u64;
        let extent_size = sl.file_info[file_info_id as usize].extent_size as u64;
        let extent_id = (n / extent_size + sl.parity) as u16;
        let block_id = (n % extent_size as u64) as u16;

        drop(sl);

        BlockId {
            file_id,
            extent_id,
            block_id,
        }
    }

    // The function is called when new checkpoint is created.
    fn next_checkpoint(&mut self, checkpoint_csn: u64) {
        let mut xl = self.lock.write().unwrap();

        if self.cur_checkpoint_csn.load(Ordering::Relaxed) != checkpoint_csn {
            xl.parity = checkpoint_csn & 0x1;

            self.seqn.set(0);

            self.block_mgr.get_checkpoint_files(&mut xl.file_info);

            self.cur_checkpoint_csn.store(checkpoint_csn, Ordering::Relaxed);
        }
    }
}

