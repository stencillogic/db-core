/// Write data blocks from the buffer to database file


use crate::common::errors::Error;
use crate::common::defs::Sequence;
use crate::common::defs::BlockId;
use crate::common::intercom::SyncNotification;
use crate::storage::datastore::FileDesc;
use crate::buf_mgr::buf_mgr::BlockType;
use crate::block_mgr::block_mgr::BlockMgr;
use crate::block_mgr::block::BasicBlock;
use crate::block_mgr::block::BlockLockedMut;
use crate::block_mgr::block::DataBlock;
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
    writer_threads:     Arc<(Vec<JoinHandle<()>>, Arc<AtomicBool>)>,
    write_ready:        SyncNotification<usize>,
    chkpnt_allocators:  CheckpointStoreBlockAllocators,
}


impl BufWriter {

    pub fn new(block_mgr: &BlockMgr, writer_num: usize) -> Result<Self, Error> {
        let terminate = Arc::new(AtomicBool::new(false));
        let mut writer_threads = Vec::with_capacity(writer_num);
        let write_ready = SyncNotification::new(0);
        let chkpnt_allocators = CheckpointStoreBlockAllocators::new();

        for _ in 0..writer_num {
            let terminate2 = terminate.clone();

            let block_mgr2 = block_mgr.clone()?;

            let write_ready2 = write_ready.clone();

            let chkpnt_allocators2 = chkpnt_allocators.clone();

            let handle = std::thread::spawn(move || {
                Self::writer_thread(block_mgr2, terminate2, write_ready2, chkpnt_allocators2);
            });

            writer_threads.push(handle);
        }

        Ok(BufWriter {
            writer_threads: Arc::new((writer_threads, terminate)),
            write_ready,
            chkpnt_allocators,
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

    /// Write just one data block in current thread.
    /// Check if block dirty and write it. If block has not written checkpoint copy the checkpoint
    /// copy must be written to disk first.
    pub fn write_data_block(&self, block: &mut BlockLockedMut<DataBlock>, block_mgr: &BlockMgr, leave_dirty: bool) -> Result<(), Error>  {
        Self::write_block(block, block_mgr, &self.chkpnt_allocators, leave_dirty)
    }

    fn writer_thread(block_mgr: BlockMgr, terminate: Arc<AtomicBool>, write_ready: SyncNotification<usize>, mut chkpnt_allocators: CheckpointStoreBlockAllocators) {

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

    fn write_blocks(block_mgr: &BlockMgr, chkpnt_allocators: &CheckpointStoreBlockAllocators) -> Result<(), Error> {

        let mut iter = block_mgr.get_iter();

        while let Some(desc) = iter.next() {
            if desc.dirty && desc.block_type != BlockType::CheckpointBlock {
                if let Some(mut block) = block_mgr.get_block_by_idx(desc.id) {
                    Self::write_block(&mut block, block_mgr, chkpnt_allocators, false)?;
                }
            }
        }

        Ok(())
    }

    fn write_block(mut block: &mut BlockLockedMut<DataBlock>, block_mgr: &BlockMgr, chkpnt_allocators: &CheckpointStoreBlockAllocators, leave_dirty: bool) -> Result<(), Error>  {

        let desc = block_mgr.get_block_desc(block.get_buf_idx()).unwrap();

        if desc.dirty {
            if desc.block_type == BlockType::DataBlock {
                if !desc.checkpoint_written {
                    let mut checkpoint_block = block_mgr.get_block_mut(&desc.checkpoint_block_id)?;
                    let checkpoint_csn = checkpoint_block.get_checkpoint_csn();
                    let actual_block_id = chkpnt_allocators.get_next_block_id(checkpoint_csn, block_mgr);
                    checkpoint_block.set_id(actual_block_id);
                    block_mgr.write_block(&mut checkpoint_block)?;
                    block_mgr.set_checkpoint_written(desc.id, true);
                }
                block_mgr.write_block(&mut block)?;
                if !leave_dirty {
                    block_mgr.set_dirty(desc.id, false);
                }
            } else if desc.block_type == BlockType::VersionBlock {
                block_mgr.write_block(&mut block)?;
                if !leave_dirty {
                    block_mgr.set_dirty(desc.id, false);
                }
            }
        }

        Ok(())
    }
}


/// Array of two allocators. 
#[derive(Clone)]
struct CheckpointStoreBlockAllocators {
    allocators: [CheckpointStoreBlockAllocator;2],
}

impl CheckpointStoreBlockAllocators {
    pub fn new() -> Self {
        CheckpointStoreBlockAllocators {
            allocators: [CheckpointStoreBlockAllocator::new(), CheckpointStoreBlockAllocator::new()],
        }
    }

    /// Return next block id for writing block to checkpoint store.
    pub fn get_next_block_id(&self, checkpoint_csn: u64, block_mgr: &BlockMgr) -> BlockId {
        let allocator_id = checkpoint_csn as usize & 0x1;
        self.allocators[allocator_id].get_next_block_id(checkpoint_csn, &block_mgr)
    }
}


struct AllocatorState {
    file_info:              Vec<FileDesc>,
    parity:                 u64,
}


/// Block allocator determines to which block on disk in the checkpoint datastore a block 
/// from the buffer can be written.
#[derive(Clone)]
struct CheckpointStoreBlockAllocator {
    seqn:                   Sequence,
    lock:                   Arc<RwLock<AllocatorState>>,
    cur_checkpoint_csn:     Arc<AtomicU64>,
}

impl CheckpointStoreBlockAllocator {

    pub fn new() -> Self {
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
            cur_checkpoint_csn,
        }
    }

    /// Return next block id for writing block to checkpoint store.
    pub fn get_next_block_id(&self, checkpoint_csn: u64, block_mgr: &BlockMgr) -> BlockId {

        if self.cur_checkpoint_csn.load(Ordering::Relaxed) != checkpoint_csn {
            self.next_checkpoint(checkpoint_csn, block_mgr);
        }

        let sl = self.lock.read().unwrap();

        let n = self.seqn.get_next();
        let file_info_id = (n % sl.file_info.len() as u64) as u16;
        let file_id = sl.file_info[file_info_id as usize].file_id;
        let n = n / sl.file_info.len() as u64;
        let extent_size = sl.file_info[file_info_id as usize].extent_size as u64;
        let extent_id = (n / extent_size + sl.parity) as u16 + 1;   // avoid extent 0 by adding 1
        let block_id = (n % extent_size as u64) as u16;

        drop(sl);

        BlockId {
            file_id,
            extent_id,
            block_id,
        }
    }

    // The function is called when new checkpoint is created.
    fn next_checkpoint(&self, checkpoint_csn: u64, block_mgr: &BlockMgr) {
        let mut xl = self.lock.write().unwrap();

        if self.cur_checkpoint_csn.load(Ordering::Relaxed) != checkpoint_csn {
            xl.parity = checkpoint_csn & 0x1;

            self.seqn.set(0);

            block_mgr.get_checkpoint_files(&mut xl.file_info);

            self.cur_checkpoint_csn.store(checkpoint_csn, Ordering::Relaxed);
        }
    }
}

