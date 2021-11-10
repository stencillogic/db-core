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
const WAKE_WRITER_THREADS_INTERVAL_MS: u64 = 1000;


#[derive(Clone)]
pub struct BufWriter {
    writer_threads:     Arc<Vec<JoinHandle<()>>>, 
    terminate:          Arc<AtomicBool>, 
    write_ready:        SyncNotification<usize>,
    chkpnt_allocators:  CheckpointStoreBlockAllocators,
    waker:              Arc<JoinHandle<()>>
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

        let writer_threads = Arc::new(writer_threads);
        let wr = write_ready.clone();
        let wt = writer_threads.clone();
        let tm = terminate.clone();
        let waker = Arc::new(std::thread::spawn(move || {
            Self::waker_thread(wr, wt, tm);
        }));

        Ok(BufWriter {
            writer_threads,
            terminate,
            write_ready,
            chkpnt_allocators,
            waker,
        })
    }

    // wake writers with time interval
    fn waker_thread(write_ready: SyncNotification<usize>, 
        writer_threads: Arc<Vec<JoinHandle<()>>>,
        terminate: Arc<AtomicBool>) {
        while !terminate.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(WAKE_WRITER_THREADS_INTERVAL_MS));
            write_ready.send(writer_threads.len(), true);
            loop {
                if let Some(lock) = write_ready.wait_for_interruptable(
                    &mut (|count| -> bool { *count != 0 }),
                    &mut (|| -> bool { terminate.load(Ordering::Relaxed) }),
                    Duration::from_millis(CONDVAR_WAIT_INTERVAL_MS)
                ) {
                    if *lock == 0 {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    pub fn terminate(self) {
        if let Ok(waker) = Arc::try_unwrap(self.waker) {
            self.terminate.store(true, Ordering::Relaxed);
            waker.join().unwrap();

            let mut writer_threads = Arc::try_unwrap(self.writer_threads).unwrap();
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
                &mut (|count| -> bool { *count == 0 }),
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
                if let Some(mut block) = block_mgr.get_block_by_idx(desc.id, desc.block_id, desc.block_type) {
                    if block.get_id() == desc.block_id {
                        Self::write_block(&mut block, block_mgr, chkpnt_allocators, false)?;
                    }
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
                    let mut checkpoint_block = block_mgr.get_block_mut_no_lock(&desc.checkpoint_block_id)?;
                    let checkpoint_csn = checkpoint_block.get_checkpoint_csn();
                    let actual_block_id = chkpnt_allocators.get_next_block_id(checkpoint_csn, block_mgr)?;
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
    pub fn get_next_block_id(&self, checkpoint_csn: u64, block_mgr: &BlockMgr) -> Result<BlockId, Error> {
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
    pub fn get_next_block_id(&self, checkpoint_csn: u64, block_mgr: &BlockMgr) -> Result<BlockId, Error> {

        if self.cur_checkpoint_csn.load(Ordering::Relaxed) != checkpoint_csn {
            self.next_checkpoint(checkpoint_csn, block_mgr);
        }

        let sl = self.lock.read().unwrap();

        let n = self.seqn.get_next();
        let fid_shift = (n % sl.file_info.len() as u64) as usize;

        let filen = sl.file_info.len();
        for i in fid_shift..filen+fid_shift {

            let fid = if i >= filen {
                i - filen
            } else {
                i
            };

            let fi = sl.file_info[fid];

            let file_id = fi.file_id;
            let n = n / filen as u64;
            let extent_size = fi.extent_size as u64;
            let extent_id = (n / extent_size * 2 + sl.parity) as u16 + 1;   // avoid extent 0 by adding 1
            let block_id = (n % extent_size as u64) as u16;
            if extent_id >= fi.extent_num {
                if fi.extent_num == fi.max_extent_num {
                    continue;
                } else {
                    block_mgr.add_extent(file_id)?;
                    let mut inc = 1;
                    if extent_id > fi.extent_num {
                        if fi.extent_num <= fi.max_extent_num - 2 {
                            block_mgr.add_extent(file_id)?;
                            inc += 1;
                        } else {
                            continue;
                        }
                    }
                    drop(sl);
                    let mut xl = self.lock.write().unwrap();
                    xl.file_info[fid].extent_num += inc;
                    drop(xl);
                }
            }

            return Ok(BlockId {
                file_id,
                extent_id,
                block_id,
            });
        }

        Err(Error::checkpoint_store_size_limit_reached())
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


#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::datastore::DataStore;
    use crate::storage::datastore::FileType;
    use crate::storage::datastore::FileState;
    use crate::block_mgr::block::BasicBlock;
    use crate::system::config::ConfigMt;
    use crate::buf_mgr::buf_mgr::Pinned;
    use crate::buf_mgr::buf_mgr::BlockArea;
    use std::time::Duration;
    use std::path::Path;
    use std::rc::Rc;
    use std::cell::Ref;


    fn init_datastore(dspath: &str, block_size: usize) -> Vec<FileDesc> {

        if Path::new(&dspath).exists() {
            std::fs::remove_dir_all(&dspath).expect("Failed to delete test dir on cleanup");
        }
        std::fs::create_dir(&dspath).expect("Failed to create test dir");

        let mut fdset = vec![];
        let desc1 = FileDesc {
            state:          FileState::InUse,
            file_id:        3,
            extent_size:    16,
            extent_num:     3,
            max_extent_num: 65500,
            file_type:      FileType::DataStoreFile,
        };
        let desc2 = FileDesc {
            state:          FileState::InUse,
            file_id:        4,
            extent_size:    10,
            extent_num:     3,
            max_extent_num: 65500,
            file_type:      FileType::VersioningStoreFile,
        };
        let desc3 = FileDesc {
            state:          FileState::InUse,
            file_id:        5,
            extent_size:    10,
            extent_num:     3,
            max_extent_num: 65500,
            file_type:      FileType::CheckpointStoreFile,
        };

        fdset.push(desc1);
        fdset.push(desc2);
        fdset.push(desc3);

        DataStore::initialize_datastore(dspath, block_size, &fdset).expect("Failed to init datastore");
        fdset
    }

    #[test]
    fn test_buf_writer() {
        let writer_num = 2;
        let dspath = "/tmp/test_buf_writer_34554654";
        let block_size = 8192;
        let block_num = 100;

        let conf = ConfigMt::new();
        let mut c = conf.get_conf();
        c.set_datastore_path(dspath.to_owned());
        c.set_block_mgr_n_lock(10);
        c.set_block_buf_size(block_num*block_size as u64);
        drop(c);

        let _init_fdesc = init_datastore(dspath, block_size);

        let block_mgr = Rc::new(BlockMgr::new(conf.clone()).expect("Failed to create instance"));

        let bw = BufWriter::new(&block_mgr.clone(), writer_num).expect("Failed to create buffer writers");

        // prepare dirty blocks
        let mut idxs = vec![];
        for i in 1..16 {
            let block_id = BlockId::init(3,1,i);
            let mut block = block_mgr.get_block_mut(&block_id).expect("Failed to get block for write");
            block.set_checkpoint_csn(1);
            block.add_entry(148);
            let idx = block.get_buf_idx();
            idxs.push(idx);
            let desc = block_mgr.get_block_desc(idx).unwrap();
            assert!(desc.dirty);

            // add checkpoint block to it
            let mut c_block = block_mgr.allocate_on_cache_mut_no_lock(BlockId::init(0,0,i), BlockType::CheckpointBlock).expect("Failed to allocate block");
            c_block.copy_from(&block);
            c_block.set_original_id(block.get_id());
            block_mgr.set_checkpoint_block_id(block.get_buf_idx(), c_block.get_id());
            block_mgr.set_checkpoint_written(block.get_buf_idx(), false);

            drop(c_block);
            drop(block);
        }

        std::thread::sleep(Duration::from_millis(2*WAKE_WRITER_THREADS_INTERVAL_MS));

        let mut i =0;
        assert!(loop {
            std::thread::sleep(Duration::new(2,0));
            let mut dirty = false;
            for idx in idxs.iter() {
                let desc = block_mgr.get_block_desc(*idx).unwrap();
                if desc.dirty {
                    dirty = true;
                }
            }
            if ! dirty {
                break true;
            }
            i += 1;
            if i == 30 {
                break false;
            }
        }, "Writers couldn't complete in 60 secs");


        // direct block write
        let block_id = BlockId::init(4,1,1);
        let mut block = block_mgr.get_block_mut(&block_id).expect("Failed to get versioning for write");
        let idx = block.get_buf_idx();
        block.set_checkpoint_csn(123);

        bw.write_data_block(&mut block, &block_mgr, true).expect("Failed to write versioning block");
        let desc = block_mgr.get_block_desc(idx).unwrap();
        assert!(desc.dirty);

        bw.write_data_block(&mut block, &block_mgr, false).expect("Failed to write versioning block");
        let desc = block_mgr.get_block_desc(idx).unwrap();
        assert!(!desc.dirty);


        bw.terminate();

        drop(block);
        drop(block_mgr);


        let ds = DataStore::new(conf).expect("Failed to create datastore");
        let stub_pin =  AtomicU64::new(1000);
        for i in 1..16 {
            let block_id = BlockId::init(3,1,i);
            let ba: Ref<BlockArea> = ds.load_block(&block_id, FileState::InUse).expect("Failed to load block");
            let db = DataBlock::new(block_id, 0, Pinned::<BlockArea>::new(ba.clone(), &stub_pin));
            assert_eq!(db.get_checkpoint_csn(), 1);
            assert!(db.has_entry(0));
            drop(db);
            drop(ba);

            let block_id = BlockId::init(5, if i<10 {2} else {4}, if i<10 {i} else {i - 10});
            let ba: Ref<BlockArea> = ds.load_block(&block_id, FileState::InUse).expect("Failed to load block");
            let db = DataBlock::new(block_id, 0, Pinned::<BlockArea>::new(ba.clone(), &stub_pin));
            assert_eq!(db.get_checkpoint_csn(), 1);
            assert!(db.has_entry(0));
            drop(db);
            drop(ba);
        }

        let block_id = BlockId::init(4,1,1);
        let ba: Ref<BlockArea> = ds.load_block(&block_id, FileState::InUse).expect("Failed to load block");
        let db = DataBlock::new(block_id, 0, Pinned::<BlockArea>::new(ba.clone(), &stub_pin));
        assert_eq!(db.get_checkpoint_csn(), 123);
    }
}

