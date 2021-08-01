/// VersionStore represents part of BlockStorageDriver functionality related to data versioning.
/// Each entry before transaction modify it is first copied to the version store. The versioned
/// entry is used when transaction rolls back or when other transactions need to read commited
/// version of entry.
/// Data in version store is fully discarded after system restart.
/// Each transaction keeps record of the first versioning entry it created. All entries of a single
/// transaction are linked into linked list. When transaction rolls back the first entry is used to
/// find starting point of the linked list of changes made by the transaction. System iterates over
/// the linked list and restores all changed entries in main data store.
/// The length of the linked list of entries is determined by retain_timespan. 
/// Versions older than retain_timespan are discarded, and freed space can be reused.
/// Each entry in version store also has a pointer to entry in the main data store which is used
/// during rollback.
/// Version store represents extendable circular list of extents. With time, oldest and already not 
/// used extents are removed from the list, and after resued by adding at the head of the
/// list to accomodate new versioning entries. To free old extents thransactions that run longer
/// than retain_timespan should be terminated.


use crate::common::errors::Error;
use crate::common::defs::BlockId;
use crate::common::misc::epoch_as_secs;
use crate::block_mgr::block_mgr::BlockMgr;
use crate::block_mgr::block::DBLOCK_HEADER_LEN;
use crate::block_mgr::block::VERENTRY_HEADER_LEN;
use crate::block_mgr::block::DataBlockEntryMut;
use crate::block_mgr::block::DataBlock;
use crate::block_mgr::block::BasicBlock;
use crate::block_mgr::block::BlockLockedMut;
use crate::block_mgr::allocator::BlockAllocator;
use crate::storage::datastore::FileDesc;
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::rc::Rc;
use std::cell::RefCell;


/// Shared state that can be sent to other threads.
pub struct VersionStoreSharedState {
    lock:               Arc<Mutex<AllocatorState>>,
    used_space:         Arc<AtomicUsize>,
    total_free_space:   Arc<usize>,
    trn_repo:           TrnRepo,
}


pub struct VersionStore {
    block_mgr:          Rc<BlockMgr>,
    entry_allocator:    RefCell<VersioningStoreEntryAllocator>,
    trn_repo:           TrnRepo,
}

impl VersionStore {

    pub fn new(block_mgr: Rc<BlockMgr>, block_allocator: Rc<BlockAllocator>, _retain_timespan: Duration, trn_set_size: usize) -> Result<Self, Error> {
    
        let entry_allocator = RefCell::new(VersioningStoreEntryAllocator::new(block_mgr.clone(), block_allocator)?);
        let trn_repo = TrnRepo::new(trn_set_size);

        Ok(VersionStore {
            block_mgr,
            entry_allocator,
            trn_repo,
        })
    }

    /// Build instance from shared state.
    pub fn from_shared_state(block_mgr: Rc<BlockMgr>, block_allocator: Rc<BlockAllocator>, ss: VersionStoreSharedState) -> Result<Self, Error> {
        let VersionStoreSharedState {lock, used_space, total_free_space, trn_repo } = ss;
        
        let entry_allocator = RefCell::new(VersioningStoreEntryAllocator::from_shared_state(block_mgr.clone(), block_allocator, lock, used_space, total_free_space)?);
        let cur_block_id = entry_allocator.borrow().calc_next_block_id(&trn_repo)?;
        entry_allocator.borrow_mut().cur_block_id = cur_block_id;

        Ok(VersionStore {
            block_mgr,
            entry_allocator,
            trn_repo,
        })
    }

    /// Return shared state that can be sent to other threads.
    pub fn get_shared_state(&self) -> VersionStoreSharedState {
        let (lock, used_space, total_free_space) = self.entry_allocator.borrow().get_shared_state();
        VersionStoreSharedState {
            lock:               lock,
            used_space:         used_space,
            total_free_space:   total_free_space,
            trn_repo:           self.trn_repo.clone(),
        }
    }

    /// Create entry version in version store on entry modification.
    /// This will copy entry contents to the version store. And point modified entry to that
    /// version.
    pub fn create_version(&self, block_id: &BlockId, entry: &mut DataBlockEntryMut, tsn: u64) -> Result<(), Error> {
        let mut entry_allocator = self.entry_allocator.borrow_mut();
        let ver_entry_sz = VERENTRY_HEADER_LEN + entry.size() as usize;
        let mut ver_block = entry_allocator.get_next_entry_block(ver_entry_sz, &self.trn_repo)?;

        let ver_block_id = ver_block.get_id();
        let mut ver_entry = ver_block.add_version_entry(ver_entry_sz);

        ver_entry.set_main_storage_ptr(&block_id, entry.get_id());
        if let Some((last_ver_block_id, last_ver_entry_id)) = self.trn_repo.update_last_version_store_entry(tsn, ver_block_id, ver_entry.get_id()) {
            ver_entry.set_prev_created_entry_ptr(&last_ver_block_id, last_ver_entry_id);
        } else {
            ver_entry.set_prev_created_entry_ptr(&BlockId::new(), 0);
        }

        ver_entry.copy_from_mut(&entry);
        entry.set_prev_version_ptr(&ver_block_id, ver_entry.get_id());

        Ok(())
    }

    /// Return iterator over entries created by the specified transaction in versioning store.
    pub fn get_iter_for_tran(&self, tsn: u64) -> Result<Iterator, Error> {
        if let Some((last_ver_block_id, last_ver_entry_id)) = self.trn_repo.get_last_version_store_entry(tsn) {
            Ok(Iterator {
                version_store: &self,
                last_ver_block_id,
                last_ver_entry_id,
            })
        } else {
            Ok(Iterator {
                version_store: &self,
                last_ver_block_id: BlockId::new(),
                last_ver_entry_id: 0,
            })
        }
    }

    /// Cleanup on transaction commit / rollback.
    pub fn finish_tran(&self, tsn: u64) {
        self.trn_repo.rm_tran(tsn);
    }
}


/// Iterator over entries in versioning store.
pub struct Iterator<'a> {
    version_store: &'a VersionStore,
    last_ver_block_id: BlockId,
    last_ver_entry_id: u16,
}

impl<'a> Iterator<'a> {

    pub fn get_next(&mut self) -> Result<Option<(BlockId, u16, BlockLockedMut<'a, DataBlock<'a>>, u16)>, Error> {
        if self.last_ver_block_id.file_id == 0
            && self.last_ver_block_id.extent_id == 0
            && self.last_ver_block_id.block_id == 0
            && self.last_ver_entry_id == 0
        {
            Ok(None)
        } else {
            let block = self.version_store.block_mgr.get_versioning_block_mut(&self.last_ver_block_id)?;
            let entry = block.get_version_entry(self.last_ver_entry_id)?;

            let (last_ver_block_id, last_ver_entry_id) = entry.get_prev_created_entry_ptr();

            self.last_ver_block_id = last_ver_block_id;
            self.last_ver_entry_id = last_ver_entry_id;

            let (main_storage_block_id, main_storage_entry_id) = entry.get_main_storage_ptr();
            let entry_id = entry.get_id();
            Ok(Some((main_storage_block_id, main_storage_entry_id, block, entry_id)))
        }
    }
}



/// Shared state of version store allocator.
struct AllocatorState {
    tail_extent:        (u16, u16, u16),
    block_id:           BlockId,
    extent_size:        u16,
}


/// Allocate new blocks in versioning store.
#[derive(Clone)]
struct VersioningStoreEntryAllocator {
    block_mgr:          Rc<BlockMgr>,
    block_allocator:    Rc<BlockAllocator>,
    lock:               Arc<Mutex<AllocatorState>>,
    used_space:         Arc<AtomicUsize>,
    total_free_space:   Arc<usize>,
    cur_block_id:       BlockId,
}

impl VersioningStoreEntryAllocator {

    pub fn new(block_mgr: Rc<BlockMgr>, block_allocator: Rc<BlockAllocator>) -> Result<VersioningStoreEntryAllocator, Error> {
        let mut file_desc_buf = RefCell::new(vec![]);
        let (file_id, extent_id, extent_size) = Self::get_versioning_extent(&block_mgr, &mut file_desc_buf)?;
        let block_id = BlockId::init(file_id, extent_id, 1);

        let used_space          = Arc::new(AtomicUsize::new(2 + DBLOCK_HEADER_LEN));
        let total_free_space    = Arc::new(block_mgr.get_block_size());
        let cur_block_id        = block_id;

        let allocator_state = AllocatorState {
            tail_extent: (0, 0, 0),
            block_id,
            extent_size,
        };

        let lock                = Arc::new(Mutex::new(allocator_state));

        Ok(VersioningStoreEntryAllocator {
            block_mgr,
            block_allocator,
            lock, 
            used_space,
            total_free_space,
            cur_block_id,
        })
    }


    /// Build instance from shared state.
    pub fn from_shared_state(
        block_mgr:          Rc<BlockMgr>, 
        block_allocator:    Rc<BlockAllocator>,
        lock:               Arc<Mutex<AllocatorState>>,
        used_space:         Arc<AtomicUsize>,
        total_free_space:   Arc<usize>) -> Result<Self, Error> 
    {
        let cur_block_id = BlockId::new(); 
        Ok(VersioningStoreEntryAllocator {
            block_mgr,
            block_allocator,
            lock, 
            used_space,
            total_free_space,
            cur_block_id,
        })
    }

    /// Return shared state that can be sent to other threads.
    pub fn get_shared_state(&self) -> (Arc<Mutex<AllocatorState>>, Arc<AtomicUsize>, Arc<usize>) {
        (self.lock.clone(), self.used_space.clone(), self.total_free_space.clone())
    }

    /// Return a block for the entry to be placed in.
    pub fn get_next_entry_block(&mut self, entry_size: usize, trn_repo: &TrnRepo) -> Result<BlockLockedMut<DataBlock>, Error> {
        let block = self.block_mgr.get_versioning_block_mut(&self.cur_block_id)?;
        if block.get_free_space() >= entry_size {
            return Ok(block);
        } else {
            self.block_mgr.set_dirty(block.get_buf_idx(), true);
            self.cur_block_id = self.calc_next_block_id(trn_repo)?;
            self.block_mgr.get_versioning_block_mut(&self.cur_block_id)
        }
    }

    fn calc_next_block_id(&self, trn_repo: &TrnRepo) -> Result<BlockId, Error> {
        let mut lock = self.lock.lock().unwrap();
        let AllocatorState {
            tail_extent,
            mut block_id,
            extent_size,
        } = *lock;
        block_id.block_id += 1;
        if block_id.block_id == extent_size {
            // get oldest extent
            let (file_id, extent_id, extent_size) = tail_extent;

            let (file_id, extent_id, extent_size) = if extent_size > 0 {
                let mut tail_ehb = self.block_mgr.get_extent_header_block_mut_no_lock(&BlockId::init(file_id, extent_id, 0))?;
                if trn_repo.get_earliest_start_time() > tail_ehb.get_seal_date() {

                    tail_ehb.set_seal_date(u64::MAX);
                    let (tail_file_id, tail_extent_id) = tail_ehb.get_next_versioning_extent();
                    let desc = self.block_mgr.get_file_desc(tail_file_id).ok_or(Error::file_does_not_exist())?;
                    lock.tail_extent =(tail_file_id, tail_extent_id, desc.extent_size);

                    (file_id, extent_id, extent_size)
                } else {
                    self.block_allocator.allocate_versioning_extent()?
                }
            } else {
                self.block_allocator.allocate_versioning_extent()?
            };

            // set last used time and next extent for the current extent
            let mut ehb = self.block_mgr.get_extent_header_block_mut_no_lock(&BlockId::init(block_id.file_id, block_id.extent_id, 0))?;
            ehb.set_seal_date(epoch_as_secs());
            ehb.set_next_versioning_extent(file_id, extent_id);
            self.block_mgr.set_dirty(ehb.get_buf_idx(), true);

            block_id.file_id = file_id;
            block_id.extent_id = extent_id;
            block_id.block_id = 1;
            lock.extent_size = extent_size;
        }

        self.used_space.store(DBLOCK_HEADER_LEN + 2, Ordering::Relaxed);
        lock.block_id = block_id;

        Ok(block_id)
    }

    fn get_versioning_extent(block_mgr: &BlockMgr, file_desc_buf: &mut RefCell<Vec<FileDesc>>) -> Result<(u16, u16, u16), Error> {
        block_mgr.get_versioning_files(&mut file_desc_buf.borrow_mut());
        let file_desc_set = &file_desc_buf.borrow();
        for desc in file_desc_set.iter() {
            return Ok((desc.file_id, 1, desc.extent_size));
        }
        return Err(Error::db_size_limit_reached())
    }
}



/// Repository for tracking version entries on per-transaction basis.
#[derive(Clone)]
struct TrnRepo {
    trn_info:            Arc<Mutex<TrnRepoBody>>,
    earliest_start_time: Arc<AtomicU64>,
}

impl TrnRepo {

    fn new(size: usize) -> Self {
        let body = TrnRepoBody {
            trn_map: HashMap::with_capacity(size),
            start_time: Vec::with_capacity(size),
        };

        let earliest_start_time = Arc::new(AtomicU64::new(epoch_as_secs()));

        TrnRepo {
            trn_info: Arc::new(Mutex::new(body)),
            earliest_start_time,
        }
    }

    fn get_last_version_store_entry(&self, tsn: u64) -> Option<(BlockId, u16)> {
        let body = self.trn_info.lock().unwrap();
        let (block_id, entry_id, _) = body.trn_map.get(&tsn)?;
        Some((*block_id, *entry_id))
    }

    fn update_last_version_store_entry(&self, tsn: u64, block_id: BlockId, entry_id: u16) -> Option<(BlockId, u16)> {
        let cur_time = epoch_as_secs();

        let mut body = self.trn_info.lock().unwrap();
        if let Some((_, _, start_time)) = body.trn_map.get(&tsn) {
            let st = *start_time;
            let (block_id, entry_id, _) = body.trn_map.insert(tsn, (block_id, entry_id, st)).unwrap();
            Some((block_id, entry_id))
        } else {
            let start_time = std::cmp::max(cur_time, if body.start_time.len() > 0 {body.start_time[0]} else {self.get_earliest_start_time()});
            body.trn_map.insert(tsn, (block_id, entry_id, start_time));
            let pos = body.start_time.binary_search(&start_time).unwrap_or_else(|x| x);
            body.start_time.insert(pos, start_time);
            None
        }
    }

    fn rm_tran(&self, tsn: u64) {
        let mut body = self.trn_info.lock().unwrap();
        if let Some((_, _, start_time)) = body.trn_map.get(&tsn) {
            let st = *start_time;
            let pos = body.start_time.binary_search(start_time).unwrap_or_else(|x| x);
            body.start_time.remove(pos);
            if pos == 0 && st > self.get_earliest_start_time() {
                self.earliest_start_time.store(body.start_time[0], Ordering::Relaxed);
            }
        }
        body.trn_map.remove(&tsn);
    }

    fn get_earliest_start_time(&self) -> u64 {
        self.earliest_start_time.load(Ordering::Relaxed)
    }
}


struct TrnRepoBody {
    trn_map:        HashMap<u64, (BlockId, u16, u64)>,
    start_time:     Vec<u64>,
}



#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::datastore::DataStore;
    use crate::storage::datastore::FileType;
    use crate::storage::datastore::FileDesc;
    use crate::storage::datastore::FileState;
    use crate::system::config::ConfigMt;
    use std::time::Duration;
    use std::path::Path;


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
            extent_num:     2,
            max_extent_num: 65500,
            file_type:      FileType::VersioningStoreFile,
        };
        let desc3 = FileDesc {
            state:          FileState::InUse,
            file_id:        5,
            extent_size:    10,
            extent_num:     5,
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
    fn test_version_store() {
        let dspath = "/tmp/test_version_store_4546456";
        let block_size = 8192;
        let block_num = 100;
        let retain_timespan = Duration::from_secs(3600);
        let trn_set_size = 100;

        let conf = ConfigMt::new();
        let mut c = conf.get_conf();
        c.set_datastore_path(dspath.to_owned());
        c.set_block_mgr_n_lock(10);
        c.set_free_info_n_file_lock(10);
        c.set_free_info_n_extent_lock(10);
        c.set_block_buf_size(block_num*block_size as u64);
        c.set_checkpoint_data_threshold(10*1024);
        c.set_version_retain_time(10_000);
        c.set_writer_num(2);
        drop(c);

        let _init_fdesc = init_datastore(dspath, block_size);

        let block_mgr = Rc::new(BlockMgr::new(conf.clone()).expect("Failed to create block mgr"));
        let block_allocator = Rc::new(BlockAllocator::new(conf.clone(), block_mgr.clone()));

        let vs = VersionStore::new(block_mgr.clone(), block_allocator.clone(), retain_timespan, trn_set_size).expect("Failed to create version store");


        // two transactions create several versons 

        let block_id = BlockId::init(3,1,1);
        let tsn = 123;
        let entry_sz = 47;
        let vcnt = 6;
        let mut block = block_mgr.get_block_mut(&block_id).expect("Failed to get block");
        for i in 0..vcnt {
            let mut entry = block.add_entry(entry_sz + i);
            entry.mut_slice(0,1)[0] = (vcnt + i) as u8;
            vs.create_version(&block_id, &mut entry, tsn + i as u64 % 2).expect("Failed to create a version");
        }

        for t in 0..2 {
            let mut cnt = 0;
            let mut iter = vs.get_iter_for_tran(tsn + t as u64).expect("Failed to get iterator");
            while let Some((_main_storage_block_id, _main_storage_entry_id, mut block, entry_id)) = iter.get_next().expect("Failed to iterate") {
                let mut ventry = block.get_version_entry_mut(entry_id).expect("Failed to get entry");
                let mut entry = ventry.inner_entry();
                let i = vcnt - cnt - 2 + t;
                assert_eq!(entry.size() as usize, entry_sz + i);
                let slice = entry.mut_slice(0, 2);
                assert_eq!(slice[0] as usize, vcnt + i);
                cnt += 2;
            }
            assert_eq!(cnt, vcnt);
        }


        // recreating version store from shared state
        
        
        let ss = vs.get_shared_state();
        let vs2 = VersionStore::from_shared_state(block_mgr.clone(), block_allocator.clone(), ss).expect("Failed to create from shared state");


        // finalize transaction


        assert!(vs2.get_iter_for_tran(tsn).unwrap().get_next().unwrap().is_some());
        assert!(vs.get_iter_for_tran(tsn).unwrap().get_next().unwrap().is_some());
        vs.finish_tran(tsn);
        assert!(vs2.get_iter_for_tran(tsn).unwrap().get_next().unwrap().is_none());
        assert!(vs.get_iter_for_tran(tsn).unwrap().get_next().unwrap().is_none());
        vs2.finish_tran(tsn + 1);


        // test allocation

        
        let block_id = BlockId::init(3,1,5);
        let tsn = 125;
        let entry_sz = block_size / 4;
        let mut block = block_mgr.get_block_mut(&block_id).expect("Failed to get block");
        let mut entry = block.add_entry(entry_sz);
        for _ in 0..32 {
            vs.create_version(&block_id, &mut entry, tsn).expect("Failed to create a version");
            vs.finish_tran(tsn);
        }
    }
}
