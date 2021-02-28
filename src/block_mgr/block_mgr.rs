/// Block manager manipulates blocks.
/// Block manager loads blocks into buffer from main data store, version store, or checkpoint
/// store.


use crate::common::errors::Error;
use crate::common::defs::BlockId;
use crate::storage::datastore::DataStore;
use crate::storage::datastore::FileDesc;
use crate::storage::datastore::FileType;
use crate::storage::datastore::FileState;
use crate::system::config::ConfigMt;
use crate::buf_mgr::buf_mgr::BufMgr;
use crate::buf_mgr::buf_mgr::BlockDesc;
use crate::buf_mgr::buf_mgr::Pinned;
use crate::buf_mgr::buf_mgr::BlockArea;
use crate::buf_mgr::buf_mgr::BlockType;
use crate::buf_mgr::lru::LruList;
use crate::buf_mgr::lru::LruNodeRef;
use crate::block_mgr::block::BasicBlock;
use crate::block_mgr::block::FileHeaderBlock;
use crate::block_mgr::block::ExtentHeaderBlock;
use crate::block_mgr::block::FreeInfoBlock;
use crate::block_mgr::block::DataBlock;
use crate::block_mgr::block::BlockLocked;
use crate::block_mgr::block::BlockLockedMut;
use crate::block_mgr::block::RwLockGuard;
use std::sync::Arc;
use std::sync::RwLock;
use std::ops::DerefMut;


pub struct BlockMgr {
    locks:                  Arc<Vec<RwLock<()>>>,
    buf_mgr:                Arc<BufMgr<LruNodeRef<(usize, BlockId)>, LruList<(usize, BlockId)>>>,
    ds:                     DataStore,
}


impl BlockMgr {

    pub fn new(conf: ConfigMt) -> Result<BlockMgr, Error> {
        let block_mgr_n_lock = *conf.get_conf().get_block_mgr_n_lock();
        let block_buf_size = *conf.get_conf().get_block_buf_size();

        let mut locks = Vec::with_capacity(block_mgr_n_lock as usize);
        for _ in 0..block_mgr_n_lock {
            locks.push(RwLock::new(()));
        }

        let ds = DataStore::new(conf.clone())?;

        let block_size = ds.get_block_size();
        
        let buf_mgr = Arc::new(BufMgr::new(block_size, block_buf_size as usize / block_size)?);

        Ok(BlockMgr {
            locks: Arc::new(locks),
            buf_mgr,
            ds,
        })
    }

    /// Lock for reading and return data block.
    pub fn get_block(&self, block_id: &BlockId) -> Result<BlockLocked<DataBlock>, Error> {
        self.get_block_for_read::<DataBlock>(block_id, DataBlock::new)
    }

    /// Lock for reading and return file-level header block.
    pub fn get_file_header_block(&self, block_id: &BlockId) -> Result<BlockLocked<FileHeaderBlock>, Error> {
        self.get_block_for_read::<FileHeaderBlock>(block_id, FileHeaderBlock::new)
    }

    /// Lock for reading and return extent-level header block.
    pub fn get_extent_header_block(&self, block_id: &BlockId) -> Result<BlockLocked<ExtentHeaderBlock>, Error> {
        self.get_block_for_read::<ExtentHeaderBlock>(block_id, ExtentHeaderBlock::new)
    }

    /// Lock for reading and return a block containing free info bitmap section which didn't fint
    /// into header block.
    pub fn get_free_info_block(&self, block_id: &BlockId) -> Result<BlockLocked<FreeInfoBlock>, Error> {
        self.get_block_for_read::<FreeInfoBlock>(block_id, FreeInfoBlock::new)
    }


    /// Lock for writing and return data block.
    pub fn get_block_mut(&self, block_id: &BlockId) -> Result<BlockLockedMut<DataBlock>, Error> {
        self.get_block_for_write::<DataBlock>(block_id, DataBlock::new, false)
    }

    /// Lock for writing and return file-level header block.
    pub fn get_file_header_block_mut(&self, block_id: &BlockId) -> Result<BlockLockedMut<FileHeaderBlock>, Error> {
        self.get_block_for_write::<FileHeaderBlock>(block_id, FileHeaderBlock::new, false)
    }

    /// Lock for writing and return extent-level header block.
    pub fn get_extent_header_block_mut(&self, block_id: &BlockId) -> Result<BlockLockedMut<ExtentHeaderBlock>, Error> {
        self.get_block_for_write::<ExtentHeaderBlock>(block_id, ExtentHeaderBlock::new, false)
    }

    /// Lock for writing and return a block containing free info bitmap section which didn't fint
    /// into header block.
    pub fn get_free_info_block_mut(&self, block_id: &BlockId) -> Result<BlockLockedMut<FreeInfoBlock>, Error> {
        self.get_block_for_write::<FreeInfoBlock>(block_id, FreeInfoBlock::new, false)
    }


    // lock and return block of type T
    fn get_block_for_read<'b, T>(&'b self, block_id: &BlockId, init_fun: fn(BlockId, usize, Pinned<'b, BlockArea>) -> T) -> Result<BlockLocked<T>, Error> 
        where T: BasicBlock
    {
        let lid = block_id.hash(self.locks.len());
        let lock_holder = RwLockGuard::Read(self.locks[lid].read().unwrap());

        if let Some((data, buf_idx)) = self.buf_mgr.get_block(&block_id) {
            Ok(BlockLocked::new(lock_holder, init_fun(*block_id, buf_idx, data)))
        } else {
            drop(lock_holder);

            let block_type = self.determine_block_type(&block_id);
            let ds_data = self.ds.load_block(&block_id, FileState::InUse)?;

            let lock_holder = RwLockGuard::Read(self.locks[lid].read().unwrap());
            let (mut data, buf_idx) = self.allocate_on_cache(*block_id, block_type);
            data.copy_from_slice(&ds_data);
            return Ok(BlockLocked::new(lock_holder, init_fun(*block_id, buf_idx, data)))
        }
    }

    // lock and return block of type T suitable for modification;
    // if try_lock is true then first try locking, Error will be returned if block is blocked.
    pub fn get_block_mut_no_lock(&self, block_id: &BlockId) -> Result<DataBlock, Error> {

        if let Some((data, buf_idx)) = self.buf_mgr.get_block(&block_id) {
            self.set_dirty(buf_idx, true);
            Ok(DataBlock::new(*block_id, buf_idx, data))
        } else {
            let block_type = self.determine_block_type(&block_id);
            let ds_data = self.ds.load_block(&block_id, FileState::InUse)?;
            let (mut data, buf_idx) = self.allocate_on_cache(*block_id, block_type);
            data.copy_from_slice(&ds_data);

            self.set_dirty(buf_idx, true);

            Ok(DataBlock::new(*block_id, buf_idx, data))
        }
    }

    fn determine_block_type(&self, block_id: &BlockId) -> BlockType {
        let file_desc = self.ds.get_file_desc(block_id.file_id).unwrap();
        match file_desc.file_type {
            FileType::DataStoreFile => BlockType::DataBlock,
            FileType::CheckpointStoreFile => BlockType::CheckpointBlock,
            FileType::VersioningStoreFile => BlockType::VersionBlock,
        }
    }


    // lock and return block of type T suitable for modification;
    // if try_lock is true then first try locking, Error will be returned if block is blocked.
    pub fn get_block_for_write<'b, T>(&'b self, block_id: &BlockId, init_fun: fn(BlockId, usize, Pinned<'b, BlockArea>) -> T, try_lock: bool) -> Result<BlockLockedMut<T>, Error>
        where T: BasicBlock
    {
        let lid = block_id.hash(self.locks.len());
        let lock_holder = if try_lock {
            RwLockGuard::Write(self.locks[lid].try_write().map_err(|_| Error::try_lock_error())?)
        } else {
            RwLockGuard::Write(self.locks[lid].write().unwrap())
        };

        if let Some((data, buf_idx)) = self.buf_mgr.get_block(&block_id) {
            self.set_dirty(buf_idx, true);
            Ok(BlockLockedMut::new(BlockLocked::new(lock_holder, init_fun(*block_id, buf_idx, data))))
        } else {
            drop(lock_holder);

            let block_type = self.determine_block_type(&block_id);
            let ds_data = self.ds.load_block(&block_id, FileState::InUse)?;

            let lock_holder = if try_lock {
                RwLockGuard::Write(self.locks[lid].try_write().map_err(|_| Error::try_lock_error())?)
            } else {
                RwLockGuard::Write(self.locks[lid].write().unwrap())
            };
            let (mut data, buf_idx) = self.allocate_on_cache(*block_id, block_type);
            data.copy_from_slice(&ds_data);

            self.set_dirty(buf_idx, true);

            Ok(BlockLockedMut::new(BlockLocked::new(lock_holder, init_fun(*block_id, buf_idx, data))))
        }
    }

    pub fn block_fill_size(&self) -> usize {
        self.ds.block_fill_size()
    }

    /// Return iterator over blocks in the buffer.
    pub fn get_iter(&self) -> BlockIterator {
        BlockIterator::new(&self.buf_mgr)
    }

    /// Return block from buffer by index.
    pub fn get_block_by_idx(&self, id: usize) -> Option<BlockLockedMut<DataBlock>> {

        let lid = id % self.locks.len();

        let lock_holder = RwLockGuard::Read(self.locks[lid].read().unwrap());
        if let Some(data) = self.buf_mgr.get_block_by_idx(id) {
            let bdesc = self.buf_mgr.get_bdesc_by_idx(id).unwrap();
            Some(BlockLockedMut::new(BlockLocked::new(lock_holder, DataBlock::new(bdesc.block_id, id, data))))
        } else {
            None 
        }
    }

    /// Write block to disk.
    pub fn write_block(&self, block: &mut DataBlock) -> Result<(), Error> {
        self.ds.write_block(block, FileState::InUse)
    }

    fn allocate_on_cache(&self, block_id: BlockId, block_type: BlockType) -> (Pinned<BlockArea>, usize) {
        loop {
            if let Some((data, buf_idx)) = self.buf_mgr.allocate_on_cache(&block_id, block_type) {
                return (data, buf_idx)
            } else {
                // trigger buffer writer and wait for completion
                panic!("No free space inf buffer");
            }
        }
    }

    /// take a free block from the buffer, assign the specified block_id, for it and return.
    pub fn allocate_on_cache_mut_no_lock(&self, block_id: BlockId, block_type: BlockType) -> Result<DataBlock, Error> {
        let (mut data, buf_idx) = self.allocate_on_cache(block_id, block_type);
        self.set_dirty(buf_idx, true);
        for b in data.deref_mut().deref_mut() {*b = 0;};
        Ok(DataBlock::new(block_id, buf_idx, data))
    }

    /// Clear and fill file_desc_set with file descriptions of data files.
    pub fn get_data_files(&self, file_desc: &mut Vec<FileDesc>) {
        self.ds.get_data_files(file_desc);
    }

    /// Clear and fill file_desc_set with file descriptions of versioning store files.
    pub fn get_versioning_files(&self, file_desc: &mut Vec<FileDesc>) {
        self.ds.get_versioning_files(file_desc);
    }

    /// Clear and fill file_desc_set with file descriptions of checkpoint store files.
    pub fn get_checkpoint_files(&self, file_desc: &mut Vec<FileDesc>) {
        self.ds.get_checkpoint_files(file_desc);
    }

    /// Mark the block in buffer if checkpoint block is written to disk or not.
    pub fn set_checkpoint_written(&self, desc_id: usize, state: bool) {
        self.buf_mgr.set_checkpoint_written(desc_id, state);
    }

    /// Mark the block in buffer if checkpoint block is written to disk or not.
    pub fn set_checkpoint_block_id(&self, desc_id: usize, block_id: BlockId) {
        self.buf_mgr.set_checkpoint_block_id(desc_id, block_id);
    }

    /// Mark the block in buffer if if it is dirty or not.
    pub fn set_dirty(&self, desc_id: usize, state: bool) {
        self.buf_mgr.set_dirty(desc_id, state);
    }

    /// add extent to a file in the datastore.
    pub fn add_extent(&self, file_id: u16) -> Result<(), Error> {
        self.ds.add_extent(file_id, FileState::InUse)
    }

    /// Clone
    pub fn clone(&self) -> Result<Self, Error> {
        Ok(BlockMgr {
            locks: self.locks.clone(),
            buf_mgr: self.buf_mgr.clone(),
            ds: self.ds.clone()?,
        })
    }

    /// Return block descriptor by block index in buffer.
    pub fn get_block_desc(&self, idx: usize) -> Option<BlockDesc> {
        self.buf_mgr.get_bdesc_by_idx(idx)
    }

    pub fn get_block_size(&self) -> usize {
        self.ds.get_block_size()
    }

    /// Add a new file to datastore.
    pub fn add_datafile(&self, file_type: FileType, extent_size: u16, extent_num: u16, max_extent_num: u16) -> Result<(), Error> {
        self.ds.add_datafile(file_type, extent_size, extent_num, max_extent_num)
    }

    /// Return number of free info blocks for an extent of a certain size.
    pub fn calc_extent_fi_block_num(&self, extent_size: usize) -> usize {
        self.ds.calc_extent_fi_block_num(extent_size)
    }
}


/// Itarator over blocks residing in the buffer.
pub struct BlockIterator<'a> {
    buf_mgr: &'a BufMgr<LruNodeRef<(usize, BlockId)>, LruList<(usize, BlockId)>>,
    idx:     usize
}

impl<'a> BlockIterator<'a> {

    fn new(buf_mgr: &'a BufMgr<LruNodeRef<(usize, BlockId)>, LruList<(usize, BlockId)>>) -> Self {
        BlockIterator {
            buf_mgr,
            idx: 0,
        }
    }

    pub fn next(&mut self) -> Option<BlockDesc> {
        let ret = self.buf_mgr.get_bdesc_by_idx(self.idx);
        self.idx += 1;
        ret
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::path::Path;
    use std::ops::Deref;
    use crate::block_mgr::block::FreeInfoHeaderSection;
    use crate::block_mgr::block::FreeInfoSection;


    fn init_datastore(dspath: &str, block_size: usize) -> Vec<FileDesc> {

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
    fn test_block_mgr() {

        let dspath = "/tmp/test_block_mgr_5689394";
        let block_size = 8192;
        let block_num = 100;
        
        if Path::new(&dspath).exists() {
            std::fs::remove_dir_all(&dspath).expect("Failed to delete test dir on cleanup");
        }
        std::fs::create_dir(&dspath).expect("Failed to create test dir");

        let mut conf = ConfigMt::new();
        let mut c = conf.get_conf();
        c.set_datastore_path(dspath.to_owned());
        c.set_block_mgr_n_lock(10);
        c.set_block_buf_size(block_num*block_size as u64);
        drop(c);

        let init_fdesc = init_datastore(dspath, block_size);

        let block_mgr = BlockMgr::new(conf.clone()).expect("Failed to create instance");

        let entry_id = 0;
        let mut entry_sz = 501;
        let mut full_cnt = 0;
        let someval = 123u8;
        let block_id1 = BlockId::init(3,1,5);
        let block_id2 = BlockId::init(3,0,0);
        let block_id3 = BlockId::init(3,1,0);
        let block_id4 = BlockId::init(3,0,1);
        let block_id5 = BlockId::init(3,1,4);
        let block_id6 = BlockId::init(1,1,1);

        let mut block1 = block_mgr.get_block_mut(&block_id1).expect("failed to get block");
        assert!(block1.get_entry(entry_id).is_err());
        block1.add_entry(entry_sz);
        drop(block1);

        let mut block2 = block_mgr.get_file_header_block_mut(&block_id2).expect("failed to get block");
        assert_eq!(block2.get_full_cnt(), 1);
        block2.set_full_cnt(full_cnt);
        drop(block2);

        let mut block3 = block_mgr.get_extent_header_block_mut(&block_id3).expect("failed to get block");
        assert_eq!(block3.get_full_cnt(), 1);
        block3.set_full_cnt(full_cnt);
        drop(block3);

        let mut block4 = block_mgr.get_free_info_block_mut(&block_id4).expect("failed to get block");
        assert_eq!(block4.fi_slice()[0], 0);
        block4.fi_slice_mut()[0] = someval;
        drop(block4);

        let block5 = block_mgr.get_block_for_write(&block_id5, DataBlock::new, false).expect("failed to get block");
        drop(block5);


        let block1 = block_mgr.get_block(&block_id1).expect("failed to get block");
        assert!(block1.get_entry(entry_id).is_ok());
        drop(block1);

        let block2 = block_mgr.get_file_header_block(&block_id2).expect("failed to get block");
        assert_eq!(block2.get_full_cnt(), full_cnt);
        drop(block2);

        let block3 = block_mgr.get_extent_header_block(&block_id3).expect("failed to get block");
        assert_eq!(block3.get_full_cnt(), full_cnt);
        drop(block3);

        let block4 = block_mgr.get_free_info_block(&block_id4).expect("failed to get block");
        assert_eq!(block4.fi_slice()[0], someval);
        drop(block4);


        assert_eq!(*conf.get_conf().get_block_fill_ratio() as usize * block_size / 100, block_mgr.block_fill_size());

        let mut block_iter = block_mgr.get_iter();
        while let Some(desc) = block_iter.next() { }
        for i in 0..block_num as usize {
            assert!(block_mgr.get_block_by_idx(i).is_some());
        }

        let mut block1 = block_mgr.get_block_mut(&block_id1).expect("failed to get block");
        block_mgr.write_block(&mut block1).expect("Faield to write block");
        let dsblock = block_mgr.ds.load_block(&block_id1, FileState::InUse).expect("Failed to load block");
        assert_eq!(&block1.slice(), &dsblock.deref().deref());
        drop(dsblock);

        assert!(block_mgr.allocate_on_cache_mut_no_lock(block_id6, BlockType::CheckpointBlock).is_ok());

        let mut files = Vec::new();
        block_mgr.get_checkpoint_files(&mut files);
        assert_eq!(1, files.len());
        assert_eq!(init_fdesc[2], files[0]);

        block_mgr.get_versioning_files(&mut files);
        assert_eq!(1, files.len());
        assert_eq!(init_fdesc[1], files[0]);

        block_mgr.get_data_files(&mut files);
        assert_eq!(1, files.len());
        assert_eq!(init_fdesc[0], files[0]);

        let desc_id = 99;
        let bdesc = block_mgr.get_block_desc(desc_id).expect("Failed to get block desc");
        assert_eq!(bdesc.dirty, false);
        assert_eq!(bdesc.checkpoint_block_id, BlockId::new());
        assert_eq!(bdesc.checkpoint_written, true);

        let block_id = BlockId::init(1,1,1);
        block_mgr.set_checkpoint_written(desc_id, false);
        block_mgr.set_checkpoint_block_id(desc_id, block_id);
        block_mgr.set_dirty(desc_id, true);

        let bdesc = block_mgr.get_block_desc(desc_id).expect("Failed to get block desc");
        assert_eq!(bdesc.dirty, true);
        assert_eq!(bdesc.checkpoint_block_id, block_id);
        assert_eq!(bdesc.checkpoint_written, false);

        assert!(block_mgr.add_extent(3).is_ok());
        let fdesc = block_mgr.ds.get_file_desc(3).expect("Failed to get file desc");
        assert_eq!(fdesc.extent_num, 4);

        let cloned = block_mgr.clone();
        drop(cloned);

        assert_eq!(block_mgr.get_block_size(), block_size);

        assert!(block_mgr.add_datafile(FileType::VersioningStoreFile, 12, 2, 6500).is_ok());
        let fdesc = block_mgr.ds.get_file_desc(6).expect("Failed to get file desc");
        assert_eq!(fdesc.extent_num, 2);
        assert_eq!(fdesc.extent_size, 12);
        assert_eq!(fdesc.max_extent_num, 6500);
        assert_eq!(fdesc.file_type, FileType::VersioningStoreFile);
    }
}
