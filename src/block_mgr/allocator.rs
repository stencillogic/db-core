/// Searh of free blocks and allocation of new blocks.


use crate::common::errors::Error;
use crate::common::errors::ErrorKind;
use crate::common::defs::BlockId;
use crate::common::defs::Sequence;
use crate::storage::datastore::FileDesc;
use crate::system::config::ConfigMt;
use crate::block_mgr::block_mgr::BlockMgr;
use crate::block_mgr::block::DataBlock;
use crate::block_mgr::block::BlockLockedMut;
use crate::block_mgr::free_info::FreeInfo;
use crate::block_mgr::free_info::FiData;
use crate::block_mgr::free_info::FreeInfoSharedState;
use crate::buf_mgr::buf_mgr::BlockType;
use std::cell::RefCell;
use std::rc::Rc;


/// Shared state that can be sent to other threads.
pub struct BlockAllocatorSharedState {
    fi_ss:                  FreeInfoSharedState,
    checkpoint_store_seq:   Sequence,
}


pub struct BlockAllocator {
    block_mgr:              Rc<BlockMgr>,
    file_desc_buf:          RefCell<Vec<FileDesc>>,
    free_info:              FreeInfo,
    file_fi_data:           RefCell<FiData>,
    extent_fi_data:         RefCell<FiData>,
    checkpoint_store_seq:   Sequence,
}

impl BlockAllocator {

    pub fn new(conf: ConfigMt, block_mgr: Rc<BlockMgr>) -> Self {
        let free_info = FreeInfo::new(conf.clone(), block_mgr.clone());
        let file_desc_buf = RefCell::new(vec![]);
        let file_fi_data = RefCell::new(FiData:: new());
        let extent_fi_data = RefCell::new(FiData:: new());
        let checkpoint_store_seq = Sequence::new(1);

        BlockAllocator {
            block_mgr,
            free_info,
            file_desc_buf,
            file_fi_data,
            extent_fi_data,
            checkpoint_store_seq,
        }
    }

    /// Build instance from shared state.
    pub fn from_shared_state(block_mgr: Rc<BlockMgr>, ss: BlockAllocatorSharedState) -> Result<Self, Error> {
        let BlockAllocatorSharedState { fi_ss, checkpoint_store_seq } = ss;

        let free_info = FreeInfo::from_shared_state(block_mgr.clone(), fi_ss)?;
        let file_desc_buf = RefCell::new(vec![]);
        let file_fi_data = RefCell::new(FiData:: new());
        let extent_fi_data = RefCell::new(FiData:: new());

        Ok(BlockAllocator {
            block_mgr,
            free_info,
            file_desc_buf,
            file_fi_data,
            extent_fi_data,
            checkpoint_store_seq,
        })
    }

    /// Return shared state that can be sent to other threads.
    pub fn get_shared_state(&self) -> BlockAllocatorSharedState {
        BlockAllocatorSharedState {
            fi_ss:                  self.free_info.get_shared_state(), 
            checkpoint_store_seq:   self.checkpoint_store_seq.clone(),
        }
    }

    /// Find or allocate a new data block.
    pub fn get_free(&self) -> Result<BlockLockedMut<DataBlock>, Error> {
        if let Some(block) = self.find_free_block()? {
            Ok(block)
        } else {
            self.allocate_block()
        }
    }

    /// mark an extent as full.
    pub fn mark_extent_full(&self, file_id: u16, extent_id: u16) -> Result<(), Error> {
        self.free_info.set_extent_bit(file_id, extent_id, true)
    }

    /// In some of datastore files add a new extent, return first free block from that extent.
    pub fn allocate_block(&self) -> Result<BlockLockedMut<DataBlock>, Error> {
        // in some of data files add a new extent;
        // try to get free block in that extent and return the block;
        // if free block was not found then add extent and repeat attempt.
        self.block_mgr.get_data_files(&mut self.file_desc_buf.borrow_mut());
        let file_desc_set = &self.file_desc_buf.borrow();
        for desc in file_desc_set.iter() {
            // try adding a new extent to datastore file
            self.free_info.get_fi_for_file(desc.file_id, &mut self.file_fi_data.borrow_mut())?;
            if self.file_fi_data.borrow().size() < desc.max_extent_num {
                self.block_mgr.add_extent(desc.file_id)?;
                self.free_info.add_extent(desc.file_id)?;
                let extent_id = self.file_fi_data.borrow().size();
                if let Some(block) = self.find_free_block_in_extent(desc.file_id, extent_id)? {
                    return Ok(block);
                }
            }
        }
        return Err(Error::db_size_limit_reached());
    }

    /// Mark block as full in free info section.
    pub fn set_free_info_used(&self, block_id: &BlockId) -> Result<(), Error> {
        // lock extent free info and set the bit for the block accordingly
        // check if extent changed from full to free or wise versa
        // if extent changed then lock file free info, and then release extent lock
        // update file free info accordingly
        self.free_info.set_block_bit(block_id, true)
    }

    /// Mark block as having free space in free info section.
    pub fn set_free_info_free(&self, block_id: &BlockId) -> Result<(), Error> {
        // lock extent free info and set the bit for the block accordingly
        // check if extent changed from full to free or wise versa
        // if extent changed then lock file free info, and then release extent lock
        // update file free info accordingly
        self.free_info.set_block_bit(block_id, false)
    }

    /// Return free data block from checkpoint store.
    pub fn get_free_checkpoint_block(&self, checkpoint_csn: u64) -> Result<DataBlock, Error> {
        // determine next available block_id;
        // find a free block from buffer and assign it to block_id;
        // return the block.
        let block_id = self.get_next_checkpoint_block_id(checkpoint_csn);
        self.block_mgr.allocate_on_cache_mut_no_lock(block_id, BlockType::CheckpointBlock)
    }

    /// Allocate extent in the versioning store.
    pub fn allocate_versioning_extent(&self) -> Result<(u16, u16, u16), Error> {
        self.block_mgr.get_versioning_files(&mut self.file_desc_buf.borrow_mut());
        let file_desc_set = &self.file_desc_buf.borrow();
        for desc in file_desc_set.iter() {
            if desc.extent_num < desc.max_extent_num {
                self.block_mgr.add_extent(desc.file_id)?;
                return Ok((desc.file_id, desc.extent_num, desc.extent_size));
            }
        }
        return Err(Error::db_size_limit_reached());
    }

    // return next generated checkpoint block_id.
    fn get_next_checkpoint_block_id(&self, checkpoint_csn: u64) -> BlockId {
        // use fake block_id because it is not considered by writer when writer
        // writes checkpoint block to disk.
        let seq_num = self.checkpoint_store_seq.get_next();
        let file_id = (checkpoint_csn & 0x1) as u16;                 // file_id 0 and 1 are reserved for checkpointing store file ids.
        let block_id = (seq_num & 0xffff) as u16;
        let seq_num = seq_num >> 16;
        let extent_id = (seq_num & 0xffff) as u16;

        BlockId {
            file_id,
            extent_id,
            block_id,
        }
    }

    // find and return block with free space.
    fn find_free_block(&self) -> Result<Option<BlockLockedMut<DataBlock>>, Error> {
        // search through free info of each file in datastore;
        // find exntents with free blocks;
        // try getting the block.
        self.block_mgr.get_data_files(&mut self.file_desc_buf.borrow_mut());
        let file_desc_set = &self.file_desc_buf.borrow();
        for desc in file_desc_set.iter() {
            self.free_info.get_fi_for_file(desc.file_id, &mut self.file_fi_data.borrow_mut())?;
            let file_fi_data = self.file_fi_data.borrow();
            let mut free_iter = file_fi_data.free_iter();
            while let Some(extent_id) = free_iter.next() {
                if let Some(block) = self.find_free_block_in_extent(desc.file_id, extent_id)? {
                    return Ok(Some(block));
                }
            }
        }
        Ok(None)
    }

    fn find_free_block_in_extent(&self, file_id: u16, extent_id: u16) -> Result<Option<BlockLockedMut<DataBlock>>, Error> {
        // find a free block in given extent;
        // try locking the block; 
        // if the block is already locked then continue search,
        // else check the used space in the block;
        // if block is full then unlock and continue search,
        // else return the block.
        self.free_info.get_fi_for_extent(file_id, extent_id, &mut self.extent_fi_data.borrow_mut())?;
        let extent_fi_data = self.extent_fi_data.borrow();
        let mut free_iter = extent_fi_data.free_iter();
        while let Some(block_id) = free_iter.next() {
            let blid = BlockId {
                file_id,
                extent_id: extent_id,
                block_id,
            };

            match self.block_mgr.get_block_for_write::<DataBlock>(&blid, DataBlock::new, true, 0) {
                Ok(block) => {
                    if block.get_used_space() < self.block_mgr.block_fill_size() {
                        return Ok(Some(block));
                    }
                    drop(block);
                },
                Err(e) => {
                    match e.kind() {
                        ErrorKind::TryLockError => {},
                        _ => return Err(e)
                    }
                },
            };
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::datastore::DataStore;
    use crate::storage::datastore::FileType;
    use crate::storage::datastore::FileState;
    use crate::block_mgr::block::BasicBlock;
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
    fn test_allocator() {
        let dspath = "/tmp/test_allocator_655637";
        let block_size = 8192;
        let block_num = 100;

        let mut conf = ConfigMt::new();
        let mut c = conf.get_conf();
        c.set_datastore_path(dspath.to_owned());
        c.set_block_mgr_n_lock(10);
        c.set_block_buf_size(block_num*block_size as u64);
        drop(c);

        let init_fdesc = init_datastore(dspath, block_size);

        let block_mgr = Rc::new(BlockMgr::new(conf.clone()).expect("Failed to create instance"));

        let ba = BlockAllocator::new(conf.clone(), block_mgr.clone());
        let ss = ba.get_shared_state();
        let ba = BlockAllocator::from_shared_state(block_mgr.clone(), ss).expect("Failed to get block allocator");

        let checkpoint_csn = 34544;

        let block_id = BlockId::init(3, 1, 1);
        let block = ba.get_free().unwrap();
        assert_eq!(block_id, block.get_id());
        drop(block);
        ba.set_free_info_used(&block_id).expect("Failed to set block bit");
        let block = ba.get_free().unwrap();
        assert_eq!(BlockId::init(3, 1, 2), block.get_id());
        drop(block);
        ba.set_free_info_free(&block_id).expect("Failed to set block bit");
        let block = ba.get_free().unwrap();
        assert_eq!(block_id, block.get_id());
        drop(block);


        let block_id = BlockId::init(3, 3, 1);
        let block = ba.allocate_block().expect("Failed to allocate block");
        assert_eq!(block_id, block.get_id());

        let block = ba.get_free_checkpoint_block(checkpoint_csn).expect("Failed to get checkpoint block");
        assert_eq!(BlockId::init(0, 0, 2), block.get_id());
        drop(block);
        let block = ba.get_free_checkpoint_block(checkpoint_csn+1).expect("Failed to get checkpoint block");
        assert_eq!(BlockId::init(1, 0, 3), block.get_id());
    }
}
