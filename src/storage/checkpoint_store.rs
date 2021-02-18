/// Block storage for checkpoint restore.
/// CheckpointStore represents part of BlockStorageDriver functionality related to checkpointing.
/// Before any data block can be modifed its original state must be saved in the special block
/// checkpoint store. 
/// When system starts it searches through the transaction log for the last checkpoint
/// mark and then uses checkpoint store to restore all blocks from it. After all blocks are
/// restored database represents state as of checkpoint start, and system can read the changes from
/// the transaction log and apply them to database.
/// Each checkpoint is identified by unique checkpoint sequence number. Checkpoint can be in
/// not completed state, because when new checkpoint begins, a sequence of actions like writing all the 
/// previous checkpoint blocks to disk is required. As soon as all them done, the checkpoint is marked as
/// completed, and corresponding record is made in the transaction log. 
/// System can use only completed checkpoint for restore. That means the checkpoint
/// store keeps previous checkpoint's blocks until the new checkpoint is marked as completed. As soon
/// as the new checkpoint is completed all blocks of the previous checkpoint are discarded and space
/// can be reused by the current and future checkpoints.


use crate::common::errors::Error;
use crate::common::defs::BlockId;
use crate::storage::datastore::FileDesc;
use crate::block_mgr::block_mgr::BlockMgr;
use crate::block_mgr::allocator::BlockAllocator;
use crate::block_mgr::block::BlockLockedMut;
use crate::block_mgr::block::BlockLocked;
use crate::block_mgr::block::DataBlock;
use crate::block_mgr::block::BasicBlock;
use std::cell::RefCell;
use std::cell::Ref;
use std::rc::Rc;


#[derive(Clone)]
pub struct CheckpointStore {
    block_mgr: Rc<BlockMgr>,
    block_allocator: Rc<BlockAllocator>,
    file_info: RefCell<Vec<FileDesc>>,
}

impl CheckpointStore {

    pub fn new(block_mgr: Rc<BlockMgr>, block_allocator: Rc<BlockAllocator>) -> Result<Self, Error> {
        let file_info = RefCell::new(vec![]);
        Ok(CheckpointStore {
            block_mgr,
            block_allocator,
            file_info,
        })
    }

    /// Add block to checkpoint store.
    pub fn add_block(&self, block: &BlockLockedMut<DataBlock>, checkpoint_csn: u64) -> Result<(), Error> {
        let mut tgt_block = self.block_allocator.get_free_checkpoint_block(checkpoint_csn)?;

        tgt_block.copy_from(&block);
        tgt_block.set_original_id(block.get_id());
        self.block_mgr.set_checkpoint_block_id(block.get_buf_idx(), tgt_block.get_id());
        self.block_mgr.set_checkpoint_written(block.get_buf_idx(), false);

        Ok(())
    }

    /// Return iterator over checkpoint store blocks.
    pub fn get_iter(&self, checkpoint_csn: u64) -> Result<Iterator, Error> {
        self.block_mgr.get_checkpoint_files(&mut self.file_info.borrow_mut());
        Ok(Iterator::new(&self.block_mgr, self.file_info.borrow(), checkpoint_csn))
    }
}

/// Iterator over blocks of checkpoint store for certain checkpoint sequence number.
pub struct Iterator<'a> {
    block_mgr:      &'a BlockMgr,
    file_desc:      Ref<'a, Vec<FileDesc>>,
    cur_extent_id:  u16,
    cur_block_id:   u16,
    checkpoint_csn: u64,
    cur_file_idx:   usize,
}

impl<'a> Iterator<'a> {

    fn new(block_mgr: &'a BlockMgr, file_desc: Ref<'a, Vec<FileDesc>>, checkpoint_csn: u64) -> Self {
        Iterator {
            block_mgr,
            file_desc,
            cur_extent_id: (checkpoint_csn & 0x1) as u16,
            cur_block_id: 0,
            checkpoint_csn,
            cur_file_idx: 0,
        }
    }

    pub fn get_next(&mut self) -> Result<Option<(BlockId, BlockLocked<DataBlock>)>, Error> {
        while let Some(block_id) = self.calc_next_block_id() {
            let block = self.block_mgr.get_block(&block_id)?;
            if block.get_checkpoint_csn() > self.checkpoint_csn {
                return Ok(Some((block.get_id(), block)));
            }
        }

        Ok(None)
    }

    fn calc_next_block_id(&mut self) -> Option<BlockId> {
        self.cur_block_id += 1;
        if self.cur_block_id == self.file_desc[self.cur_file_idx].extent_size {
            self.cur_block_id = 0;
            self.cur_extent_id += 2;
            if self.cur_extent_id >= self.file_desc[self.cur_file_idx].extent_num {
                self.cur_extent_id = (self.checkpoint_csn & 0x1) as u16;
                self.cur_file_idx += 1;
                if self.cur_file_idx == self.file_desc.len() {
                    return None;
                }
            }
        }

        Some(BlockId {
            file_id: self.file_desc[self.cur_file_idx].file_id,
            extent_id: self.cur_extent_id,
            block_id: self.cur_block_id,
        })
    }
}

