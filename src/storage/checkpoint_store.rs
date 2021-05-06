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
            cur_extent_id: (checkpoint_csn & 0x1) as u16 + 1,   // avoid extent 0 by adding 1
            cur_block_id: 0,
            checkpoint_csn,
            cur_file_idx: 0,
        }
    }

    pub fn get_next(&mut self) -> Result<Option<(BlockId, BlockLocked<DataBlock>)>, Error> {
        while let Some(block_id) = self.calc_next_block_id() {
            let block = self.block_mgr.get_block(&block_id)?;
            if block.get_checkpoint_csn() == self.checkpoint_csn {
                return Ok(Some((block.get_original_id(), block)));
            } else {
                break;
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


#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::datastore::DataStore;
    use crate::storage::datastore::FileType;
    use crate::storage::datastore::FileDesc;
    use crate::storage::datastore::FileState;
    use crate::buf_mgr::buf_writer::BufWriter;
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
            extent_num:     3,
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

    fn check_added_num(expected_cnt: usize, cs: &CheckpointStore, checkpoint_csn: u64) {
        let mut block_num = 0;

        let mut iter = cs.get_iter(checkpoint_csn).expect("Failed to get iterator");

        while let Some(block) = iter.get_next().expect("Failed to get next block") {
            block_num += 1;
        }

        assert_eq!(expected_cnt, block_num);
    }

    fn add_block(file_id: u16, extent_id: u16, block_id: u16, cs: &CheckpointStore, block_mgr: &BlockMgr, checkpoint_csn: u64) -> usize {
        let block_id = BlockId::init(file_id, extent_id, block_id);
        let mut block = block_mgr.get_block_mut(&block_id).expect("Failed to get block");
        let ret = block.get_buf_idx();
        block.set_checkpoint_csn(checkpoint_csn);
        cs.add_block(&block, checkpoint_csn).expect("Failed to add block");
        ret
    }

    fn flush_blocks(bw: &BufWriter, block_mgr: &BlockMgr, idxs: &[usize]) {

        bw.wake_writers();

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
    }

    #[test]
    fn test_checkpoint_store() {
        let dspath = "/tmp/test_checkpoint_store_68343467";
        let block_size = 8192;
        let block_num = 100;
        let writer_num = 2;

        let mut conf = ConfigMt::new();
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
        let buf_writer = BufWriter::new(&block_mgr, writer_num).expect("Failed to create buf writer");
        let cs = CheckpointStore::new(block_mgr.clone(), block_allocator).expect("Failed to create checkpoint store");

        let mut checkpoint_csn = 1;
        let add_cnt = 7;
        let mut idxs = vec![];

        for i in 0..add_cnt {
            idxs.push(add_block(3, 1, 1 + i, &cs, &block_mgr, checkpoint_csn));
        }
        flush_blocks(&buf_writer, &block_mgr, &idxs);

        check_added_num(add_cnt as usize, &cs, checkpoint_csn);

        idxs.truncate(0);
        idxs.push(add_block(3, 1, 1 + add_cnt, &cs, &block_mgr, checkpoint_csn));
        flush_blocks(&buf_writer, &block_mgr, &idxs);

        // emulate restart, otherwise buf can contain unsynced block data previously read from checkpint store.
        drop(cs);
        buf_writer.terminate();
        if let Ok(bm) = Rc::try_unwrap(block_mgr) {
            drop(bm);
        } else {
            panic!("Failed to unwrap block mgr");
        }

        let block_mgr = Rc::new(BlockMgr::new(conf.clone()).expect("Failed to create block mgr"));
        let block_allocator = Rc::new(BlockAllocator::new(conf.clone(), block_mgr.clone()));
        let buf_writer = BufWriter::new(&block_mgr, writer_num).expect("Failed to create buf writer");
        let cs = CheckpointStore::new(block_mgr.clone(), block_allocator).expect("Failed to create checkpoint store");

        check_added_num(add_cnt as usize + 1, &cs, checkpoint_csn);


        checkpoint_csn += 1;

        idxs.truncate(0);
        for i in 0..add_cnt {
            idxs.push(add_block(3, 1, 1 + i, &cs, &block_mgr, checkpoint_csn));
            idxs.push(add_block(3, 2, 1 + i, &cs, &block_mgr, checkpoint_csn));
        }

        flush_blocks(&buf_writer, &block_mgr, &idxs);

        check_added_num(add_cnt as usize * 2, &cs, checkpoint_csn);
    }
}
