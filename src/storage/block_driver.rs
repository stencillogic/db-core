/// Interface for storage

use crate::common::errors::Error;
use crate::common::defs::ObjectId;
use crate::common::defs::BlockId;
use crate::common::defs::SharedSequences;
use crate::system::config::ConfigMt;
use crate::storage::datastore::FileType;
use crate::storage::version_store::VersionStore;
use crate::storage::version_store::VersionStoreSharedState;
use crate::storage::checkpoint_store::CheckpointStore;
use crate::block_mgr::allocator::BlockAllocator;
use crate::block_mgr::allocator::BlockAllocatorSharedState;
use crate::block_mgr::block_mgr::BlockMgr;
use crate::block_mgr::block::BlockLocked;
use crate::block_mgr::block::BlockLockedMut;
use crate::block_mgr::block::DataBlock;
use crate::block_mgr::block::DataBlockEntry;
use crate::block_mgr::block::BasicBlock;
use crate::block_mgr::block::ENTRY_HEADER_LEN;
use crate::block_mgr::block::VERENTRY_HEADER_LEN;
use crate::buf_mgr::buf_mgr::BlockType;
use crate::buf_mgr::buf_writer::BufWriter;
use std::time::Duration;
use std::rc::Rc;
use std::ops::Deref;


/// Parts of BlockStorageDriver that can be shared between threads.
pub struct BlockStorageSharedState {
    buf_writer:     BufWriter,
    block_mgr:      BlockMgr,
    csns:           SharedSequences,
    ba_ss:          BlockAllocatorSharedState,
    vs_ss:          VersionStoreSharedState,
}


/// Cursor keeps track of the next write position: block, entry, offset,
///and some other information, like tsn, and csn
#[derive(Debug)]
pub struct Cursor {
    block_id:   BlockId,
    entry_id:   u16,
    entry_pos:  u16,
    appending:  bool,
    tsn:        u64,
    csn:        u64,
}


/// Implementation of StorageDriver using block store.
pub struct BlockStorageDriver {
    block_mgr:          Rc<BlockMgr>,
    block_allocator:    Rc<BlockAllocator>,
    version_store:      VersionStore,
    checkpoint_store:   CheckpointStore,
    buf_writer:         BufWriter,
    csns:               SharedSequences
}


impl<'b> BlockStorageDriver {

    pub fn new(conf: ConfigMt, csns: SharedSequences) -> Result<Self, Error> {
        let version_retain_time = *conf.get_conf().get_version_retain_time();
        let tran_num = *conf.get_conf().get_tran_mgr_n_tran();
        let writer_num = *conf.get_conf().get_writer_num();

        let block_mgr = Rc::new(BlockMgr::new(conf.clone())?);

        let block_allocator = Rc::new(BlockAllocator::new(conf.clone(), block_mgr.clone()));

        let version_store = VersionStore::new(block_mgr.clone(), block_allocator.clone(), Duration::from_secs(version_retain_time as u64), tran_num as usize)?;
        let checkpoint_store = CheckpointStore::new(block_mgr.clone(), block_allocator.clone())?;

        let buf_writer = BufWriter::new(&block_mgr, writer_num as usize)?;

        Ok(BlockStorageDriver{
            block_mgr,
            block_allocator,
            version_store,
            checkpoint_store,
            buf_writer,
            csns,
        })
    }

    /// Build instance of storage driver using shared state.
    pub fn from_shared_state(ss: BlockStorageSharedState) -> Result<Self, Error> {

        let BlockStorageSharedState { buf_writer, block_mgr, csns, ba_ss, vs_ss } = ss;

        let block_mgr = Rc::new(block_mgr);

        let block_allocator = Rc::new(BlockAllocator::from_shared_state(block_mgr.clone(), ba_ss)?);

        let version_store = VersionStore::from_shared_state(block_mgr.clone(), block_allocator.clone(), vs_ss)?;
        let checkpoint_store = CheckpointStore::new(block_mgr.clone(), block_allocator.clone())?;

        Ok(BlockStorageDriver{
            block_mgr,
            block_allocator,
            version_store,
            checkpoint_store,
            buf_writer,
            csns,
        })
    }

    pub fn get_shared_state(&self) -> Result<BlockStorageSharedState, Error> {
        Ok(BlockStorageSharedState {
            buf_writer:     self.buf_writer.clone(),
            block_mgr:      self.block_mgr.deref().clone()?,
            csns:           self.csns.clone(),
            ba_ss:          self.block_allocator.get_shared_state(),
            vs_ss:          self.version_store.get_shared_state(),
        })
    }


    /// Return tsn if object is currently being locked / written by other transaction.
    pub fn is_locked(&self, obj_id: &ObjectId) -> Result<Option<u64>, Error> {
        let block_id = BlockId::from_obj(obj_id);
        let block = self.block_mgr.get_block(&block_id)?;
        Self::check_object_exists(&block, obj_id.entry_id)?;
        let entry = block.get_entry(obj_id.entry_id)?;
        Ok(Some(entry.get_tsn()))
    }

    /// Create a new object.
    pub fn create(&self, tsn: u64, csn: u64, initial_size: usize) -> Result<(ObjectId, Cursor), Error> {
        let (mut block, _) = self.get_free_mut(initial_size)?;

        let block_id = block.get_id();
        let entry_sz = std::cmp::min(block.get_free_space() - VERENTRY_HEADER_LEN, initial_size + ENTRY_HEADER_LEN);
        let mut entry = block.add_entry(entry_sz);
        let entry_id = entry.get_id();

        self.version_store.create_version(&block_id, &mut entry, tsn)?;

        entry.set_tsn(tsn);
        entry.set_csn(csn);
        entry.set_start(true);
        entry.set_end(true);

        if block.get_used_space() >= self.block_mgr.block_fill_size() {
            self.block_allocator.set_free_info_used(&block.get_id())?;
        }

        Ok((ObjectId {
            file_id: block_id.file_id,
            extent_id: block_id.extent_id,
            block_id: block_id.block_id,
            entry_id,
        },
        Cursor {
            block_id,
            entry_id,
            entry_pos: 0,
            appending: initial_size == 0,
            tsn,
            csn,
        }))
    }

    /// Delete existing object.
    /// Each entry of the object is marked as deleted. The data itself can be physically deleted
    /// when there is guarantie that there can be no readers.
    pub fn delete(&self, obj_id: &ObjectId, tsn: u64, csn: u64) -> Result<u64, Error>  {

        let mut next_block_id = BlockId::from_obj(obj_id);
        let mut next_entry_id = obj_id.entry_id;

        let (b, c) = self.get_block_mut(&next_block_id)?;
        let mut block = b;
        let mut checkpoint_csn = c;

        Self::check_object_exists(block.immut(), next_entry_id)?;

        let mut entry = block.get_entry_mut(next_entry_id)?;

        Self::check_not_deleted(&entry.immut())?;

        loop {
            self.version_store.create_version(&next_block_id, &mut entry, tsn)?;

            entry.set_tsn(tsn);
            entry.set_csn(csn);
            entry.set_deleted(true);

            if entry.is_end() {
                break;
            }

            let (block_id, entry_id) = entry.get_continuation();

            next_block_id = block_id;
            next_entry_id = entry_id;

            let (b, c) = self.get_block_mut(&next_block_id)?;
            block = b;
            checkpoint_csn = c;
            entry = block.get_entry_mut(next_entry_id)?;
        };

        Ok(checkpoint_csn)
    }

    fn check_object_exists(block: &BlockLocked<DataBlock>, entry_id: u16) -> Result<(), Error> {
        if !block.has_entry(entry_id) {
            Err(Error::object_does_not_exist())
        } else {
            let entry = block.get_entry(entry_id)?;

            if !entry.is_start() {
                Err(Error::object_does_not_exist())
            } else {
                Ok(())
            }
        }
    }

    fn check_not_deleted(entry: &DataBlockEntry) -> Result<(), Error> {
        if entry.is_deleted() {
            Err(Error::object_is_deleted())
        } else {
            Ok(())
        }
    }

    /// Begin writing to an existing object.
    pub fn begin_write(&self, obj_id: &ObjectId, tsn: u64, csn: u64) -> Result<Cursor, Error> {
        let block_id = BlockId::from_obj(obj_id);
        let (mut block, _) = self.get_block_mut(&block_id)?;

        Self::check_object_exists(&block.immut(), obj_id.entry_id)?;

        let mut entry = block.get_entry_mut(obj_id.entry_id)?;

        Self::check_not_deleted(&entry.immut())?;

        if entry.get_tsn() != tsn {
            self.version_store.create_version(&block_id, &mut entry, tsn)?;
            entry.set_tsn(tsn);
            entry.set_csn(csn);
        }

        Ok(Cursor {
            block_id,
            entry_id: obj_id.entry_id,
            entry_pos: 0,
            appending: false,
            tsn,
            csn,
        })
    }

    /// Begin reading an existing object.
    pub fn begin_read(&self, obj_id: &ObjectId, tsn: u64, csn: u64) -> Result<Cursor, Error> {
        let block_id = BlockId::from_obj(obj_id);
        let block = self.block_mgr.get_block(&block_id)?;

        Self::check_object_exists(&block, obj_id.entry_id)?;

        let c = Cursor {
            block_id,
            entry_id: obj_id.entry_id,
            entry_pos: 0,
            appending: false,
            tsn,
            csn,
        };

        let (b, e, v) = self.find_entry_version(&c)?;
        let block = b;
        let entry_id = e;
        let entry = if v {block.get_version_entry(entry_id)?.to_inner_entry()} else {block.get_entry(entry_id)?};

        Self::check_not_deleted(&entry)?;

        Ok(c)
    }

    /// Write data to object opened for write.
    pub fn write(&'b self, c: &'b mut Cursor, data: &[u8]) -> Result<(usize, u64), Error> {
        if c.appending {
            return self.write_append(c, data);
        } else {
            let (block, checkpoint_csn) = self.get_block_mut(&c.block_id)?;
            let entry = block.get_entry(c.entry_id)?;

            if c.entry_pos < entry.data_size() {
                return self.write_ins(c, data, block, c.entry_id, checkpoint_csn);
            } else {
                if entry.is_end() {
                    c.appending = true;
                    drop(block);
                    return self.write_append(c, data);
                } else {
                    let (block_id, entry_id) = entry.get_continuation();
                    c.block_id = block_id;
                    c.entry_id = entry_id;
                    c.entry_pos = 0;
                    let (block, checkpoint_csn) = self.get_block_mut(&c.block_id)?;
                    return self.write_ins(c, data, block, c.entry_id, checkpoint_csn);
                }
            }
        }
    }

    /// Return writing destination for the case when we are in the middle of object.
    fn write_ins(&'b self, 
                 c: &'b mut Cursor, 
                 data: &[u8], 
                 mut block: BlockLockedMut<'b, DataBlock<'b>>, 
                 entry_id: u16, 
                 checkpoint_csn: u64) -> Result<(usize, u64), Error>
    {
        let start_pos = c.entry_pos;
        let block_id = block.get_id();
        let mut entry = block.get_entry_mut(entry_id)?;
        let remaining_entry_space = (entry.data_size() - c.entry_pos) as usize;
        let written;
        if remaining_entry_space > data.len() {
            written = data.len();
            c.entry_pos += data.len() as u16;
        } else {
            written = remaining_entry_space;
            c.entry_pos = entry.data_size();
        }

        if entry.get_tsn() != c.tsn {
            self.version_store.create_version(&block_id, &mut entry, c.tsn)?;
            entry.set_tsn(c.tsn);
            entry.set_csn(c.csn);
        }

        entry.mut_slice(start_pos, c.entry_pos).copy_from_slice(&data[..written]);
        Ok((written, checkpoint_csn))
    }

    /// Return writing destination for the case when we have reached the end of the object and need
    /// to append now.
    fn write_append(&'b self, c: &'b mut Cursor, data: &[u8]) -> Result<(usize, u64), Error>{
        let (mut cur_block, checkpoint_csn) = self.get_block_mut(&c.block_id)?;
        let cur_block_id = cur_block.get_id();
        let free = cur_block.get_free_space();

        if free > VERENTRY_HEADER_LEN {
            // try extending current entry
            let extend_sz = std::cmp::min(data.len(), free - VERENTRY_HEADER_LEN);
            let cbid = cur_block.get_id();
            let mut entry = cur_block.extend_entry(c.entry_id, extend_sz)?;

            if entry.get_tsn() != c.tsn {
                self.version_store.create_version(&cbid, &mut entry, c.tsn)?;
                entry.set_tsn(c.tsn);
                entry.set_csn(c.csn);
            }

            let start_pos = c.entry_pos;
            c.entry_pos += extend_sz as u16;

            entry.mut_slice(start_pos, c.entry_pos).copy_from_slice(&data[..extend_sz]);

            if cur_block.get_used_space() >= self.block_mgr.block_fill_size() {
                self.block_allocator.set_free_info_used(&cur_block.get_id())?;
            }

            Ok((extend_sz, checkpoint_csn))

        } else {
            // get new block
            let (mut new_block, checkpoint_csn) = self.get_free_mut(data.len())?;
            let new_block_id = new_block.get_id();

            let mut prev_entry = cur_block.get_entry_mut(c.entry_id)?;
            if prev_entry.get_tsn() != c.tsn {
                self.version_store.create_version(&cur_block_id, &mut prev_entry, c.tsn)?;
                prev_entry.set_tsn(c.tsn);
            }
            prev_entry.set_end(false);

            let entry_sz = std::cmp::min(new_block.get_free_space() - VERENTRY_HEADER_LEN, data.len() + ENTRY_HEADER_LEN);
            let mut entry = new_block.add_entry(entry_sz);

            self.version_store.create_version(&new_block_id, &mut entry, c.tsn)?;

            entry.set_tsn(c.tsn);
            entry.set_csn(c.csn);
            entry.set_end(true);

            prev_entry.set_continuation(&new_block_id, entry.get_id());

            c.block_id = new_block_id;
            c.entry_id = entry.get_id();
            c.entry_pos = entry.data_size();

            entry.mut_slice(0, c.entry_pos).copy_from_slice(&data[..c.entry_pos as usize]);

            if new_block.get_used_space() >= self.block_mgr.block_fill_size() {
                self.block_allocator.set_free_info_used(&new_block.get_id())?;
            }

            Ok((c.entry_pos as usize, checkpoint_csn))
        }
    }

    /// Returns entry version which contains data as of certain moment in time. Since dirty reads
    /// are not allowed we should not see changes made by other transactions, and thus must look up
    /// in the version store for the latest commited changes (not later than certain csn).
    /// Or if the entry was changed by the current transaction then we can return those changes.
    fn find_entry_version(&self, c: &Cursor) -> Result<(BlockLocked<DataBlock>, u16, bool), Error> {
        let (b, e) = (c.block_id, c.entry_id);
        let mut block_id = b;
        let mut entry_id = e;

        let block = self.block_mgr.get_block(&block_id)?;
        let entry = block.get_entry(entry_id)?;

        if entry.get_csn() <= c.csn || entry.get_tsn() == c.tsn {
            return Ok((block, entry_id, false));
        }

        loop {
            let (b, e) = entry.get_prev_version_ptr();
            block_id = b;
            entry_id = e;

            let block = self.block_mgr.get_versioning_block(&block_id)?;
            let ver_entry = block.get_version_entry(entry_id)?;
            let entry = ver_entry.inner_entry();

            if entry.get_csn() <= c.csn || entry.get_tsn() == c.tsn {
                return Ok((block, entry_id, true));
            }
        }
    }

    /// Read data from an object opened for read.
    pub fn read(&self, c: &mut Cursor, buf: &mut [u8]) -> Result<usize, Error> {
        let mut remaining = buf.len();
        let (b, e, v) = self.find_entry_version(c)?;
        let mut block = b;
        let mut entry_id = e;
        let mut entry = if v {block.get_version_entry(entry_id)?.to_inner_entry()} else {block.get_entry(entry_id)?};

        while remaining > 0 {

            let l = std::cmp::min(remaining, (entry.data_size() - c.entry_pos) as usize) as u16;
            let buf_pos = buf.len() - remaining;
            let dst = &mut buf[buf_pos..buf_pos + l as usize];
            let src = entry.slice(c.entry_pos, c.entry_pos + l);
            dst.copy_from_slice(src);
            c.entry_pos += l;
            remaining -= l as usize;

            if c.entry_pos == entry.data_size() {

                if entry.is_end() {
                    c.appending = true;
                    return Ok(buf.len() - remaining);
                }

                let (block_id, eid) = entry.get_continuation();
                c.block_id = block_id;
                c.entry_id = eid;
                c.entry_pos = 0;
                if remaining > 0 {
                    let (b, e, v) = self.find_entry_version(c)?;
                    block = b;
                    entry_id = e;
                    entry = if v {block.get_version_entry(entry_id)?.to_inner_entry()} else {block.get_entry(entry_id)?};
                }
            }
        }

        Ok(buf.len())
    }

    /// Seek inside object from current position forward.
    pub fn seek(&self, c: &mut Cursor, pos: u64) -> Result<u64, Error> {
        let mut remaining = pos;
        let (b, e, v) = self.find_entry_version(c)?;
        let mut block = b;
        let mut entry_id = e;
        let mut entry = if v {block.get_version_entry(entry_id)?.to_inner_entry()} else {block.get_entry(entry_id)?};

        while remaining > 0 {

            let l = std::cmp::min(remaining, (entry.data_size() - c.entry_pos) as u64) as u16;
            remaining -= l as u64;
            c.entry_pos += l;

            if c.entry_pos == entry.data_size() {

                if entry.is_end() {
                    c.appending = true;
                    return Ok(pos - remaining);
                }

                let (block_id, eid) = entry.get_continuation();
                c.block_id = block_id;
                c.entry_id = eid;
                c.entry_pos = 0;
                if remaining > 0 {
                    let (b, e, v) = self.find_entry_version(c)?;
                    block = b;
                    entry_id = e;
                    entry = if v {block.get_version_entry(entry_id)?.to_inner_entry()} else {block.get_entry(entry_id)?};
                }
            }
        }

        Ok(pos)
    }

    /// Go through the version store and find all modifications done by the corresponding
    /// transaction denoted with tsn parameter, and restore data entries in the main data 
    /// store using versions from the version store.
    pub fn rollback_transaction(&self, tsn: u64) -> Result<(), Error> {
        let mut trn_entry_iter = self.version_store.get_iter_for_tran(tsn)?;

        while let Some((tgt_block_id, _tgt_entry_id, mut ver_block, ver_entry_id)) = trn_entry_iter.get_next()? {
            let mut ver_entry = ver_block.get_version_entry_mut(ver_entry_id)?;
            let (mut block, _) = self.get_block_mut(&tgt_block_id)?;
            block.restore_entry(&ver_entry.inner_entry())?;
        }

        Ok(())
    }


    /// Go through checkpoint store blocks and restore all the blocks in the main data store to the
    /// state as of before the first modification has been made under the last completed checkpoint.
    /// So we could then replay all modifications from the transaction log with corresponding
    /// checkpoint sequence number or larger sequence number (all the changes happend after the
    /// checkpoint).
    pub fn restore_checkpoint(&self, checkpoint_csn: u64) -> Result<(), Error> {
        let mut iter = self.checkpoint_store.get_iter(checkpoint_csn)?;
        while let Some((block_id, block)) = iter.get_next()? {
            let mut tgt_block = self.block_mgr.get_block_mut(&block_id)?;
            tgt_block.copy_from(&block);
            let original_block_id = tgt_block.get_original_id();
            tgt_block.set_id(original_block_id);
            self.block_mgr.write_block(&mut tgt_block)?;
            self.block_mgr.set_dirty(tgt_block.get_buf_idx(), false);
        }

        Ok(())
    }


    /// get block with certain id for writing.
    fn get_block_mut(&self, block_id: &BlockId) -> Result<(BlockLockedMut<DataBlock>, u64), Error> {
        let mut block = self.block_mgr.get_block_mut(block_id)?;

        let checkpoint_csn = self.process_checkpoint(&mut block)?;

        Ok((block, checkpoint_csn))
    }

    /// Get a block with some free space or allocate a new block for writing. 
    /// The process of retreiving free block in general is the following:
    /// 1. Go through free info blocks. First at file header level, then at extent level.
    /// 2. In the free info block find block marked as free.
    /// 3. Get the block marked as free and check if it actually has free space. If not then
    ///    continue search.
    /// 4. If free block was not found then try to allocate a new extent and get free block in that
    ///    extent.
    /// 5. If requested_size is larger than the block size then don't search for free block, rather
    ///    allocate a new one at once.
    fn get_free_mut(&self, requested_size: usize) -> Result<(BlockLockedMut<DataBlock>, u64), Error> {
        let mut block = if requested_size > self.block_mgr.block_fill_size() {
            self.block_allocator.allocate_block()?
        } else {
            self.block_allocator.get_free()?
        };

        let checkpoint_csn = self.process_checkpoint(&mut block)?;

        Ok((block, checkpoint_csn))
    }

    /// When we want to modify a data block we need to do some actions related to checkpointing:
    /// 1. Check if a new checkpoint has been triggered first. 
    /// 2. In case of the new checkpoint the old checkpointed block copy must be first written
    ///    to disk if it is not yet written along with the block itself if it is dirty.
    /// 3. If the block is being modified for the first time in the current checkpoint then we need
    ///    to create block copy and add it to checkpoint store. This copy will represent the
    ///    original block state as of checkpoint start and will be used to reset the original block
    ///    contents in the main data store after system restart.
    fn process_checkpoint(&self, block: &mut BlockLockedMut<DataBlock>) -> Result<u64, Error> {
        let checkpoint_csn = self.csns.checkpoint_csn.get_cur();

        if block.get_checkpoint_csn() == 0 {
            block.set_checkpoint_csn(checkpoint_csn);
        } else {
            if block.get_checkpoint_csn() < checkpoint_csn {
                self.buf_writer.write_data_block(block, &self.block_mgr, true)?;

                block.set_checkpoint_csn(checkpoint_csn);
                self.checkpoint_store.add_block(&block, checkpoint_csn)?;
            }
        }

        Ok(checkpoint_csn)
    }


    /// This function completes checkpoint.
    /// It goes through the memory buffer in search of dirty blocks of 
    /// the main data store.
    /// If it finds a dirty data block it locks the block and writes the checkpoint copy of the
    /// block from the previous checkpoint if it is not yet written and the block itself to disk.
    /// After all blocks have been written we can be sure that any new changes in data store will
    /// appear with the new checkpoint sequence number, and hence can be replayed starting with this
    /// checkpoint.
    pub fn checkpoint(&self, checkpoint_csn: u64) -> Result<(), Error> {

        let mut iter = self.block_mgr.get_iter();

        while let Some(desc) = iter.next() {
            if desc.dirty && desc.block_type == BlockType::DataBlock {
                let mut block = self.block_mgr.get_block_by_idx(desc.id, desc.block_id, desc.block_type).unwrap();

                if block.get_id() == desc.block_id {
                    if block.get_checkpoint_csn() < checkpoint_csn {
                        self.buf_writer.write_data_block(&mut block, &self.block_mgr, false)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn finish_tran(&self, tsn: u64) {
        self.version_store.finish_tran(tsn)
    }

    pub fn terminate(self) {
        let BlockStorageDriver {
            block_mgr: _,
            block_allocator: _,
            version_store: _,
            checkpoint_store: _,
            buf_writer,
            csns: _
        } = self;

        buf_writer.terminate();
    }

    /// Add a new file to datastore.
    pub fn add_datafile(&self, file_type: FileType, extent_size: u16, extent_num: u16, max_extent_num: u16) -> Result<(), Error> {
        self.block_mgr.add_datafile(file_type, extent_size, extent_num, max_extent_num)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::datastore::DataStore;
    use crate::storage::datastore::FileType;
    use crate::storage::datastore::FileDesc;
    use crate::storage::datastore::FileState;
    use crate::block_mgr::block::BasicBlock;
    use crate::common::defs::Sequence;
    use crate::common::defs::SharedSequences;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;


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

    fn write_full_slice(bd: &BlockStorageDriver, cursor: &mut Cursor, data: &[u8]) {
        let mut written = 0;
        while written < data.len() {
            let (w, _checkpoint_csn) = bd.write(cursor, &data[written..]).expect("Failed to write to a block");
            written += w;
        }
    }

    fn read_full_slice(bd: &BlockStorageDriver, cursor: &mut Cursor, read_buf: &mut [u8]) {
        let mut read = 0;
        let len = read_buf.len();
        while read < len {
            let r = bd.read(cursor, &mut read_buf[read..len]).expect("Failed to read");
            if r == 0 {break;}
            read += r;
        }
    }

    #[test]
    fn test_block_driver() {
        let dspath = "/tmp/test_block_driver_5645379";
        let block_size = 8192;
        let block_num = 10000;
        let csn =               Sequence::new(1);
        let latest_commit_csn = Arc::new(AtomicU64::new(1));
        let checkpoint_csn =    Sequence::new(1);
        let csns = SharedSequences {
                csn,
                latest_commit_csn,
                checkpoint_csn,
            };

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

        let bd = BlockStorageDriver::new(conf.clone(), csns.clone()).expect("Block driver not created");
        let ss = bd.get_shared_state().expect("Failed to get shared state");
        let bd = BlockStorageDriver::from_shared_state(ss).expect("Block driver not created");

        let obj = ObjectId::init(3,1,1,0);
        assert!(bd.is_locked(&obj).is_err());


        // Create an object and write to it, and then read

        let tsn = 123;
        let csn = csns.csn.get_next();
        let initial_size = 100;
        let (obj, mut cursor) = bd.create(tsn, csn, initial_size).expect("Failed to create object");
        let mut total = 3;
        let (written, checkpoint_csn) = bd.write(&mut cursor, b"123").expect("Failed to write to a block");
        assert_eq!(written, total);
        let data = b"12345678901234567890";
        for i in 0..1000 {
            write_full_slice(&bd, &mut cursor, data);
            total += data.len();
        }
        assert_eq!(total, 3 + 20*1000);

        let csn = csns.csn.get_next();
        let mut cursor = bd.begin_write(&obj, tsn, csn).expect("Failed to begin write");
        let pos = 999*20 + 3;
        let ret_pos = bd.seek(&mut cursor, pos).expect("Failed to seek to position");
        assert_eq!(ret_pos, pos);

        let data2 = b"asdfghjkl;";
        for _ in 0..100 {
            write_full_slice(&bd, &mut cursor, data2);
        }

        let read_buf = &mut [0;20];
        let mut cursor = bd.begin_read(&obj, tsn, csn).expect("Failed to start reading");
        read_full_slice(&bd, &mut cursor, &mut read_buf[0..3]);
        assert_eq!(&read_buf[0..3], b"123");

        for i in 0..999 {
            let read_buf = &mut [0;20];
            read_full_slice(&bd, &mut cursor, read_buf);
            assert_eq!(&read_buf[0..data.len()], data);
        }

        for i in 0..100 {
            let read_buf = &mut [0;10];
            read_full_slice(&bd, &mut cursor, read_buf);
            assert_eq!(read_buf, data2);
        }

        let read_buf = &mut [0;10];
        let r = bd.read(&mut cursor, read_buf).expect("Failed to read");
        assert_eq!(0, r);

        bd.finish_tran(tsn);
        let last_committed_csn = csn;


        // Check isolation

        let tsn = 124;
        let csn = csns.csn.get_next();
        let mut cursor = bd.begin_write(&obj, tsn, csn).expect("Failed to begin write");
        let data3 = b"-=-=-=++++++";
        write_full_slice(&bd, &mut cursor, data3);
        let seek_pos = 999*20 + 3 - data3.len() as u64;
        let ret_pos = bd.seek(&mut cursor, seek_pos).expect("Failed to seek to position");
        assert_eq!(ret_pos, seek_pos);
        write_full_slice(&bd, &mut cursor, data3);

        let tsn2 = 125;
        let mut cursor = bd.begin_read(&obj, tsn2, last_committed_csn).expect("Failed to start reading");
        let read_buf = &mut [0;3];
        read_full_slice(&bd, &mut cursor, read_buf);
        assert_eq!(read_buf, b"123");
        let seek_pos = 999*20;
        let ret_pos = bd.seek(&mut cursor, seek_pos).expect("Failed to seek to position");
        assert_eq!(ret_pos, seek_pos);
        read_full_slice(&bd, &mut cursor, read_buf);
        assert_eq!(read_buf, b"asd");

        let mut cursor = bd.begin_read(&obj, tsn2, csn).expect("Failed to start reading");
        let read_buf = &mut [0;3];
        read_full_slice(&bd, &mut cursor, read_buf);
        assert_eq!(read_buf, b"-=-");
        let ret_pos = bd.seek(&mut cursor, seek_pos).expect("Failed to seek to position");
        assert_eq!(ret_pos, seek_pos);
        read_full_slice(&bd, &mut cursor, read_buf);
        assert_eq!(read_buf, b"-=-");


        // Rollback and see both transactions see the same previous value.

        bd.rollback_transaction(tsn).expect("Failed to rollback changes");

        let mut cursor = bd.begin_read(&obj, tsn, csn).expect("Failed to start reading");
        let read_buf = &mut [0;3];
        read_full_slice(&bd, &mut cursor, read_buf);
        assert_eq!(read_buf, b"123");
        let ret_pos = bd.seek(&mut cursor, seek_pos).expect("Failed to seek to position");
        assert_eq!(ret_pos, seek_pos);
        read_full_slice(&bd, &mut cursor, read_buf);
        assert_eq!(read_buf, b"asd");

        let mut cursor = bd.begin_read(&obj, tsn2, last_committed_csn).expect("Failed to start reading");
        let read_buf = &mut [0;3];
        read_full_slice(&bd, &mut cursor, read_buf);
        assert_eq!(read_buf, b"123");
        let ret_pos = bd.seek(&mut cursor, seek_pos).expect("Failed to seek to position");
        assert_eq!(ret_pos, seek_pos);
        read_full_slice(&bd, &mut cursor, read_buf);
        assert_eq!(read_buf, b"asd");


        // Increase checkpoint_csn and test addition to checkpoint store.

        let checkpoint_csn = csns.checkpoint_csn.get_next();
        let tsn = 126;
        let csn = csns.csn.get_next();
        let mut cursor = bd.begin_write(&obj, tsn, csn).expect("Failed to begin write");
        let data4 = b"123";
        write_full_slice(&bd, &mut cursor, data4);
        let seek_pos = 999*20;
        let ret_pos = bd.seek(&mut cursor, seek_pos).expect("Failed to seek to position");
        assert_eq!(ret_pos, seek_pos);
        write_full_slice(&bd, &mut cursor, data4);

        bd.finish_tran(tsn);
        let last_committed_csn = csn;


        // Delete object.

        let tsn3 = 127;
        let csn = csns.csn.get_next();
        let _checkpoint_csn = bd.delete(&obj, tsn3, csn).expect("Failed to delete an object");

        let mut cursor = bd.begin_read(&obj, tsn2, last_committed_csn).expect("Failed to start reading");
        let read_buf = &mut [0;3];
        read_full_slice(&bd, &mut cursor, read_buf);
        assert_eq!(read_buf, b"123");

        bd.finish_tran(tsn3);
        let last_committed_csn = csn;

        assert!(bd.begin_read(&obj, tsn2, last_committed_csn).is_err());


        // Perform checkpoint, then restore from it and see the deleted object is back.

        let checkpoint_csn = csns.checkpoint_csn.get_next();
        bd.checkpoint(checkpoint_csn).expect("Checkpoint executed successfully");

        bd.restore_checkpoint(checkpoint_csn-1).expect("Checkpoint restored successfully");

        let mut cursor = bd.begin_read(&obj, tsn2, last_committed_csn).expect("Failed to start reading");
        let read_buf = &mut [0;3];
        read_full_slice(&bd, &mut cursor, read_buf);

        assert_eq!(read_buf, b"123");

        bd.terminate();
    }
}
