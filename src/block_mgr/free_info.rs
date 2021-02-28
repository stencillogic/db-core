/// FreeInfo manages free info section of a file or extent header.


use crate::common::errors::Error;
use crate::common::defs::BlockId;
use crate::common::misc::BYTE_BITS;
use crate::common::misc::SliceToIntConverter;
use crate::system::config::ConfigMt;
use crate::block_mgr::block_mgr::BlockMgr;
use crate::block_mgr::block::FIBLOCK_HEADER_LEN;
use crate::block_mgr::block::BasicBlock;
use crate::block_mgr::block::BlockLocked;
use crate::block_mgr::block::FreeInfoSection;
use crate::block_mgr::block::FreeInfoHeaderSection;
use std::sync::Arc;
use std::sync::RwLock;
use std::ops::DerefMut;
use std::rc::Rc;


/// Shared state that can be sent to other threads.
pub struct FreeInfoSharedState {
    file_locks:     Arc<Vec<RwLock<()>>>,
    extent_locks:   Arc<Vec<RwLock<()>>>,
}


/// Free info manager instance.
pub struct FreeInfo {
    block_mgr:      Rc<BlockMgr>,
    file_locks:     Arc<Vec<RwLock<()>>>,
    extent_locks:   Arc<Vec<RwLock<()>>>,
    block_size:     u16,
}

impl FreeInfo {

    pub fn new(conf: ConfigMt, block_mgr: Rc<BlockMgr>) -> Self {
        let free_info_n_file_lock = *conf.get_conf().get_free_info_n_file_lock();
        let free_info_n_extent_lock = *conf.get_conf().get_free_info_n_extent_lock();

        let mut file_locks = Vec::with_capacity(free_info_n_file_lock as usize);
        for _ in 0..free_info_n_file_lock {
            file_locks.push(RwLock::new(()));
        }

        let mut extent_locks = Vec::with_capacity(free_info_n_extent_lock as usize);
        for _ in 0..free_info_n_extent_lock {
            extent_locks.push(RwLock::new(()));
        }

        let block_size = block_mgr.get_block_size() as u16;

        FreeInfo {
            block_mgr,
            file_locks: Arc::new(file_locks),
            extent_locks: Arc::new(extent_locks),
            block_size,
        }
    }

    /// Build instance from shared state.
    pub fn from_shared_state(block_mgr: Rc<BlockMgr>, ss: FreeInfoSharedState) -> Result<Self, Error> {
        let FreeInfoSharedState { file_locks, extent_locks } = ss;

        let block_size = block_mgr.get_block_size() as u16;

        Ok(FreeInfo {
            block_mgr,
            file_locks,
            extent_locks,
            block_size,
        })
    }

    /// Return shared state that can be sent to other threads.
    pub fn get_shared_state(&self) -> FreeInfoSharedState {
        FreeInfoSharedState {
            file_locks:     self.file_locks.clone(),
            extent_locks:   self.extent_locks.clone(),
        }
    }

    pub fn get_fi_for_file(&self, file_id: u16, fi_data: &mut FiData) -> Result<(), Error> {
        let block_id = BlockId {
            file_id,
            extent_id: 0,
            block_id: 0,
        };

        let block = self.block_mgr.get_file_header_block(&block_id)?;

        self.fill_fi_data(block, fi_data)
    }

    pub fn get_fi_for_extent(&self, file_id: u16, extent_id: u16, fi_data: &mut FiData) -> Result<(), Error> {
        let block_id = BlockId {
            file_id,
            extent_id,
            block_id: 0,
        };

        let block = self.block_mgr.get_extent_header_block(&block_id)?;

        self.fill_fi_data(block, fi_data)
    }

    pub fn add_extent(&self, file_id: u16) -> Result<(), Error> {
        let block_id = BlockId {
            file_id,
            extent_id: 0,
            block_id: 0,
        };

        let mut block = self.block_mgr.get_file_header_block_mut(&block_id)?;
        let new_size = block.fi_size() + 1;
        block.set_fi_size(new_size);
        Ok(())
    }

    pub fn set_extent_bit(&self, file_id: u16, extent_id: u16, set: bool) -> Result<(), Error> {
        let lock_id = file_id as usize % self.file_locks.len();
        let lock_holder = self.extent_locks[lock_id].write().unwrap();
        self.set_extent_bit_locked(file_id, extent_id, set)?;
        drop(lock_holder);
        Ok(())
    }

    pub fn set_block_bit(&self, block_id: &BlockId, set: bool) -> Result<(), Error> {
        // lock extent-level free info;
        // mark block as full/free;
        // increment/decrement full blocks count;
        // if all blocks are full then lock file-level free info and mark extent as full.
        // if all blocks were full and one became free then lock file-level free info and
        // mark extent as free.
        let lock_id = block_id.extent_id as usize % self.extent_locks.len();
        let lock_holder = self.extent_locks[lock_id].write().unwrap();
        let mut header_block_id = *block_id;
        header_block_id.block_id = 0;

        let block = self.block_mgr.get_extent_header_block(&header_block_id)?;
        if block_id.block_id >= block.fi_size() {
            return Err(Error::block_does_not_exist());
        }

        let fi_size = block.fi_size();
        let header_fi_size = block.fi_slice().len() as u16;

        let fi_block_id = self.calc_block_by_byte(block_id.block_id / 8, header_fi_size);
        let bid = BlockId {
            file_id: block_id.file_id,
            extent_id: block_id.extent_id,
            block_id: fi_block_id,
        };
        drop(block);

        let mut prev_full_cnt = 0;
        let mut prev_changed = false;
        if fi_block_id > 0 {
            let mut block = self.block_mgr.get_free_info_block_mut(&bid)?;
            let prev = self.set_fi_slice_bit(block.deref_mut(), block_id.block_id, header_fi_size, set);
            drop(block);

            if set != prev {
                let mut block = self.block_mgr.get_extent_header_block_mut(&bid)?;
                prev_full_cnt = block.get_full_cnt();
                block.set_full_cnt(if set {prev_full_cnt + 1} else {prev_full_cnt - 1});
                prev_changed = true;
                drop(block);
            }
        } else {
            let mut block = self.block_mgr.get_extent_header_block_mut(&bid)?;
            if set != self.set_fi_slice_bit(block.deref_mut(), block_id.block_id, header_fi_size, set) {
                prev_full_cnt = block.get_full_cnt();
                block.set_full_cnt(if set {prev_full_cnt + 1} else {prev_full_cnt - 1});
                prev_changed = true;
            }
            drop(block);
        };

        if prev_changed && ((!set && prev_full_cnt == fi_size) 
            || (set && prev_full_cnt == fi_size - 1))
        {
            // lock chained
            let file_lock_id = block_id.file_id as usize % self.file_locks.len();
            let file_lock_holder = self.file_locks[file_lock_id].write().unwrap();
            drop(lock_holder);

            self.set_extent_bit_locked(block_id.file_id, block_id.extent_id, set)?;
            drop(file_lock_holder);
        };

        Ok(())
    }


    fn fill_fi_data<T: FreeInfoHeaderSection + BasicBlock>(&self, block: BlockLocked<'_, T>, fi_data: &mut FiData) -> Result<(), Error> {
        let mut block_id = block.get_id();

        fi_data.reset();
        fi_data.set_size(block.fi_size());
        fi_data.push_slice(block.fi_slice());

        let mut remaining_bytes: i32 = (block.fi_size() as i32) / 8 - block.fi_slice().len() as i32;
        drop(block);

        while remaining_bytes > 0 {
            block_id.block_id += 1;
            let block = self.block_mgr.get_free_info_block(&block_id)?;
            fi_data.push_slice(block.fi_slice());
            remaining_bytes -= block.fi_slice().len() as i32;
            drop(block);
        }

        Ok(())
    }

    // find a block of free info by byte position
    fn calc_block_by_byte(&self, byte_pos: u16, header_fi_size: u16) -> u16 {
        if byte_pos < header_fi_size {
            0
        } else {
            (byte_pos - header_fi_size) / (self.block_size - FIBLOCK_HEADER_LEN as u16)
        }
    }

    // find bit corresponding to certain block/extent id
    fn calc_bit_for_id(&self, id: u16, header_fi_size: u16) -> (usize, u8) {
        let mut byte_pos = id / 8;
        if byte_pos >= header_fi_size {
            byte_pos -= header_fi_size;
            byte_pos /= self.block_size - FIBLOCK_HEADER_LEN as u16;
        };
        (byte_pos as usize, BYTE_BITS[id as usize % 8])
    }

    fn set_fi_slice_bit<T: FreeInfoSection>(&self, block: &mut T, extent_id: u16, header_fi_size: u16, set: bool) -> bool {
        let fi_slice = block.fi_slice_mut();
        let (byte_pos, bit) = self.calc_bit_for_id(extent_id, header_fi_size);
        let ret = 0 != (fi_slice[byte_pos] & bit);
        if set {
            fi_slice[byte_pos] |= bit;
        } else {
            fi_slice[byte_pos] &= !bit;
        };
        ret
    }

    // set a certain bit assuming extent level lock is acquired.
    fn set_extent_bit_locked(&self, file_id: u16, extent_id: u16, set: bool) -> Result<(), Error> {
        let block_id = BlockId {
            file_id,
            extent_id: 0,
            block_id: 0,
        };

        let block = self.block_mgr.get_file_header_block_mut(&block_id)?;
        if extent_id >= block.fi_size() {
            return Err(Error::extent_does_not_exist());
        }

        let header_fi_size = block.fi_slice().len() as u16;

        let fi_block_id = self.calc_block_by_byte(extent_id / 8, header_fi_size);
        let bid = BlockId {
            file_id,
            extent_id: 0,
            block_id: fi_block_id,
        };
        drop(block);

        let prev_full_cnt;
        if fi_block_id > 0 {
            let mut block = self.block_mgr.get_free_info_block_mut(&bid)?;
            let prev_set = self.set_fi_slice_bit(block.deref_mut(), extent_id, header_fi_size, set);
            drop(block);

            if prev_set != set {
                let mut block = self.block_mgr.get_file_header_block_mut(&bid)?;
                prev_full_cnt = block.get_full_cnt();
                block.set_full_cnt(if set {prev_full_cnt + 1} else {prev_full_cnt - 1});
                drop(block);
            }
        } else {
            let mut block = self.block_mgr.get_file_header_block_mut(&bid)?;
            if set != self.set_fi_slice_bit(block.deref_mut(), extent_id, header_fi_size, set) {
                prev_full_cnt = block.get_full_cnt();
                block.set_full_cnt(if set {prev_full_cnt + 1} else {prev_full_cnt - 1});
            }
            drop(block);
        };

        Ok(())
    }

}


/// File or extent free info representation.
pub struct FiData {
    fi:     Vec<u8>,
    size:   u16,
}

impl FiData {

    pub fn new() -> Self {
        let fi = Vec::new();
        let size = 0;
        FiData {
            fi,
            size,
        }
    }

    // number of blocks/extent
    pub fn size(&self) -> u16 {
        self.size
    }

    pub fn used_iter(&self) -> FiDataIter {
        FiDataIter {
            fi_data: &self,
            set: true,
            scanned: 0,
        }
    }

    pub fn free_iter(&self) -> FiDataIter {
        FiDataIter {
            fi_data: &self,
            set: false,
            scanned: 0,
        }
    }

    fn reset(&mut self) {
        self.fi.truncate(0);
        self.size = 0;
    }

    fn set_size(&mut self, size: u16) {
        self.size = size;
    }

    fn push_slice(&mut self, slice: &[u8]) {
        self.fi.extend_from_slice(slice);
    }
}


/// Iterator over free info of a file or extent.
pub struct FiDataIter<'a> {
    fi_data: &'a FiData,
    set: bool,
    scanned: u16,
}

impl FiDataIter<'_> {

    fn analyze_u64(&self, mut val: u64, starting_bit: u16) -> u16 {
        let mut pos = 0;
        for _ in 0..8 {
            let inc = self.anaylyze_u8(val as u8, starting_bit);
            pos += inc;
            if inc < 8 {
                break;
            }
            val >>= 8;
        }
        pos
    }

    fn anaylyze_u8(&self, val: u8, starting_bit: u16) -> u16 {
        let mut pos = starting_bit;
        while pos < BYTE_BITS.len() as u16 {
            if  (self.set == true  && (val & BYTE_BITS[pos as usize] != 0)) ||
                (self.set == false && (val & BYTE_BITS[pos as usize] == 0))
            {
                return pos;
            }
            pos += 1;
        }
        pos
    }

    // get part of free info bitmap as u64
    fn get_u64(&self, byte_pos: u16) -> u64 {
        u64::slice_to_int(&self.fi_data.fi[byte_pos as usize..byte_pos as usize+8]).unwrap()
    }

    // get part of free info bitmap as u8
    fn get_u8(&self, byte_pos: u16) -> u8 {
        self.fi_data.fi[byte_pos as usize]
    }
}

impl Iterator for FiDataIter<'_> {

    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        // go by u64
        while self.fi_data.size() - self.scanned >= 64 {
            let val = self.get_u64(self.scanned / 8);
            let inc = self.analyze_u64(val, 0);
            if inc < 64 {
                self.scanned += inc + 1;        // point to next bit,
                return Some(self.scanned - 1);  // and return found bit
            }
            self.scanned += 64;
        }

        // scan remaining bytes
        while self.fi_data.size - self.scanned >= 8 {
            let val = self.get_u8(self.scanned / 8);
            let inc = self.anaylyze_u8(val, 0);
            if inc < 8 {
                self.scanned += inc + 1;        // point to next bit,
                return Some(self.scanned - 1);  // and return found bit
            }
            self.scanned += 8;
        }

        // scan ramining individual bits
        let mut bit = 0;
        let val = self.get_u8(self.scanned / 8);
        while self.fi_data.size > self.scanned {
            if  (self.set == true  && (val & BYTE_BITS[bit] != 0)) ||
                (self.set == false && (val & BYTE_BITS[bit] == 0))
            {
                self.scanned += 1;              // point to next bit,
                return Some(self.scanned - 1);  // and return found bit
            }
            bit += 1;
            self.scanned += 1;
        }

        None
    }

}

