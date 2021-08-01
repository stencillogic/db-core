/// Data block structures.

use crate::common::errors::Error;
use crate::common::defs::BlockId;
use crate::common::crc32;
use crate::common::misc::SliceToIntConverter;
use crate::buf_mgr::buf_mgr::Pinned;
use crate::buf_mgr::buf_mgr::BlockArea;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::ops::Deref;
use std::ops::DerefMut;


pub const DBLOCK_HEADER_LEN:   usize = 20;       // datablock header length: crc32, checkpoint_csn, nent, etc.
pub const FHBLOCK_HEADER_LEN:  usize = 32;       // file header block header length
pub const EHBLOCK_HEADER_LEN:  usize = 31;       // extent header block header length
pub const FIBLOCK_HEADER_LEN:  usize = 18;       // free info block header length
pub const ENTRY_HEADER_LEN:    usize = 33;       // data entry header length
pub const VERENTRY_HEADER_LEN: usize = 16;       // versioning entry header length


/// Unified lock guard for read and write locking.
pub enum RwLockGuard<'a> {
    Read(RwLockReadGuard<'a, ()>),
    Write(RwLockWriteGuard<'a, ()>),
}


/// Data block locked and protected by lock guard.
pub struct BlockLocked<'a, T> {
    _lock_holder: RwLockGuard<'a>,
    block: T,
}

impl<'a, T> BlockLocked<'a, T> {

    pub fn new(lock_holder: RwLockGuard<'a>, block: T) -> Self {
        BlockLocked {
            _lock_holder: lock_holder,
            block,
        }
    }
}

impl<'a, T> Deref for BlockLocked<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}


/// Data block locked and protected by lock guard. Mutable version.
pub struct BlockLockedMut<'a, T>(BlockLocked<'a, T>);

impl<'a, T> BlockLockedMut<'a, T> {

    pub fn new(block: BlockLocked<'a, T>) -> Self {
        BlockLockedMut(block)
    }

    pub fn immut(&'a self) -> &BlockLocked<'a, T> {
        &self.0
    }
}

impl<'a, T> Deref for BlockLockedMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0.block
    }
}

impl<'a, T> DerefMut for BlockLockedMut<'a, T> {

    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.block
    }
}


/// Common functionality for all block types.
pub trait BasicBlock {

    fn get_crc(&self) -> u32;

    fn prepare_for_write(&mut self);

    fn get_id(&self) -> BlockId;

    fn set_id(&mut self, block_id: BlockId);

    fn get_buf_idx(&self) -> usize;

    fn slice(&self) -> &[u8];

    fn get_original_id(&self) -> BlockId;

    fn set_original_id(&mut self, block_id: BlockId);

    fn get_checkpoint_csn(&self) -> u64;

    fn set_checkpoint_csn(&mut self, csn: u64);
}


/// General data block representation.
/// Data block header:
///     crc             - u32: header block crc
///     checkpoint_csn  - u64: checkpoint csn
///     original_id     - 3 x u16: in checkpoint blocks points to the original block id.
///     nent            - u16: number of entries
/// Entry info section:
///     pos0            - u16: start position of the 1st entry.
///     pos1            - u16: start position of the 2nd entry.
///     pos2            - u16: start position of the 3rd entry.
///     ...
///     posn            - u16: start position of the last entry (upper boundary of data section).
/// Data section:
///     entries are filled from the bottom to the top of the block.
pub struct DataBlock<'a> {
    id:             BlockId,
    buf_idx:        usize,
    data:           Pinned<'a, BlockArea>,
    nent:           u16,
}

impl<'a> DataBlock<'a> {

    pub fn new(id: BlockId, buf_idx: usize, data: Pinned<'a, BlockArea>) -> Self {
        let nent = u16::slice_to_int(&data[18..20]).unwrap();
        DataBlock {
            id,
            buf_idx,
            data,
            nent,
        }
    }

    pub fn get_entry(&self, entry_id: u16) -> Result<DataBlockEntry, Error> {
        if self.has_entry(entry_id) {
            let (start_offset, end_offset) = self.get_data_offsets(entry_id);

            let slice = &self.data[start_offset..end_offset];
            let slice_mut = unsafe { std::slice::from_raw_parts_mut(slice.as_ptr() as *mut u8, slice.len()) };

            Ok(DataBlockEntry {
                _id: entry_id,
                data: slice_mut,
            })
        } else {
            Err(Error::object_does_not_exist())
        }
    }

    pub fn get_entry_mut(&mut self, entry_id: u16) -> Result<DataBlockEntryMut, Error> {
        if self.has_entry(entry_id) {
            let (start_offset, end_offset) = self.get_data_offsets(entry_id);
            Ok(DataBlockEntryMut {
                id: entry_id,
                data: &mut self.data[start_offset..end_offset],
            })
        } else {
            Err(Error::object_does_not_exist())
        }
    }

    /// how much space can we get if we want to add an entiry.
    pub fn get_free_space(&self) -> usize {
        let mut ret = self.get_upper_bound() - self.nent as usize * 2 - DBLOCK_HEADER_LEN;
        if ret >= 2 {       // avoid overflow to negative
            ret -= 2;
        }
        ret
    }

    /// how much space is used by data entries.
    pub fn get_used_space(&self) -> usize {
        self.data.len() - self.get_upper_bound()
    }

    pub fn add_entry(&mut self, size: usize) -> DataBlockEntryMut {
        assert!(size >= ENTRY_HEADER_LEN);
        let (start_pos, end_pos, entry_id) = self.determine_new_entry_pos(size);
        DataBlockEntryMut {
            id: entry_id,
            data: &mut self.data[start_pos as usize..end_pos],
        }
    }

    pub fn add_version_entry(&mut self, size: usize) -> DataBlockEntryVerMut {
        assert!(size >= ENTRY_HEADER_LEN + VERENTRY_HEADER_LEN);
        let (start_pos, end_pos, entry_id) = self.determine_new_entry_pos(size);
        DataBlockEntryVerMut {
            id: entry_id,
            data: &mut self.data[start_pos as usize..end_pos],
        }
    }

    pub fn has_entry(&self, entry_id: u16) -> bool {
        let ptr = DBLOCK_HEADER_LEN + entry_id as usize * 2;
        entry_id < self.nent && u16::slice_to_int(&self.data[ptr..ptr+2]).unwrap() != 0
    }

    // increase size of an entry. This function doesn't check argument for sanity.
    pub fn extend_entry(&mut self, entry_id: u16, extend_size: usize) -> Result<DataBlockEntryMut, Error> {
        if self.has_entry(entry_id) {
            self.shift_entries_above(entry_id, -(extend_size as i32));
            let (start_offset, end_offset) = self.get_data_offsets(entry_id);
            Ok(DataBlockEntryMut {
                id: entry_id,
                data: &mut self.data[start_offset..end_offset],
            })
        } else {
            Err(Error::object_does_not_exist())
        }
    }

    pub fn copy_from(&mut self, src: &DataBlock) {
        self.data.clone_from_slice(&src.data);
        self.nent = src.nent;
    }

    pub fn get_version_entry(&self, entry_id: u16) -> Result<DataBlockEntryVer, Error> {
        if self.has_entry(entry_id) {
            let (start_offset, end_offset) = self.get_data_offsets(entry_id);
            Ok(DataBlockEntryVer {
                id: entry_id,
                data: &self.data[start_offset..end_offset],
            })
        } else {
            Err(Error::object_does_not_exist())
        }
    }

    pub fn get_version_entry_mut(&mut self, entry_id: u16) -> Result<DataBlockEntryVerMut, Error> {
        if entry_id < self.nent {
            let (start_offset, end_offset) = self.get_data_offsets(entry_id);
            Ok(DataBlockEntryVerMut {
                id: entry_id,
                data: &mut self.data[start_offset..end_offset],
            })
        } else {
            Err(Error::object_does_not_exist())
        }
    }

    pub fn restore_entry(&mut self, entry: &DataBlockEntryMut) -> Result<(), Error> {
        let entry_id = entry.get_id();

        if !self.has_entry(entry_id) {
            return Err(Error::object_does_not_exist());
        }

        let (mut start_offset, end_offset) = self.get_data_offsets(entry_id);
        if end_offset - start_offset != entry.size() as usize {

            // entry sizes differ: restore previous size first
            let shift = (end_offset - start_offset) as i32 - entry.size() as i32;
            self.shift_entries_above(entry_id, shift);
            if shift > 0 {
                start_offset += shift as usize;
            } else {
                start_offset -= (-shift) as usize;
            }
        }

        // copy data
        self.data[start_offset..end_offset].copy_from_slice(entry.data);
        Ok(())
    }

    pub fn delete_entry(&mut self, entry_id: u16) {
        if !self.has_entry(entry_id) {
            return;
        }

        // shift all entries above, and set entry start position to 0, which means "deleted"
        let (start_offset, end_offset) = self.get_data_offsets(entry_id);
        self.shift_entries_above(entry_id, (end_offset - start_offset) as i32);
        let mut entry_ptr_pos = DBLOCK_HEADER_LEN + 2*entry_id as usize;
        self.data[entry_ptr_pos] = 0;
        self.data[entry_ptr_pos + 1] = 0;

        // remove all "deleted" from the tail of the entries position list
        if entry_id == self.nent - 1 {
            self.nent -= 1;
            entry_ptr_pos -= 2;
            while self.data[entry_ptr_pos] == 0 && self.data[entry_ptr_pos+1] == 0 {
                self.nent -= 1;
                entry_ptr_pos -= 2;
            }
        }
    }

    // return start position of the last entry
    fn get_upper_bound(&self) -> usize {
        if self.nent == 0 {
            self.data.len()
        } else {
            let i = DBLOCK_HEADER_LEN + 2 * (self.nent - 1) as usize;
            u16::slice_to_int(&self.data[i..i+2]).unwrap() as usize
        }
    }

    // move all entires laying above a certain entry
    // negative shift is moving up (extending entry), positive is vise versa
    fn shift_entries_above(&mut self, entry_id: u16, shift: i32) {
        // define region borders
        let i = DBLOCK_HEADER_LEN + 2*entry_id as usize;
        let region_end = {
            // walk through entries in reverse order and find position of the first
            // non-deleted entry, or return block size as region end
            let mut pos = i;
            loop {
                if pos == DBLOCK_HEADER_LEN {
                    break self.data.len();
                }
                let esp = u16::slice_to_int(&self.data[pos-2..pos]).unwrap() as usize;
                if esp != 0 {
                    break esp;
                }
                pos -= 2;
            }
        } - std::cmp::max(0, shift) as usize;

        let region_start = self.get_upper_bound();

        // update entry positions
        for idx in entry_id..self.nent {
            let i = DBLOCK_HEADER_LEN + 2*idx as usize;
            let start_pos = u16::slice_to_int(&self.data[i..i+2]).unwrap();
            if start_pos != 0 {
                let val = (start_pos as i32 + shift) as u16;
                self.data[i..i+2].clone_from_slice(&val.to_ne_bytes());
            }
        }

        let src = self.data[region_start .. region_end].as_ptr();
        let dst = (self.data[(region_start as i32 + shift) as usize .. (region_end as i32 + shift) as usize]).as_mut_ptr();
        unsafe { std::intrinsics::copy(src, dst, region_end - region_start) };
    }

    // allocate space for a new entry
    fn determine_new_entry_pos(&mut self, size: usize) -> (usize, usize, u16) {
        let mut end_pos = self.data.len();
        let mut ptr = DBLOCK_HEADER_LEN;
        while ptr < self.nent as usize * 2 + DBLOCK_HEADER_LEN {
            if self.data[ptr] == 0 && self.data[ptr+1] == 0 {
                let mut next_existing = ptr + 2;
                while next_existing < self.nent as usize + DBLOCK_HEADER_LEN 
                    && self.data[next_existing] == 0 && self.data[next_existing+1] == 0 
                {
                    next_existing += 2;
                }
                let entry_id = (next_existing - DBLOCK_HEADER_LEN) as u16;
                self.shift_entries_above(entry_id, -(size as i32));
                break;
            }
            end_pos = u16::slice_to_int(&self.data[ptr..ptr+2]).unwrap() as usize;
            ptr += 2;
        }
        let start_pos = end_pos - size;
        self.data[ptr..ptr+2].clone_from_slice(&(start_pos as u16).to_ne_bytes());
        let entry_id = ((ptr - DBLOCK_HEADER_LEN) / 2) as u16;
        if entry_id == self.nent {
            self.nent += 1;
        }
        (start_pos, end_pos, entry_id)
    }

    // return entry data slice position inside the block
    fn get_data_offsets(&self, entry_id: u16) -> (usize, usize) {
        let mut i = DBLOCK_HEADER_LEN + 2*entry_id as usize;
        let start_offset = u16::slice_to_int(&self.data[i..i+2]).unwrap() as usize;
        let mut end_offset = self.data.len();
        while i > DBLOCK_HEADER_LEN {
            let offset = u16::slice_to_int(&self.data[i-2..i]).unwrap() as usize;
            if offset != 0 {
                end_offset = offset;
                break;
            }
            i -= 2;
        }

        (start_offset, end_offset)
    }

}


impl BasicBlock for DataBlock<'_> {

    fn get_crc(&self) -> u32 {
        u32::slice_to_int(&self.data[0..4]).unwrap()
    }

    fn prepare_for_write(&mut self) {
        // update nent
        self.data[18..20].copy_from_slice(&self.nent.to_ne_bytes());

        // update crc
        CommonOps::update_block_crc(&mut self.data);
    }

    fn get_id(&self) -> BlockId {
        self.id
    }

    fn set_id(&mut self, block_id: BlockId) {
        self.id = block_id;
    }

    fn get_buf_idx(&self) -> usize {
        self.buf_idx
    }

    fn slice(&self) -> &[u8] {
        &self.data
    }

    fn get_original_id(&self) -> BlockId {
        BlockId {
            file_id:    u16::slice_to_int(&self.data[12..14]).unwrap(),
            extent_id:  u16::slice_to_int(&self.data[14..16]).unwrap(),
            block_id:   u16::slice_to_int(&self.data[16..18]).unwrap(),
        }
    }

    fn set_original_id(&mut self, block_id: BlockId) {
        self.data[12..14].copy_from_slice(&block_id.file_id.to_ne_bytes());
        self.data[14..16].copy_from_slice(&block_id.extent_id.to_ne_bytes());
        self.data[16..18].copy_from_slice(&block_id.block_id.to_ne_bytes());
    }

    fn get_checkpoint_csn(&self) -> u64 {
        u64::slice_to_int(&self.data[4..12]).unwrap()
    }

    fn set_checkpoint_csn(&mut self, csn: u64) {
        self.data[4..12].copy_from_slice(&csn.to_ne_bytes());
    }
}

impl Drop for DataBlock<'_> {
    fn drop(&mut self) {
        self.data[18..20].copy_from_slice(&self.nent.to_ne_bytes());
    }
}


/// Mutable data block entry.
pub struct DataBlockEntry<'a> {
    _id:     u16,
    data:   &'a [u8],
}


impl<'a> DataBlockEntry<'a> {

    pub fn is_start(&self) -> bool {
        (self.data[0] & 0x01) != 0
    }

    pub fn is_end(&self) -> bool {
        (self.data[0] & 0x02) != 0
    }

    pub fn is_deleted(&self) -> bool {
        (self.data[0] & 0x04) != 0
    }

    // size of entry data
    pub fn data_size(&self) -> u16 {
        (self.data.len() - ENTRY_HEADER_LEN) as u16
    }

    pub fn get_tsn(&self) -> u64 {
        u64::slice_to_int(&self.data[1..9]).unwrap()
    }

    pub fn get_csn(&self) -> u64 {
        u64::slice_to_int(&self.data[9..17]).unwrap()
    }

    pub fn get_prev_version_ptr(&self) -> (BlockId, u16) {
        CommonOps::parse_obj_id(&self.data, 17)
    }

    pub fn get_continuation(&self) -> (BlockId, u16) {
        CommonOps::parse_obj_id(&self.data, 25)
    }

    // return slice chunk with data in given bounds
    pub fn slice(&self, start_pos: u16, end_pos: u16) -> &[u8] {
        &self.data[ENTRY_HEADER_LEN + start_pos as usize .. ENTRY_HEADER_LEN + end_pos as usize]
    }

}

/// Data block entry.
pub struct DataBlockEntryMut<'a> {
    id:     u16,
    data:   &'a mut [u8],
}

impl<'a> DataBlockEntryMut<'a> {

    pub fn set_start(&mut self, set: bool) {
        if set {self.data[0] |= 0x01;} else {self.data[0] &= !0x01;}
    }

    pub fn set_end(&mut self, set: bool) {
        if set {self.data[0] |= 0x02;} else {self.data[0] &= !0x02;}
    }

    pub fn set_deleted(&mut self, set: bool) {
        if set {self.data[0] |= 0x04;} else {self.data[0] &= !0x04;}
    }

    pub fn set_tsn(&mut self, tsn: u64) {
        self.data[1..9].clone_from_slice(&tsn.to_ne_bytes());
    }

    pub fn set_csn(&mut self, csn: u64) {
        self.data[9..17].clone_from_slice(&csn.to_ne_bytes());
    }

    pub fn set_prev_version_ptr(&mut self, block_id: &BlockId, entry_id: u16) {
        CommonOps::write_obj_id(self.data, block_id, entry_id, 17);
    }

    pub fn set_continuation(&mut self, block_id: &BlockId, entry_id: u16) {
        CommonOps::write_obj_id(self.data, block_id, entry_id, 25);
        // unset end
        self.data[0] &= !0x04;
    }

    // return slice chunk for data writing in given bounds
    pub fn mut_slice(&mut self, start_pos: u16, end_pos: u16) -> &mut [u8] {
        &mut self.data[ENTRY_HEADER_LEN + start_pos as usize .. ENTRY_HEADER_LEN + end_pos as usize]
    }

    pub fn get_id(&self) -> u16 {
        self.id
    }

    pub fn is_end(&self) -> bool {
        (self.data[0] & 0x02) != 0
    }

    // full size of the entry
    pub fn size(&self) -> u16 {
        self.data.len() as u16
    }

    // size of entry data
    pub fn data_size(&self) -> u16 {
        (self.data.len() - ENTRY_HEADER_LEN) as u16
    }

    pub fn get_tsn(&self) -> u64 {
        u64::slice_to_int(&self.data[1..9]).unwrap()
    }

    pub fn get_continuation(&self) -> (BlockId, u16) {
        CommonOps::parse_obj_id(&self.data, 25)
    }

    pub fn immut(&self) -> DataBlockEntry {
        DataBlockEntry {
            _id:     self.id,
            data:   self.data,
        }
    }
}


/// Data block entry in version store.
///   main_store_ptr:           4 x u16: block id and entry id of the versioned entry in the main store.
///   prev_created_entry_ptr:   4 x u16: block id and entry id of previously created version of the
///                             entry.
pub struct DataBlockEntryVerMut<'a> {
    id:     u16,
    data:   &'a mut [u8],
}

impl<'a> DataBlockEntryVerMut<'a> {

    pub fn get_id(&self) -> u16 {
        self.id
    }

    pub fn inner_entry(&'a mut self) -> DataBlockEntryMut<'a> {
        let id = u16::slice_to_int(&self.data[6..8]).unwrap();
        let data = &mut self.data[VERENTRY_HEADER_LEN..];
        DataBlockEntryMut {
            id,
            data,
        }
    }

    pub fn set_main_storage_ptr(&mut self, block_id: &BlockId, entry_id: u16) {
        CommonOps::write_obj_id(self.data, block_id, entry_id, 0);
    }

    pub fn set_prev_created_entry_ptr(&mut self, last_ver_block_id: &BlockId, last_ver_entry_id: u16) {
        CommonOps::write_obj_id(self.data, last_ver_block_id, last_ver_entry_id, 8);
    }

    pub fn copy_from_mut(&mut self, entry: &DataBlockEntryMut) {
        self.data[VERENTRY_HEADER_LEN..].copy_from_slice(entry.data);
    }
}


/// Mutable data block entry in version store.
pub struct DataBlockEntryVer<'a> {
    id:     u16,
    data:   &'a [u8],
}


impl<'a> DataBlockEntryVer<'a> {

    pub fn get_id(&self) -> u16 {
        self.id
    }

    pub fn get_main_storage_ptr(&self) -> (BlockId, u16) {
        CommonOps::parse_obj_id(&self.data, 0)
    }

    pub fn get_prev_created_entry_ptr(&self) -> (BlockId, u16) {
        CommonOps::parse_obj_id(&self.data, 8)
    }

    pub fn inner_entry(&'a self) -> DataBlockEntry<'a> {
        let id = u16::slice_to_int(&self.data[6..8]).unwrap();
        let data = &self.data[VERENTRY_HEADER_LEN..];
        DataBlockEntry {
            _id: id,
            data,
        }
    }

    pub fn to_inner_entry(self) -> DataBlockEntry<'a> {
        let id = u16::slice_to_int(&self.data[6..8]).unwrap();
        let data = &self.data[VERENTRY_HEADER_LEN..];
        DataBlockEntry {
            _id: id,
            data,
        }
    }
}


/// Functions related to blocks containing free info section.
pub trait FreeInfoSection {

    // slice of block data after header for holding free info bitmask
    fn fi_slice(&self) -> &[u8];

    // mutable slice of block data after header for holding free info bitmask
    fn fi_slice_mut(&mut self) -> &mut [u8];
}

/// Functions related to blocks containing heading free info section.
pub trait FreeInfoHeaderSection: FreeInfoSection {

    // full free info bitmap length in bits
    fn fi_size(&self) -> u16;

    // set full free info bitmap length in bits
    fn set_fi_size(&mut self, fi_size: u16);

    // return number of full blocks
    fn get_full_cnt(&self) -> u16;

    // set the number of full blocks
    fn set_full_cnt(&mut self, cnt: u16);
}



/// File header block.
///     crc             - u32: header block crc
///     checkpoint_csn  - u64: checkpoint csn
///     original_id     - 3 x u16: in checkpoint blocks points to the original block id.
///     magic           - u8: either magic for regular data file, or for checkpoint, or versioning data.
///     use_mark        - u8: 1 - file is initialized and is in use, 0 - otherwise.
///     block_size      - u16: block size
///     extent_size     - u16: block size
///     max_extent_num  - u16: maximum allowed number of extents in this file
///     file_id         - u16: file id
///     fi_full_cnt     - u16: number of full blocks in the extent (number of set bits in free_info)
///     fi_length       - u16: number of bits in free_blocks bitmask
///     free_info       - bitmask containing about extents with free blocks: 0 - extent has free
///                       blocks, 1 - extent is full.
pub struct FileHeaderBlock<'a> {
    id:             BlockId,
    buf_idx:        usize,
    data:           Pinned<'a, BlockArea>,
}

impl<'a> FileHeaderBlock<'a> {

    pub fn new(id: BlockId, buf_idx: usize, data: Pinned<'a, BlockArea>) -> Self {
        FileHeaderBlock {
            id,
            buf_idx,
            data,
        }
    }

    // set if file is initialized and can be used
    pub fn set_in_use(&mut self, in_use: bool) {
        self.data[19] = if in_use {1} else {0};
    }

    // set block size in file
    pub fn set_block_size(&mut self, size: u16) {
        self.data[20..22].copy_from_slice(&size.to_ne_bytes());
    }
}


impl BasicBlock for FileHeaderBlock<'_> {

    fn get_crc(&self) -> u32 {
        u32::slice_to_int(&self.data[0..4]).unwrap()
    }

    fn prepare_for_write(&mut self) {
        CommonOps::update_block_crc(&mut self.data);
    }

    fn get_id(&self) -> BlockId {
        self.id
    }

    fn set_id(&mut self, block_id: BlockId) {
        self.id = block_id;
    }

    fn get_buf_idx(&self) -> usize {
        self.buf_idx
    }

    fn slice(&self) -> &[u8] {
        &self.data
    }

    fn get_original_id(&self) -> BlockId {
        BlockId {
            file_id:    u16::slice_to_int(&self.data[12..14]).unwrap(),
            extent_id:  u16::slice_to_int(&self.data[14..16]).unwrap(),
            block_id:   u16::slice_to_int(&self.data[16..18]).unwrap(),
        }
    }

    fn set_original_id(&mut self, block_id: BlockId) {
        self.data[12..14].copy_from_slice(&block_id.file_id.to_ne_bytes());
        self.data[14..16].copy_from_slice(&block_id.extent_id.to_ne_bytes());
        self.data[16..18].copy_from_slice(&block_id.block_id.to_ne_bytes());
    }

    fn get_checkpoint_csn(&self) -> u64 {
        u64::slice_to_int(&self.data[4..12]).unwrap()
    }

    fn set_checkpoint_csn(&mut self, csn: u64) {
        self.data[4..12].copy_from_slice(&csn.to_ne_bytes());
    }
}


impl FreeInfoSection for FileHeaderBlock<'_> {

    // slice of block data after header for holding free info bitmask
    fn fi_slice(&self) -> &[u8] {
        &self.data[FHBLOCK_HEADER_LEN..]
    }

    // mutable slice of block data after header for holding free info bitmask
    fn fi_slice_mut(&mut self) -> &mut [u8] {
        &mut self.data[FHBLOCK_HEADER_LEN..]
    }
}

impl FreeInfoHeaderSection for FileHeaderBlock<'_> {

    // full free info bitmap length in bits
    fn fi_size(&self) -> u16 {
        u16::slice_to_int(&self.data[30..32]).unwrap()
    }

    // set full free info bitmap length in bits
    fn set_fi_size(&mut self, fi_size: u16) {
        self.data[30..32].copy_from_slice(&fi_size.to_ne_bytes());
    }

    // return number of full blocks
    fn get_full_cnt(&self) -> u16 {
        u16::slice_to_int(&self.data[28..30]).unwrap()
    }

    // set the number of full blocks
    fn set_full_cnt(&mut self, cnt: u16) {
        self.data[28..30].copy_from_slice(&cnt.to_ne_bytes());
    }
}


/// Extent header block.
///     crc                         - u32: header block crc
///     checkpoint_csn              - u64: checkpoint csn
///     original_id                 - 3 x u16: in checkpoint blocks points to the original block id.
///     use_mark                    - u8: 1 - file is initialized and is in use, 0 - otherwise.
///     last_update_ts              - u64: unix epoch seconds of extent sealing. Only used in versioning
///                                     store extents.
///     fi_full_cnt (next_file_id)  - u16: number of full blocks in the extent (pointer to next versioning
///                                     extent)
///     fi_length (next_extent_id)  - u16: number of bits in free_blocks bitmask (pointer to next 
///                                     versioning extent)
///     free_info                   - bitmask containing about extents with free blocks: 0 - extent has free
///                                     blocks, 1 - extent is full.
pub struct ExtentHeaderBlock<'a> {
    id:             BlockId,
    buf_idx:        usize,
    data:           Pinned<'a, BlockArea>,
}

impl<'a> ExtentHeaderBlock<'a> {

    pub fn new(id: BlockId, buf_idx: usize, data: Pinned<'a, BlockArea>) -> Self {
        ExtentHeaderBlock {
            id,
            buf_idx,
            data,
        }
    }

    // return sealing timestamp 
    pub fn get_seal_date(&self) -> u64 {
        u64::slice_to_int(&self.data[19..27]).unwrap()
    }

    // set sealing timestamp 
    pub fn set_seal_date(&mut self, ts: u64) {
        self.data[19..27].copy_from_slice(&ts.to_ne_bytes());
    }

    // return pointer to next versioning extent
    pub fn get_next_versioning_extent(&self) -> (u16, u16) {
        (u16::slice_to_int(&self.data[27..29]).unwrap(),
         u16::slice_to_int(&self.data[29..31]).unwrap())
    }

    // set pointer to next versioning extent
    pub fn set_next_versioning_extent(&mut self, file_id: u16, extent_id: u16) {
        self.data[27..29].copy_from_slice(&file_id.to_ne_bytes());
        self.data[29..31].copy_from_slice(&extent_id.to_ne_bytes());
    }
}


impl BasicBlock for ExtentHeaderBlock<'_> {

    fn get_crc(&self) -> u32 {
        u32::slice_to_int(&self.data[0..4]).unwrap()
    }

    fn prepare_for_write(&mut self) {
        CommonOps::update_block_crc(&mut self.data);
    }

    fn get_id(&self) -> BlockId {
        self.id
    }

    fn set_id(&mut self, block_id: BlockId) {
        self.id = block_id;
    }

    fn get_buf_idx(&self) -> usize {
        self.buf_idx
    }

    fn slice(&self) -> &[u8] {
        &self.data
    }

    fn get_original_id(&self) -> BlockId {
        BlockId {
            file_id:    u16::slice_to_int(&self.data[12..14]).unwrap(),
            extent_id:  u16::slice_to_int(&self.data[14..16]).unwrap(),
            block_id:   u16::slice_to_int(&self.data[16..18]).unwrap(),
        }
    }

    fn set_original_id(&mut self, block_id: BlockId) {
        self.data[12..14].copy_from_slice(&block_id.file_id.to_ne_bytes());
        self.data[14..16].copy_from_slice(&block_id.extent_id.to_ne_bytes());
        self.data[16..18].copy_from_slice(&block_id.block_id.to_ne_bytes());
    }

    fn get_checkpoint_csn(&self) -> u64 {
        u64::slice_to_int(&self.data[4..12]).unwrap()
    }

    fn set_checkpoint_csn(&mut self, csn: u64) {
        self.data[4..12].copy_from_slice(&csn.to_ne_bytes());
    }
}

impl FreeInfoSection for ExtentHeaderBlock<'_> {

    // slice of block data after header for holding free info bitmask
    fn fi_slice(&self) -> &[u8] {
        &self.data[EHBLOCK_HEADER_LEN..]
    }

    // mutable slice of block data after header for holding free info bitmask
    fn fi_slice_mut(&mut self) -> &mut [u8] {
        &mut self.data[EHBLOCK_HEADER_LEN..]
    }
}

impl FreeInfoHeaderSection for ExtentHeaderBlock<'_> {

    fn fi_size(&self) -> u16 {
        u16::slice_to_int(&self.data[29..31]).unwrap()
    }

    // set full free info bitmap length in bits
    fn set_fi_size(&mut self, fi_size: u16) {
        self.data[29..31].copy_from_slice(&fi_size.to_ne_bytes());
    }

    // return number of full blocks
    fn get_full_cnt(&self) -> u16 {
        u16::slice_to_int(&self.data[27..29]).unwrap()
    }

    // set the number of full blocks
    fn set_full_cnt(&mut self, cnt: u16) {
        self.data[27..29].copy_from_slice(&cnt.to_ne_bytes());
    }
}


/// Block for keeping free info bitmap.
///     crc             - u32: header block crc.
///     checkpoint_csn  - u64: checkpoint csn
///     original_id     - 3 x u16: in checkpoint blocks points to the original block id.
///     free_info       - bitmask containing about extents with free blocks: 0 - extent has free
///                       blocks, 1 - extent is full.
pub struct FreeInfoBlock<'a> {
    id:             BlockId,
    buf_idx:        usize,
    data:           Pinned<'a, BlockArea>,
}

impl<'a> FreeInfoBlock<'a> {

    pub fn new(id: BlockId, buf_idx: usize, data: Pinned<'a, BlockArea>) -> Self {
        FreeInfoBlock {
            id,
            buf_idx,
            data,
        }
    }

}

impl BasicBlock for FreeInfoBlock<'_> {

    fn get_crc(&self) -> u32 {
        u32::slice_to_int(&self.data[0..4]).unwrap()
    }

    fn prepare_for_write(&mut self) {
        CommonOps::update_block_crc(&mut self.data);
    }

    fn get_id(&self) -> BlockId {
        self.id
    }

    fn set_id(&mut self, block_id: BlockId) {
        self.id = block_id;
    }

    fn get_buf_idx(&self) -> usize {
        self.buf_idx
    }

    fn slice(&self) -> &[u8] {
        &self.data
    }

    fn get_original_id(&self) -> BlockId {
        BlockId {
            file_id:    u16::slice_to_int(&self.data[12..14]).unwrap(),
            extent_id:  u16::slice_to_int(&self.data[14..16]).unwrap(),
            block_id:   u16::slice_to_int(&self.data[16..18]).unwrap(),
        }
    }

    fn set_original_id(&mut self, block_id: BlockId) {
        self.data[12..14].copy_from_slice(&block_id.file_id.to_ne_bytes());
        self.data[14..16].copy_from_slice(&block_id.extent_id.to_ne_bytes());
        self.data[16..18].copy_from_slice(&block_id.block_id.to_ne_bytes());
    }

    fn get_checkpoint_csn(&self) -> u64 {
        u64::slice_to_int(&self.data[4..12]).unwrap()
    }

    fn set_checkpoint_csn(&mut self, csn: u64) {
        self.data[4..12].copy_from_slice(&csn.to_ne_bytes());
    }
}


impl FreeInfoSection for FreeInfoBlock<'_> {

    // slice of block data after header for holding free info bitmask
    fn fi_slice(&self) -> &[u8] {
        &self.data[FIBLOCK_HEADER_LEN..]
    }

    // mutable slice of block data after header for holding free info bitmask
    fn fi_slice_mut(&mut self) -> &mut [u8] {
        &mut self.data[FIBLOCK_HEADER_LEN..]
    }
}




/// Reusable functions.
struct CommonOps {}

impl CommonOps {

    fn parse_obj_id(data: &[u8], from_pos: usize) -> (BlockId, u16) {
        let mut i = from_pos;
        let file_id = u16::slice_to_int(&data[i..i+2]).unwrap(); i += 2;
        let extent_id = u16::slice_to_int(&data[i..i+2]).unwrap(); i += 2;
        let block_id = u16::slice_to_int(&data[i..i+2]).unwrap(); i += 2;
        let entry_id = u16::slice_to_int(&data[i..i+2]).unwrap();
        (BlockId {
            file_id,
            extent_id,
            block_id,
        }, entry_id)
    }

    fn write_obj_id(data: &mut [u8], block_id: &BlockId, entry_id: u16, pos: usize) {
        let mut i = pos;
        data[i..i+2].clone_from_slice(&block_id.file_id.to_ne_bytes()); i += 2;
        data[i..i+2].clone_from_slice(&block_id.extent_id.to_ne_bytes()); i += 2;
        data[i..i+2].clone_from_slice(&block_id.block_id.to_ne_bytes()); i += 2;
        data[i..i+2].clone_from_slice(&entry_id.to_ne_bytes());
    }

    pub fn update_block_crc(data: &mut [u8]) {
        let mut crc32 = crc32::crc32_begin();
        crc32 = crc32::crc32_arr(crc32, &data[4..]);
        crc32 = crc32::crc32_finalize(crc32);
        data[0..4].copy_from_slice(&crc32.to_ne_bytes());
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::atomic::AtomicU64;
    use crate::common::misc::alloc_buf;
    use crate::common::misc::dealloc_buf;

    #[test]
    fn test_data_block() {

        let block_size = 4096;
        let mut block_buf = BlockArea::new(alloc_buf(block_size).expect("Failed to allocate block"), block_size);
        for i in 0..block_size { block_buf[i] = 0; }
        let stub_pin = AtomicU64::new(1000);

        let mut block_id = BlockId {
            file_id: 1,
            extent_id: 2,
            block_id: 3,
        };

        let buf_idx = 213;

        let mut block = DataBlock::new(block_id, buf_idx, Pinned::<BlockArea>::new(block_buf.clone(), &stub_pin));

        let checkpoint_csn = 35434;
        let mut orig_id = BlockId::new();
        assert!(block.get_entry(0).is_err());
        assert!(block.get_entry_mut(0).is_err());
        assert!(block.get_version_entry(1).is_err());
        assert!(block.get_version_entry_mut(1).is_err());
        assert!(!block.has_entry(0));
        assert!(!block.has_entry(1));
        assert_eq!(block_size - DBLOCK_HEADER_LEN - 2, block.get_free_space());
        assert_eq!(0, block.get_used_space());
        assert_eq!(0, block.get_checkpoint_csn());
        assert_eq!(orig_id, block.get_original_id());
        assert_eq!(0, block.get_crc());
        assert_eq!(block_id, block.get_id());

        let entry_sz = 250;
        let mut entry = block.add_entry(entry_sz);
        let entry_id = entry.get_id();

        let b = 167;
        entry.mut_slice(0, 10)[0] = b;
        let mut entry4_buf = [0u8; 250];
        &mut entry4_buf.copy_from_slice(entry.data);
        entry.mut_slice(0, 10)[0] = b + 63;
        let entry4 = DataBlockEntryMut {
            id:   entry_id,
            data: &mut entry4_buf, 
        };

        let entry_sz2 = 301;
        let mut entry2 = block.add_version_entry(entry_sz2);
        let entry_id2 = entry2.get_id();

        block.set_checkpoint_csn(checkpoint_csn);
        orig_id.file_id = 560;
        orig_id.extent_id = 2000;
        orig_id.block_id = 3000;
        block.set_original_id(orig_id);
        block.prepare_for_write();

        block_id.block_id += 4;
        block_id.file_id += 2;
        block_id.extent_id += 3;
        block.set_id(block_id);

        assert!(block.get_entry(entry_id).is_ok());
        assert!(block.get_entry_mut(entry_id).is_ok());
        assert!(block.get_version_entry(entry_id2).is_ok());
        assert!(block.get_version_entry_mut(entry_id2).is_ok());
        assert_eq!(block_size - DBLOCK_HEADER_LEN - 6 - entry_sz - entry_sz2, block.get_free_space());
        assert_eq!(entry_sz + entry_sz2, block.get_used_space());
        assert!(block.has_entry(entry_id));
        assert!(block.has_entry(entry_id2));
        assert_eq!(checkpoint_csn, block.get_checkpoint_csn());
        assert_eq!(orig_id, block.get_original_id());
        assert!(0 != block.get_crc());
        assert_eq!(block_id, block.get_id());
        assert_eq!(buf_idx, block.get_buf_idx());
        assert_eq!(block_size, block.slice().len());

        let extend_size = 126;
        let entry3 = block.extend_entry(entry_id, extend_size).expect("Failed to extend entry");
        let entry_sz3 = entry3.size() as usize;
        let entry_id3 = entry3.get_id();
        assert_eq!(entry_id3, entry_id);
        assert_eq!(block_size - DBLOCK_HEADER_LEN - 6 - entry_sz - entry_sz2 - extend_size, block.get_free_space());
        assert_eq!(entry_sz + entry_sz2 + extend_size, block.get_used_space());
        assert_eq!(entry_sz + extend_size, entry_sz3);

        let mut block_buf2 = BlockArea::new(alloc_buf(block_size).expect("Failed to allocate block"), block_size);
        for i in 0..block_size { block_buf2[i] = 0; }
        let stub_pin2 = AtomicU64::new(1000);

        let mut block_id2 = BlockId {
            file_id: 2,
            extent_id: 3,
            block_id: 4,
        };

        let buf_idx2 = 811;

        let mut block2 = DataBlock::new(block_id2, buf_idx2, Pinned::<BlockArea>::new(block_buf2.clone(), &stub_pin2));

        block2.copy_from(&block);
        assert_eq!(block.slice(), block2.slice());
        assert_eq!(block_size - DBLOCK_HEADER_LEN - 6 - entry_sz - entry_sz2 - extend_size, block2.get_free_space());
        assert_eq!(entry_sz + entry_sz2 + extend_size, block2.get_used_space());

        let mut entry = block.get_entry_mut(entry_id).expect("No entry with specified id");

        let mut prev_block_id = BlockId::new();
        let mut prev_entry_id = 0;
        let mut cont_block_id = BlockId::new();
        let mut cont_entry_id = 0;
        let mut tsn = 0;
        let mut csn = 0;
        assert_eq!(entry.immut().get_prev_version_ptr(), (prev_block_id, prev_entry_id));
        assert_eq!(entry.get_continuation(), (cont_block_id, cont_entry_id));
        assert!(!entry.immut().is_start());
        assert!(!entry.is_end());
        assert!(!entry.immut().is_deleted());
        assert_eq!(entry.get_tsn(), tsn);
        assert_eq!(entry.immut().get_csn(), csn);

        prev_block_id.file_id = 345;
        prev_block_id.extent_id = 45;
        prev_block_id.block_id = 765;
        prev_entry_id = 23;
        cont_block_id.file_id = 1345;
        cont_block_id.extent_id = 145;
        cont_block_id.block_id = 1765;
        cont_entry_id = 123;
        tsn = 7823423982067;
        csn = 67841123423545;
        entry.set_start(true);
        entry.set_end(true);
        entry.set_deleted(true);
        entry.set_tsn(tsn);
        entry.set_csn(csn);
        entry.set_prev_version_ptr(&prev_block_id, prev_entry_id);

        assert!(entry.immut().is_deleted());

        entry.set_continuation(&cont_block_id, cont_entry_id);

        assert_eq!(entry.immut().get_prev_version_ptr(), (prev_block_id, prev_entry_id));
        assert_eq!(entry.get_continuation(), (cont_block_id, cont_entry_id));
        assert!(entry.immut().is_start());
        assert!(entry.is_end());
        assert!(!entry.immut().is_deleted());
        assert_eq!(entry.get_tsn(), tsn);
        assert_eq!(entry.immut().get_csn(), csn);

        assert_eq!(entry.size() as usize, entry_sz + extend_size);
        assert_eq!(entry.data_size() as usize, entry_sz + extend_size - ENTRY_HEADER_LEN);
        assert_eq!(entry.immut().slice(0, 10).len(), 10);
        assert_eq!(entry.mut_slice(0, 10).len(), 10);


        let mut ms_block_id = BlockId::new();
        let mut ms_entry_id = 0;
        let mut prev_block_id = BlockId::new();
        let mut prev_entry_id = 0;

        let mut entry2_immut = block.get_version_entry(entry_id2).expect("No entry with specified id");
        assert_eq!(entry2_immut.get_main_storage_ptr(), (ms_block_id, ms_entry_id));
        assert_eq!(entry2_immut.get_prev_created_entry_ptr(), (prev_block_id, prev_entry_id));

        let mut entry2 = block.get_version_entry_mut(entry_id2).expect("No entry with specified id");

        prev_block_id.file_id = 345;
        prev_block_id.extent_id = 45;
        prev_block_id.block_id = 765;
        prev_entry_id = 23;
        ms_block_id.file_id = 1345;
        ms_block_id.extent_id = 145;
        ms_block_id.block_id = 1765;
        ms_entry_id = 123;
        entry2.set_main_storage_ptr(&ms_block_id, ms_entry_id);
        entry2.set_prev_created_entry_ptr(&prev_block_id, prev_entry_id);

        let inner_len = entry2.data.len() - VERENTRY_HEADER_LEN;
        assert_eq!(entry2.inner_entry().data.len(), inner_len);

        block.restore_entry(&entry4);
        let entry = block.get_entry_mut(entry_id).expect("No entry with specified id");
        assert_eq!(b, entry.immut().slice(0, 10)[0]);
        assert_eq!(block_size - DBLOCK_HEADER_LEN - 6 - entry_sz - entry_sz2, block.get_free_space());
        assert_eq!(entry_sz + entry_sz2, block.get_used_space());

        let mut entry2_immut = block.get_version_entry(entry_id2).expect("No entry with specified id");
        assert_eq!(entry2_immut.get_main_storage_ptr(), (ms_block_id, ms_entry_id));
        assert_eq!(entry2_immut.get_prev_created_entry_ptr(), (prev_block_id, prev_entry_id));


        let c = 244;
        let mut entry5_buf = [0u8; 301 - VERENTRY_HEADER_LEN];
        let mut entry5 = DataBlockEntryMut {
            id:   3,
            data: &mut entry5_buf,
        };
        entry5.mut_slice(0, 10)[0] = c;

        let mut entry2 = block.get_version_entry_mut(entry_id2).expect("No entry with specified id");
        entry2.copy_from_mut(&entry5);
        assert_eq!(entry2.inner_entry().immut().slice(0, 10)[0], c);

        block.delete_entry(entry_id);
        assert_eq!(block_size - DBLOCK_HEADER_LEN - 6 - entry_sz2, block.get_free_space());
        block.delete_entry(entry_id2);
        assert_eq!(block_size - DBLOCK_HEADER_LEN - 2, block.get_free_space());

        dealloc_buf(block_buf.data_ptr(), block_size);
        dealloc_buf(block_buf2.data_ptr(), block_size);
    }


    #[test]
    fn test_header_blocks() {

        let block_size = 4096;

        let mut block_buf = BlockArea::new(alloc_buf(block_size).expect("Failed to allocate block"), block_size);
        let stub_pin = AtomicU64::new(1000);


        // file header


        let mut block_id = BlockId {
            file_id: 3,
            extent_id: 0,
            block_id: 0,
        };

        let mut orig_id = BlockId::init(0,0,0);
        let mut csn = 0;
        let buf_idx = 811;

        for i in 0..block_size { block_buf[i] = 0; }
        let mut block = FileHeaderBlock::new(block_id, buf_idx, Pinned::<BlockArea>::new(block_buf.clone(), &stub_pin));

        // block-specific functions
        block.set_in_use(true);
        block.set_block_size(block_size as u16);

        // basic block functions
        assert_eq!(buf_idx, block.get_buf_idx());
        assert_eq!(block_size, block.slice().len());
        assert_eq!(0, block.get_checkpoint_csn());
        assert_eq!(orig_id, block.get_original_id());
        assert_eq!(0, block.get_crc());
        assert_eq!(block_id, block.get_id());

        block_id.file_id += 1;
        block_id.extent_id += 1;
        block_id.block_id += 1;
        block.set_id(block_id);
        orig_id.file_id += 2;
        orig_id.extent_id += 2;
        orig_id.block_id += 2;
        block.set_original_id(orig_id);
        csn = 45532;
        block.set_checkpoint_csn(csn);
        block.prepare_for_write();

        assert_eq!(csn, block.get_checkpoint_csn());
        assert_eq!(orig_id, block.get_original_id());
        assert!(0 != block.get_crc());
        assert_eq!(block_id, block.get_id());

        // free info functions
        assert_eq!(block.fi_slice().len(), block_size - FHBLOCK_HEADER_LEN);
        assert_eq!(block.fi_slice_mut().len(), block_size - FHBLOCK_HEADER_LEN);

        let mut fi_size = 0;
        assert_eq!(block.fi_size(), fi_size);
        fi_size = 123;
        block.set_fi_size(fi_size);
        assert_eq!(block.fi_size(), fi_size);

        let mut full_cnt = 0;
        assert_eq!(block.get_full_cnt(), full_cnt);
        full_cnt = 11;
        block.set_full_cnt(full_cnt);
        assert_eq!(block.get_full_cnt(), full_cnt);


        // extent header


        let mut block_id = BlockId {
            file_id: 3,
            extent_id: 1,
            block_id: 0,
        };

        let mut orig_id = BlockId::init(0,0,0);
        let mut csn = 0;
        let buf_idx = 812;

        for i in 0..block_size { block_buf[i] = 0; }
        let mut block = ExtentHeaderBlock::new(block_id, buf_idx, Pinned::<BlockArea>::new(block_buf.clone(), &stub_pin));

        // block-specific functions
        let mut ts = 0;
        assert_eq!(block.get_seal_date(), ts);
        ts = 4234;
        block.set_seal_date(ts);
        assert_eq!(block.get_seal_date(), ts);

        // basic block functions
        assert_eq!(buf_idx, block.get_buf_idx());
        assert_eq!(block_size, block.slice().len());
        assert_eq!(0, block.get_checkpoint_csn());
        assert_eq!(orig_id, block.get_original_id());
        assert_eq!(0, block.get_crc());
        assert_eq!(block_id, block.get_id());

        block_id.file_id += 1;
        block_id.extent_id += 1;
        block_id.block_id += 1;
        block.set_id(block_id);
        orig_id.file_id += 2;
        orig_id.extent_id += 2;
        orig_id.block_id += 2;
        block.set_original_id(orig_id);
        csn = 45532;
        block.set_checkpoint_csn(csn);
        block.prepare_for_write();

        assert_eq!(csn, block.get_checkpoint_csn());
        assert_eq!(orig_id, block.get_original_id());
        assert!(0 != block.get_crc());
        assert_eq!(block_id, block.get_id());

        // free info function
        assert_eq!(block.fi_slice().len(), block_size - EHBLOCK_HEADER_LEN);
        assert_eq!(block.fi_slice_mut().len(), block_size - EHBLOCK_HEADER_LEN);

        let mut fi_size = 0;
        assert_eq!(block.fi_size(), fi_size);
        fi_size = 123;
        block.set_fi_size(fi_size);
        assert_eq!(block.fi_size(), fi_size);

        let mut full_cnt = 0;
        assert_eq!(block.get_full_cnt(), full_cnt);
        full_cnt = 11;
        block.set_full_cnt(full_cnt);
        assert_eq!(block.get_full_cnt(), full_cnt);


        // free info section


        let mut block_id = BlockId {
            file_id: 3,
            extent_id: 1,
            block_id: 0,
        };

        let mut orig_id = BlockId::init(0,0,0);
        let mut csn = 0;
        let buf_idx = 812;

        for i in 0..block_size { block_buf[i] = 0; }
        let mut block = FreeInfoBlock::new(block_id, buf_idx, Pinned::<BlockArea>::new(block_buf.clone(), &stub_pin));

        // basic block functions
        assert_eq!(buf_idx, block.get_buf_idx());
        assert_eq!(block_size, block.slice().len());
        assert_eq!(0, block.get_checkpoint_csn());
        assert_eq!(orig_id, block.get_original_id());
        assert_eq!(0, block.get_crc());
        assert_eq!(block_id, block.get_id());

        block_id.file_id += 1;
        block_id.extent_id += 1;
        block_id.block_id += 1;
        block.set_id(block_id);
        orig_id.file_id += 2;
        orig_id.extent_id += 2;
        orig_id.block_id += 2;
        block.set_original_id(orig_id);
        csn = 45532;
        block.set_checkpoint_csn(csn);
        block.prepare_for_write();

        assert_eq!(csn, block.get_checkpoint_csn());
        assert_eq!(orig_id, block.get_original_id());
        assert!(0 != block.get_crc());
        assert_eq!(block_id, block.get_id());

        // free info function
        assert_eq!(block.fi_slice().len(), block_size - FIBLOCK_HEADER_LEN);
        assert_eq!(block.fi_slice_mut().len(), block_size - FIBLOCK_HEADER_LEN);
    }
}

