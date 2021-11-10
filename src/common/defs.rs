//! Common definitions.


use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::common::misc::SliceToIntConverter;


/// Object identifier.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct ObjectId
{
    pub file_id:    u16,
    pub extent_id:  u16,
    pub block_id:   u16,
    pub entry_id:   u16,
}

impl ObjectId {

    pub fn new() -> Self {
        ObjectId {
            file_id:   0,
            extent_id: 0,
            block_id:  0,
            entry_id:  0,
        }
    }

    pub fn init(file_id: u16,
            extent_id: u16,
            block_id: u16,
            entry_id: u16) -> Self 
    {
        ObjectId {
            file_id,
            extent_id,
            block_id,
            entry_id,
        }
    }

    /// Calculate bucket for certain number of buckets.
    pub fn obj_bkt(&self, nbkt: usize) -> usize {
    	(self.block_id as usize + 
         self.extent_id as usize + 
         self.entry_id as usize + 
         self.file_id as usize) % nbkt
    }
}

/// Block identifier.
#[derive(Clone, Copy, Eq, Hash, PartialEq, Debug)]
pub struct BlockId
{
    pub file_id:    u16,
    pub extent_id:  u16,
    pub block_id:   u16,
}


impl BlockId {

    pub fn new() -> Self {
        BlockId {
            file_id: 0,
            extent_id: 0,
            block_id: 0,
        }
    }

    pub fn init(file_id: u16, extent_id: u16, block_id: u16) -> Self {
        BlockId {
            file_id,
            extent_id,
            block_id,
        }
    }

    pub fn from_obj(obj: &ObjectId) -> Self {
        BlockId {
            file_id: obj.file_id,
            extent_id: obj.extent_id,
            block_id: obj.block_id,
        }
    }

    pub fn hash(&self, n: usize) -> usize {
    	(self.block_id as usize +
         self.extent_id as usize +
         self.file_id as usize) % n
    }
}

/// Numeric sequence that can be shared by several threads.
#[derive(Clone)]
pub struct Sequence {
    sn: Arc<AtomicU64>,
}

impl Sequence {
    pub fn new(sn: u64) -> Self {
        Sequence {
            sn: Arc::new(AtomicU64::new(sn)),
        }
    }

    pub fn set(&self, sn: u64) {
        self.sn.store(sn, Ordering::Relaxed)
    }

    pub fn get_next(&self) -> u64 {
        self.sn.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn get_cur(&self) -> u64 {
        self.sn.load(Ordering::Relaxed)
    }
/*
    pub fn swap(&self, sn: u64) {
        let mut current = self.sn.load(Ordering::Relaxed);
        while  current < sn {
            current = self.sn.compare_and_swap(current, sn, Ordering::Relaxed);
        }
    }
*/
}

/// Change sequence numbers that often are shared gathered into one struct.
#[derive(Clone)]
pub struct SharedSequences {
    pub csn:                Sequence,
    pub latest_commit_csn:  Arc<AtomicU64>,
    pub checkpoint_csn:     Sequence,
}



pub const VECTOR_DATA_LENGTH: usize = 10;


/// Vector (data pointer). 
#[derive(Clone, Copy, Debug)]
pub struct Vector {
    obj:        ObjectId,
    entry_pos:  u16,
    data:       [u8;VECTOR_DATA_LENGTH],
}


impl Vector {
    pub fn new() -> Self {
        Vector {
            obj: ObjectId::new(),
            entry_pos: 0,
            data: [0;VECTOR_DATA_LENGTH],
        }
    }

    pub fn init(block_id: BlockId, entry_id: u16, entry_pos: u16) -> Self {
        let obj = ObjectId::init(block_id.file_id, block_id.extent_id, block_id.block_id, entry_id);
        Vector {
            obj,
            entry_pos,
            data: [0;VECTOR_DATA_LENGTH],
        }
    }

    pub fn update_from_buf(&mut self) {
        self.obj.file_id = u16::slice_to_int(&self.data[0..2]).unwrap();
        self.obj.extent_id = u16::slice_to_int(&self.data[2..4]).unwrap();
        self.obj.block_id = u16::slice_to_int(&self.data[4..6]).unwrap();
        self.obj.entry_id = u16::slice_to_int(&self.data[6..8]).unwrap();
        self.entry_pos = u16::slice_to_int(&self.data[8..10]).unwrap();
    }

    pub fn to_data(&mut self) -> &[u8] {
        let mut slice = &mut self.data[0..VECTOR_DATA_LENGTH];
        slice.write_all(&self.obj.file_id.to_ne_bytes()).unwrap();
        slice.write_all(&self.obj.extent_id.to_ne_bytes()).unwrap();
        slice.write_all(&self.obj.block_id.to_ne_bytes()).unwrap();
        slice.write_all(&self.obj.entry_id.to_ne_bytes()).unwrap();
        slice.write_all(&self.entry_pos.to_ne_bytes()).unwrap();
        slice.flush().unwrap();
        &self.data
    }

    pub fn buf_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn buf(&self) -> &[u8] {
        &self.data
    }

    pub fn obj_id(&self) -> ObjectId {
        self.obj
    }

    pub fn entry_pos(&self) -> u16 {
        self.entry_pos
    } 
}

/// Seek postition.
pub enum SeekFrom {
    Start,
    Current,
}

