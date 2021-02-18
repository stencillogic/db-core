/// Different reusable definitions


use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};


/// object identifier
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

/// block identifier
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

/// Numberic sequence
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
        self.sn.fetch_add(1, Ordering::Relaxed)
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

