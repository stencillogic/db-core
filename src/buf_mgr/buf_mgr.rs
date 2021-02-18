/// Data buffer management

use crate::common::errors::Error;
use crate::common::defs::BlockId;
use crate::common::misc::alloc_buf;
use crate::common::misc::dealloc_buf;
use crate::common::intercom::RwLockLw;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::Mutex;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicU64;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::RwLockWriteGuard;


const DIRTY_BIT: u64        = 0x4000000000000000;  // if set then block is dirty
const PINLOCK_BIT: u64      = 0x8000000000000000;  // if set then block can't be pinned
const PIN_COUNTER_MASK: u64 = 0x0fffffffffffffff;  // bits dedicated for counting pins.


#[derive(Copy, Clone)]
pub struct BlockDesc {
    pub id: usize,
    pub block_id: BlockId,
    pub dirty: bool,
    pub block_type: BlockType,
    pub checkpoint_block_id: BlockId,
    pub checkpoint_written: bool,
}


#[derive(Clone, Copy, PartialEq, Hash)]
pub enum BlockType {
    NotUsed,
    DataBlock,
    VersionBlock,
    CheckpointBlock,
}

unsafe impl <I,E> Send for BufMgr<I,E> {}
unsafe impl <I,E> Sync for BufMgr<I,E> {}


/// Block memory area accessible as slice.
pub struct BlockArea {
    data:       *mut u8,
    block_size: usize,
}

impl BlockArea {

    pub fn new(data: *mut u8, block_size: usize) -> Self {
        BlockArea {
            data,
            block_size,
        }
    }

    pub fn data_ptr(&self) -> *mut u8 {
        self.data
    }

    pub fn size(&self) -> usize {
        self.block_size
    }
}

unsafe impl Send for BlockArea {}
unsafe impl Sync for BlockArea {}


impl Deref for BlockArea {

    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.data as *const u8, self.block_size)
        }
    }
}

impl DerefMut for BlockArea {

    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.data, self.block_size)
        }
    }
}

impl Clone for BlockArea {

    fn clone(&self) -> Self {
        BlockArea {
            data: self.data,
            block_size: self.block_size,
        }
    }
}



/// Buffer of blocks is preallocated continuous region of memory divided by blocks.
/// Buffer allows search by block id, eviction of unused blocks and allocation of new blocks in
/// place of evicted according to eviction mechanism provided for buffer manager.
pub struct BufMgr<I, E> {
    mem:                *mut u8,
    block_desc:         Arc<Vec<RwLockLw<BlockDesc>>>,
    block_map:          Arc<RwLock<HashMap<BlockId, I>>>,
    eviction_mech:      Arc<Mutex<E>>,
    pins:               Arc<Vec<AtomicU64>>,
    block_size:         usize,
    block_num:          usize,
}

impl<I,E> Clone for BufMgr<I,E> {

    fn clone(&self) -> Self {
        BufMgr {
            mem:                self.mem,
            block_desc:         self.block_desc.clone(),
            block_map:          self.block_map.clone(),
            eviction_mech:      self.eviction_mech.clone(),
            pins:               self.pins.clone(),
            block_size:         self.block_size,
            block_num:          self.block_num,
        }
    }
}

impl<I, E> BufMgr<I, E> 
    where I: CacheItem<(usize, BlockId)>, 
{
    pub fn new<Q>(block_size: usize, block_num: usize) -> Result<Self, Error>
        where E: EvictionMech<(usize, BlockId), I, Q>, Q: CacheItemIterator<(usize, BlockId), I> 
    {
        let mem = alloc_buf(block_num*block_size)?;

        let mut block_desc = Vec::with_capacity(block_num);

        for i in 0..block_num {
            let bd = RwLockLw::new(BlockDesc {
                id: i,
                block_id: BlockId::new(),
                dirty: false,
                block_type: BlockType::NotUsed,
                checkpoint_block_id: BlockId::new(),
                checkpoint_written: true,
            });
            block_desc.push(bd);
        };

        let block_map = RwLock::new(HashMap::with_capacity(block_num * 2));

        let mut eviction_mech = E::new((0, BlockId::new()));
        for i in 1..block_num {
            eviction_mech.add_item((i, BlockId::new()));
        }

        let mut pins = Vec::with_capacity(block_num);
        for _ in 0..block_num {
            pins.push(AtomicU64::new(0));
        }

        Ok(BufMgr::<I, E> {
            mem,
            block_desc: Arc::new(block_desc),
            block_map: Arc::new(block_map),
            eviction_mech: Arc::new(Mutex::new(eviction_mech)),
            pins: Arc::new(pins),
            block_size,
            block_num,
        })
    }


    /// return block for reading, return None if block is not in cache
    pub fn get_block<Q>(&self, block_id: &BlockId) -> Option<(Pinned<BlockArea>, usize)>
        where E: EvictionMech<(usize, BlockId), I, Q>, Q: CacheItemIterator<(usize, BlockId), I> 
    {
        // acquire read lock
        // get block from hash map
        // pin the block
        // update lru position for the block
        // unlock and return block

        let block_map = self.block_map.read().unwrap();

        if let Some(item) = block_map.get(block_id) {
            let buf_idx = item.get_value().0;
            if let Some(pinned_block) = self.try_pin(buf_idx) {

                let mut em = self.eviction_mech.lock().unwrap();
                em.on_access(item.clone());

                return Some((pinned_block, buf_idx));
            }
        }

        None
    }

    /// put block on cache (if it not yet there)
    pub fn allocate_on_cache<Q>(&self, block_id: &BlockId, block_type: BlockType) -> Option<(Pinned<BlockArea>, usize)>
        where E: EvictionMech<(usize, BlockId), I, Q>, Q: CacheItemIterator<(usize, BlockId), I> 
    {
        // acquire write lock
        // check if block is already there
        // if block is there then pin the block, unlock, and return block
        // otherwise, find a free block
        // add block to hash map
        // pin the block
        // update lru position
        // unlock and return

        let mut block_map = self.block_map.write().unwrap();

        if let Some(mut item) = self.get_free_block(&mut block_map) {
            item.get_value_mut().1 = *block_id;
            Some(self.put_on_map(block_map, item, block_type))
        } else {
            None
        }
    }

    /// get block by index
    pub fn get_block_by_idx(&self, idx: usize) -> Option<Pinned<BlockArea>> {
        // acquire read lock
        // pin the block
        // unlock and return block
        // (lru position is not updated since this function is used only by writers to disk)

        if idx < self.block_num {
            let lock = self.block_map.read().unwrap();
            let ret = self.try_pin(idx);
            drop(lock);
            ret
        } else {
            None
        }
    }

    /// get block descriptor by index
    pub fn get_bdesc_by_idx(&self, idx: usize) -> Option<BlockDesc> {
        if idx < self.block_num {
            let mut bdesc = *(self.block_desc[idx].read_lock());
            bdesc.dirty = 0 != (self.pins[idx].load(Ordering::Relaxed) & DIRTY_BIT);
            Some(bdesc)
        } else {
            None
        }
    }

    /// Mark the block in buffer if checkpoint block is written to disk or not.
    pub fn set_checkpoint_written(&self, idx: usize, state: bool) {
        let mut bdesc = self.block_desc[idx].write_lock();
        (*bdesc).checkpoint_written = state;
    }

    /// Set checkpoint block id
    pub fn set_checkpoint_block_id(&self, idx: usize, block_id: BlockId) {
        let mut bdesc = self.block_desc[idx].write_lock();
        (*bdesc).checkpoint_block_id = block_id;
    }

    /// Mark the block in buffer if it is dirty or not.
    pub fn set_dirty(&self, idx: usize, state: bool) {
        let mut cur = self.pins[idx].load(Ordering::Relaxed);
        loop {
            // check if set
            if (state && (cur & DIRTY_BIT != 0)) ||
                (!state && (cur & DIRTY_BIT == 0)) {
                    return;
            }

            // apply bit
            let new_val = if state {
                cur | DIRTY_BIT
            } else {
                cur & (!DIRTY_BIT)
            };

            let cur2 = self.pins[idx].compare_and_swap(cur, new_val, Ordering::Relaxed);
            if cur2 == cur {
                return;
            } else {
                cur = cur2;
            }
        }
    }


    fn get_block_area(&self, idx: usize) -> BlockArea {
        BlockArea {
            data: unsafe { self.mem.offset(idx as isize * self.block_size as isize) },
            block_size: self.block_size,
        }
    }

    fn put_on_map<Q>(&self, mut block_map: RwLockWriteGuard<HashMap<BlockId, I>>, val: I, block_type: BlockType) -> (Pinned<BlockArea>, usize)
        where E: EvictionMech<(usize, BlockId), I, Q>, Q: CacheItemIterator<(usize, BlockId), I> 
    {
        let block_id = val.get_value().1;

        if let Some(item) = block_map.get_mut(&block_id) {
            *item = val.clone();
        } else {
            block_map.insert(block_id, val.clone());
        };

        let id = val.get_value().0;

        self.pinunlock_pin(id);

        let mut bdesc = self.block_desc[id].write_lock();
        bdesc.block_type = block_type;
        bdesc.block_id = block_id;
        bdesc.checkpoint_block_id = BlockId::new();
        bdesc.checkpoint_written = true;
        drop(bdesc);

        (Pinned::new(self.get_block_area(id), &self.pins[id]), id)
    }

    fn try_remove_from_map(&self, buf_idx: usize, block_id: &BlockId, block_map: &mut HashMap<BlockId, I>) -> bool {
        if self.try_pinlock(buf_idx) {
            block_map.remove(block_id);
            return true;
        }
        false
    }

    fn get_free_block<Q>(&self, block_map: &mut HashMap<BlockId, I>) -> Option<I>
        where E: EvictionMech<(usize, BlockId), I, Q>, Q: CacheItemIterator<(usize, BlockId), I> 
    {
        // lock linked list.
        // go throgh linked list of lru starting from the head.
        // for each block of ll:
        //   skip block if it is dirty
        //   check if block is pinned, and if not try to prevent it from pinning atomically.
        //   after block is marked as not pinnable remove it from hash map. 
        //   move correponding node to the tail of the lined list.
        //   unlock linked list.
        //   return index of the free block.
        let mut em = (&self).eviction_mech.lock().unwrap();

        let mut iter: Q = em.iter();
        while let Some(item) = iter.next() {
            let (buf_idx, block_id) = item.get_value();

            if (&self).try_remove_from_map(*buf_idx, &block_id, block_map) {
                em.on_access(item.clone());
                return Some(item);
            }
        }
        drop(em);

        None
    }

    // try pinning a block.
    fn try_pin(&self, buf_idx: usize) -> Option<Pinned<BlockArea>> {
        let mut cur = self.pins[buf_idx].load(Ordering::Relaxed);
        loop {
            if cur & PINLOCK_BIT != 0 {
                return None;
            }

            let cur2 = self.pins[buf_idx].compare_and_swap(cur, cur+1, Ordering::Relaxed);
            if cur2 == cur {
                return Some(Pinned::new(self.get_block_area(buf_idx), &self.pins[buf_idx]));
            } else {
                cur = cur2;
            }
        }
    }

    // try to mark block as not avaialable for pinning.
    fn try_pinlock(&self, buf_idx: usize) -> bool {
        let cur = self.pins[buf_idx].load(Ordering::Relaxed);
        if cur & (PINLOCK_BIT | PIN_COUNTER_MASK) != 0 {
            return false;
        } else {
            self.pins[buf_idx].compare_and_swap(cur, cur | PINLOCK_BIT, Ordering::Relaxed) == 0
        }
    }

    // mark a block as avilable for pinning, and pin once.
    fn pinunlock_pin(&self, buf_idx: usize) {
        self.pins[buf_idx].store(1, Ordering::Relaxed);
    }
}


impl<I,E> Drop for BufMgr<I, E> {

    fn drop(&mut self) {
        let BufMgr {
            mem,
            block_desc,
            block_map: _,
            eviction_mech: _,
            pins: _,
            block_size,
            block_num,
        } = self;
        
        if Arc::strong_count(&block_desc) == 1 {
            // we are the last instance, deallocate memory
            dealloc_buf(*mem, (*block_size) * (*block_num));
        }
    }
}



/// A block pinned in buffer. 
/// Pin counter decreases by 1 when struct instance is dropped.
pub struct Pinned<'a, T> {
    value: T,
    pin: &'a AtomicU64,
}

impl<'a, T> Pinned<'a, T>  {
    pub fn new(value: T, pin: &'a AtomicU64) -> Self {
        Pinned::<'a, T> {
            value,
            pin,
        }
    }
}

impl<'a, T> Drop for Pinned<'a, T> {
    fn drop(&mut self) {
        self.pin.fetch_sub(1, Ordering::Relaxed);
    }
}


impl<T> Deref for Pinned<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for Pinned<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}


// Eviction mechanism specification.
// Client first adds items to the eviction mech, then uses on_access function to refresh item
// state when item is accessed. When client needs item to be evicted and reused it uses iterator over 
// items and chooses suitable item. After item is chosen it can be modified and on_access can be 
// called once more to reflect new state of the item.
pub trait EvictionMech<T, I, Q> 
    where I: CacheItem<T>, Q: CacheItemIterator<T, I>
{
    // create an instance.
    fn new(value: T) -> Self;

    // register an additional item in cache.
    fn add_item(&mut self, value: T);

    // updates eviction priority of the item.
    fn on_access(&mut self, item: I);

    // iterator of items for potential eviction.
    fn iter(&self) -> Q;
}

pub trait CacheItem<T> {

    // return associated value.
    fn get_value(&self) -> &T;

    // return associated value.
    fn get_value_mut(&mut self) -> &mut T;

    // clone item reference.
    fn clone(&self) -> Self;
}

pub trait CacheItemIterator<T, I: CacheItem<T>> {

    // return next item or None if no items left.
    fn next(&mut self) -> Option<I>;
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::buf_mgr::lru::LruList;
    use crate::buf_mgr::lru::LruNodeRef;

    #[test]
    fn test() {
        assert_eq!(1, 1);
    }

    #[test]
    fn test_buf_mgr() {
        let block_size = 8192;
        let block_num = 100;
        let bm = BufMgr::<LruNodeRef<(usize, BlockId)>, LruList<(usize, BlockId)>>::new(block_size, block_num).expect("Failed to create BufMgr");

        let mut block_id = BlockId {
            file_id: 0,
            extent_id: 0,
            block_id: 0,
        };
        assert!(bm.get_block(&block_id).is_none());

        let mut block_buf = [0u8; 8192];
        block_buf[0] = 1;

        let (mut block, buf_idx) = bm.allocate_on_cache(&block_id, BlockType::DataBlock).expect("Failed to allocate block");
        block.copy_from_slice(&block_buf);
        assert!(bm.get_block(&block_id).is_some());
        drop(block);

        for i in 0..block_num {
            block_id.block_id += 1;
            block_buf[0] += 1;
            let (mut block, buf_idx) = bm.allocate_on_cache(&block_id, BlockType::DataBlock).expect("Failed to allocate block");
            block.copy_from_slice(&block_buf);
            drop(block);
        }
        block_id.block_id = 0;
        assert!(bm.get_block(&block_id).is_none());

        for i in 0..block_num {
            block_id.block_id += 1;
            let (block, buf_idx) = bm.get_block(&block_id).expect("Block was not found");
            assert_eq!(i + 2, (&block)[0] as usize);
            drop(block);
        }

        block_id.block_id = 10;
        let (block, buf_idx) = bm.get_block(&block_id).expect("Block was not found");
        drop(block);

        block_id.block_id = 1000;
        for i in 1..block_num {
            block_id.block_id += 1;
            let (mut block, buf_idx) = bm.allocate_on_cache(&block_id, BlockType::DataBlock).expect("Failed to allocate block");
            block.copy_from_slice(&block_buf);
            assert!(bm.get_block(&block_id).is_some());
            drop(block);
        }

        block_id.block_id = 0;
        for i in 0..block_num {
            block_id.block_id += 1;
            if block_id.block_id == 10 {
                bm.get_block(&block_id).expect("Block was not found");
            } else {
                assert!(bm.get_block(&block_id).is_none());
            }
        }

        for i in 0..block_num {
            assert!(bm.get_block_by_idx(i).is_some());
        }

        let bdesc = bm.get_bdesc_by_idx(0).expect("No block description found");
        assert!(!bdesc.dirty);
        assert!(bdesc.checkpoint_written);
        bm.set_dirty(1, true);
        let bdesc = bm.get_bdesc_by_idx(0).expect("No block description found");
        assert!(!bdesc.dirty);
        assert!(bdesc.checkpoint_written);
        bm.set_dirty(0, true);
        let bdesc = bm.get_bdesc_by_idx(0).expect("No block description found");
        assert!(bdesc.dirty);
        assert!(bdesc.checkpoint_written);

        bm.set_checkpoint_written(0, false);
        let bdesc = bm.get_bdesc_by_idx(0).expect("No block description found");
        assert!(!bdesc.checkpoint_written);

        bm.set_dirty(0, false);
        let bdesc = bm.get_bdesc_by_idx(0).expect("No block description found");
        assert!(!bdesc.dirty);

        let chkbid = BlockId::init(1,2,3);
        bm.set_checkpoint_block_id(0, chkbid);
        let bdesc = bm.get_bdesc_by_idx(0).expect("No block description found");
        assert_eq!(bdesc.checkpoint_block_id, chkbid);
    }
}


