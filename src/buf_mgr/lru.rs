/// LRU eviction mechanism implementation.

use crate::buf_mgr::buf_mgr::EvictionMech;
use crate::buf_mgr::buf_mgr::CacheItem;
use crate::buf_mgr::buf_mgr::CacheItemIterator;


/// Lru linked list node.
struct LruNode<T> {
    value:  T,
    prev:   Option<*mut LruNode<T>>,
    next:   Option<*mut LruNode<T>>,
}

pub struct LruNodeRef<T> {
    ptr: *mut LruNode<T>
}

impl<T> LruNodeRef<T> {

    fn new(node: *mut LruNode<T>) -> Self {
        LruNodeRef {
            ptr: node
        }
    }

    fn as_node_ptr(&self) -> *mut LruNode<T> {
        self.ptr
    }
}


impl<T> CacheItem<T> for LruNodeRef<T> {

    fn get_value(&self) -> &T {
        unsafe { &(*self.ptr).value }
    }

    fn get_value_mut(&mut self) -> &mut T {
        unsafe { &mut (*self.ptr).value }
    }

    fn clone(&self) -> Self {
        LruNodeRef {
            ptr: self.ptr,
        }
    }
}



/// Lru eviction mechanism implementation with linked list.
pub struct LruList<T> {
    head:   *mut LruNode<T>,
    tail:   *mut LruNode<T>,
}


impl<T> EvictionMech<T, LruNodeRef<T>, LruNodeIter<T>> for LruList<T> {

    fn new(value: T) -> Self {
        let node = Box::into_raw(Box::new(LruNode {
            value,
            prev: None,
            next: None,
        }));

        LruList {
            head: node,
            tail: node,
        }
    }

    // register an item in cache.
    fn add_item(&mut self, value: T) {

        let node = Box::into_raw(Box::new(LruNode {
            value,
            prev: Some(self.head),
            next: None,
        }));

        unsafe { (*self.head).next = Some(node); }

        self.head = node;
    }

    // updates eviction priority of the item.
    fn on_access(&mut self, item: LruNodeRef<T>) {

        let node = item.as_node_ptr();

        // move node to head position
        unsafe {
            if let Some(next) = (*node).next {
                (*next).prev = (*node).prev;

                if let Some(prev) = (*node).prev {
                    (*prev).next = (*node).next;
                } else {
                    self.tail = next;
                }

                (*node).next = None;
                (*node).prev = Some(self.head);
                (*self.head).next = Some(node);
                self.head = node;
            }
        }
    }

    // iterator of items for potential eviction.
    fn iter(&self) -> LruNodeIter<T> {
        LruNodeIter {
            node: Some(self.tail),
        }
    }
}


/// Lru linked list iterator.
pub struct LruNodeIter<T> {
    node: Option<*mut LruNode<T>>,
}

impl<T> CacheItemIterator<T, LruNodeRef<T>> for LruNodeIter<T> {

    fn next(&mut self) -> Option<LruNodeRef<T>> {
        if let Some(ret) = self.node {
            self.node = unsafe { (*ret).next };
            Some(LruNodeRef::new(ret))
        } else {
            None
        }
    }
}
