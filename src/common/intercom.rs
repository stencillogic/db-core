/// Interthread communication

use std::sync::{Arc, Mutex, Condvar, MutexGuard};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::ops::Deref;
use std::ops::DerefMut;
use std::cell::UnsafeCell;



/// Event notification.
#[derive(Clone)]
pub struct SyncNotification<T> {
    pair: Arc<(Mutex<T>, Condvar)>, 
}


impl<T> SyncNotification<T> {
    pub fn new(initial: T) -> SyncNotification<T> {
        let pair = Arc::new((Mutex::new(initial), Condvar::new()));
        SyncNotification {
            pair
        }
    }

    /// Wait while check_cond returns true.
    /// Return associated value when check_cond returned false.
    pub fn wait_for(&self, check_cond: &mut (dyn FnMut(&T) -> bool)) -> MutexGuard<T>
        where T: PartialEq 
    {
        let (lock, cvar) = &(*self.pair);
        let mut lock_val = lock.lock().unwrap();
        while check_cond(&(*lock_val)) {
            lock_val = cvar.wait(lock_val).unwrap();
        }
        lock_val
    }

    /// Set associated value to val and notify all or just one thread depending on notify_all
    /// value.
    pub fn send(&self, val: T, notify_all: bool) {
        let (lock, cvar) = &(*self.pair);
        let mut lock_val = lock.lock().unwrap();
        *lock_val = val;
        if notify_all {
            cvar.notify_all();
        } else {
            cvar.notify_one();
        }
    }

    /// Wait while check_cond returns true and check for interrupt_cond periodically with interval
    /// specified by timeout.
    /// Return associated value or None if interrupt_cond returned true.
    pub fn wait_for_interruptable(&self, 
                                  check_cond: &mut (dyn FnMut(&T) -> bool), 
                                  interrupt_cond: &mut (dyn FnMut() -> bool), 
                                  timeout: Duration
                                  ) -> Option<MutexGuard<T>> where T: PartialEq 
    {
        let (lock, cvar) = &(*self.pair);
        let mut lock_val = lock.lock().unwrap();
        while check_cond(&(*lock_val)) {
            lock_val = cvar.wait_timeout(lock_val, timeout).unwrap().0;

            if interrupt_cond() {
                return None;
            }
        }
        Some(lock_val)
    }
/*
    pub fn notify_all(&self) {
        let (_, cvar) = &(*self.pair);
        cvar.notify_all();
    }

    pub fn notify_one(&self) {
        let (_, cvar) = &(*self.pair);
        cvar.notify_one();
    }
*/

}

/*
/// Lightweight lock
pub struct LockLw<T> {
    lock: AtomicBool,
    val: UnsafeCell<T>,
}

impl<T> LockLw<T> {

    pub fn new(val: T) -> Self {
        LockLw {
            lock: AtomicBool::new(false),
            val: UnsafeCell::new(val),
        }
    }

    pub fn lock(&self) -> LockLwGuard<T> {

        let mut i = 0;
        let cur = self.lock.load(Ordering::Relaxed);

        loop {

            let cur = self.lock.compare_and_swap(cur, true, Ordering::Relaxed);

            if !cur {
                break;
            }

            i += 1;
            if i > 10000 {
                std::thread::yield_now();
                i = 0;
            }
        }

        std::sync::atomic::fence(Ordering::Acquire);

        LockLwGuard {
            parent: self,
        }
    }

    fn unlock(&self) {
        self.lock.store(false, Ordering::Release);
    }
}

pub struct LockLwGuard<'a, T> {
    parent: &'a LockLw<T>,
}

impl<T> Drop for LockLwGuard<'_, T> {
    fn drop(&mut self) {
        self.parent.unlock();
    }
}

impl<T> Deref for LockLwGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { & *self.parent.val.get() }
    }
}

impl<T> DerefMut for LockLwGuard<'_, T> {

    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.parent.val.get() }
    }
}
*/


/// Read/write lightweight lock (multiple readers and single writer)
pub struct RwLockLw<T> {
    wr_lock: AtomicBool,
    rd_lock: AtomicU64,
    val: UnsafeCell<T>,
}

impl<T> RwLockLw<T> {

    pub fn new(val: T) -> Self {
        RwLockLw {
            wr_lock: AtomicBool::new(false),
            rd_lock: AtomicU64::new(0),
            val: UnsafeCell::new(val),
        }
    }

    pub fn read_lock(&self) -> ReadLockLwGuard<T> {

        self.wr_lock();

        self.rd_lock();

        self.wr_unlock();

        std::sync::atomic::fence(Ordering::Acquire);

        ReadLockLwGuard {
            parent: self,
        }
    }

    pub fn write_lock(&self) -> WriteLockLwGuard<T> {

        self.wr_lock();

        let mut i = 0;
        while self.rd_lock.load(Ordering::Relaxed) > 0 {
            core::sync::atomic::spin_loop_hint();
            i += 1;
            if i > 10000 {
                std::thread::yield_now();
                i = 0;
            }
        }

        std::sync::atomic::fence(Ordering::Acquire);

        WriteLockLwGuard {
            parent: self,
        }
    }

    fn wr_unlock(&self) {
        self.wr_lock.store(false, Ordering::Relaxed);
    }

    fn wr_lock(&self) {
        let mut i = 0;
        while let Err(_) = self.wr_lock.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed) {
            i += 1;
            if i > 10000 {
                std::thread::yield_now();
            }
        }
    }

    fn rd_unlock(&self) {
        self.rd_lock.fetch_sub(1, Ordering::Relaxed);
    }

    fn rd_lock(&self) {
        self.rd_lock.fetch_add(1, Ordering::Relaxed);
    }
}


/// Read-lock guard
pub struct ReadLockLwGuard<'a, T> {
    parent: &'a RwLockLw<T>,
}

impl<T> Drop for ReadLockLwGuard<'_, T> {
    
    fn drop(&mut self) {
        self.parent.rd_unlock();
    }
}

impl<T> Deref for ReadLockLwGuard<'_, T> {

    type Target = T;

    fn deref(&self) -> &T {
        unsafe { & *self.parent.val.get() }
    }
}


/// Write-lock guard
pub struct WriteLockLwGuard<'a, T> {
    parent: &'a RwLockLw<T>,
}

impl<T> Drop for WriteLockLwGuard<'_, T> {

    fn drop(&mut self) {
        self.parent.wr_unlock();
        std::sync::atomic::fence(Ordering::Release);
    }
}

impl<T> Deref for WriteLockLwGuard<'_, T> {

    type Target = T;

    fn deref(&self) -> &T {
        unsafe { & *self.parent.val.get() }
    }
}

impl<T> DerefMut for WriteLockLwGuard<'_, T> {

    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.parent.val.get() }
    }
}
