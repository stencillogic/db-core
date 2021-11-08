//! Log buffer

use std::sync::atomic::{AtomicUsize, Ordering, compiler_fence};
use std::sync::{Mutex, Condvar, Arc};
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};
use crate::common::errors::{Error};


#[cfg(feature = "metrics")]
use std::sync::atomic::AtomicU64;


thread_local! {
    static CUR_BUF: RefCell<usize> = RefCell::new(0);
}


#[repr(align(64))]
struct CacheAligned<T> (T);


/// Shared buffer which allows reserving mutable areas of memory concurrently.
/// # Safety
/// This structure is internal, and can't be considered safe by itself.
pub struct Buf<T> {
    acquire_size:   CacheAligned<AtomicUsize>,
    done_size:      CacheAligned<AtomicUsize>,
    used_size:      AtomicUsize,
    ptr:            *mut T,
    size:           usize,
}

impl<T> Buf<T> {

    /// Create a new instance with allocation of `size` items. The memory is never deallocated
    /// after the allocation.
    /// `size` must be > 0 and <= std::isize::MAX.
    /// # Errors
    /// Returns `Err` if allocation has failed.
    fn new(size: usize) -> Result<Buf<T>, Error> {

        let dt_sz = std::mem::size_of::<T>();

        if size == 0 || dt_sz == 0 || size > (std::isize::MAX as usize) / dt_sz {

            return Err(Error::incorrect_allocation_size());
        }

        let ptr: *mut T;

        unsafe {

            let align = std::mem::align_of::<T>();
            ptr = std::alloc::alloc(
                std::alloc::Layout::from_size_align(size * dt_sz, align)
                .map_err(|e| { Error::incorrect_layout(e) })?
            ) as *mut T;
        }

        if ptr.is_null() {

            Err(Error::allocation_failure())

        } else {

            Ok(Buf {
                acquire_size: CacheAligned(AtomicUsize::new(0)),
                done_size: CacheAligned(AtomicUsize::new(0)),
                used_size: AtomicUsize::new(0),
                ptr,
                size,
            })
        }
    }


    /// Sets used space count to zero.
    fn reset(&self) {

        self.used_size.store(0, Ordering::Relaxed);
        self.done_size.0.store(0, Ordering::Relaxed);

        compiler_fence(Ordering::SeqCst);

        self.acquire_size.0.store(0, Ordering::Relaxed);
    }


    /// Returns Slice instance, and "notify writer"
    fn reserve_slice(&self, reserve_size: usize, relaxed: bool) -> (Option<&mut [T]>, bool) {

        if reserve_size == 0 {

            return (Some(&mut []), false);
        }

        if reserve_size > self.size || reserve_size > std::usize::MAX - self.size {

            return (None, false);
        }

        let mut prev_acq_size = self.acquire_size.0.load(Ordering::Relaxed);

        loop {

            if prev_acq_size > self.size {

                return (None, false);
            }

            let cur_acq_size = self.acquire_size.0.compare_and_swap(
                prev_acq_size,
                prev_acq_size + reserve_size,
                Ordering::Relaxed,
            );

            if cur_acq_size == prev_acq_size {

                if cur_acq_size + reserve_size > self.size {

                    if self.size > cur_acq_size {
                        let done_size = self.size - cur_acq_size;
                        if relaxed {
                            self.used_size.fetch_add(done_size, Ordering::Relaxed);

                            let slice;
                            unsafe {

                                slice = std::slice::from_raw_parts_mut(self.ptr.offset(cur_acq_size as isize), done_size);
                            }

                            return (Some(slice), true);
                        } else {
                            let total_done = self.done_size.0.fetch_add(done_size, Ordering::Relaxed) + done_size;
                            return (None, total_done == self.size);
                        }
                    }

                    return (None, false);

                } else {

                    self.used_size.fetch_add(reserve_size, Ordering::Relaxed);

                    let slice;
                    unsafe {

                        slice = std::slice::from_raw_parts_mut(self.ptr.offset(cur_acq_size as isize), reserve_size);
                    }

                    return (Some(slice), true);
                }

            } else {

                prev_acq_size = cur_acq_size;
            }
        }
    }


    #[inline]
    fn inc_done_size(&self, reserve_size: usize) -> usize {
        return self.done_size.0.fetch_add(reserve_size, Ordering::Relaxed) + reserve_size;
    }

    /// try to take up all remaining space, return true if to notify writer
    fn reserve_rest(&self) -> bool {
        
        let reserve_size = self.size + 1;

        let mut prev_acq_size = self.acquire_size.0.load(Ordering::Relaxed);

        loop {

            if prev_acq_size > self.size {

                return false;
            }

            let cur_acq_size = self.acquire_size.0.compare_and_swap(
                prev_acq_size,
                prev_acq_size + reserve_size,
                Ordering::Relaxed,
            );

            if cur_acq_size == prev_acq_size {

                if self.size > cur_acq_size {

                    let done_size = self.size - cur_acq_size;
                    let total_done = self.done_size.0.fetch_add(done_size, Ordering::Relaxed) + done_size;

                    return total_done == self.size;
                }

                return false;

            } else {

                prev_acq_size = cur_acq_size;
            }
        }
    }

    /// Returns buffer for reading.
    fn acquire_for_read(&self) -> &mut [T] {

        let total_written = self.used_size.load(Ordering::Relaxed);

        let ret;

        unsafe {

            ret = std::slice::from_raw_parts_mut(self.ptr, total_written);
        };

        ret
    }
}


impl<T> Drop for Buf<T> {

    fn drop(&mut self) {

        let align = std::mem::align_of::<T>();

        unsafe {

            std::alloc::dealloc(self.ptr as *mut u8, std::alloc::Layout::from_size_align(self.size, align).unwrap());
        }
    }
}


unsafe impl<T> Sync for Buf<T> {}
unsafe impl<T> Send for Buf<T> {}

/// Metrics values.
#[derive(Debug)]
pub struct Metrics {
    wait_time:   u64,
    wait_count:  u64,
}

#[cfg(feature = "metrics")]
struct MetricsInternal {
    wait_time:      CacheAligned<AtomicU64>,
    wait_count:     CacheAligned<AtomicU64>,
}



/// Doubled Buf instances (flip-flop buffer)
pub struct DoubleBuf<T> {
    bufs: Arc<Vec<Buf<T>>>,
    #[cfg(feature = "metrics")]
    metrics: Arc<MetricsInternal>,
    buf_state: Arc<(Mutex<[BufState; 2]>, Condvar, Condvar)>,
    size: usize,
}


impl<T> DoubleBuf<T> {


    /// Create an instance of buffer pair, each of size `sz`.
    pub fn new(sz: usize) -> Result<DoubleBuf<T>, Error> {

        let bufs = Arc::new(vec![Buf::<T>::new(sz)?, Buf::new(sz)?]);

        let buf_state = Arc::new((Mutex::new([BufState::Appendable, BufState::Appendable]), Condvar::new(), Condvar::new()));

        #[cfg(feature = "metrics")]
        let metrics = Arc::new(MetricsInternal {
            wait_time: CacheAligned(AtomicU64::new(0)),
            wait_count: CacheAligned(AtomicU64::new(0)),
        });

        Ok(DoubleBuf {
            bufs,
            #[cfg(feature = "metrics")]
            metrics,
            buf_state,
            size: sz,
        })
    }

    /// return number of buffers
    #[inline]
    pub fn get_buf_cnt(&self) -> usize {

        self.bufs.len()
    }

    fn try_reserve(&self, buf_id: usize, reserve_size: usize, relaxed: bool) -> Option<Slice<T>> {

        match self.bufs[buf_id].reserve_slice(reserve_size, relaxed) {
            (None, notify) => {
                if notify {
                    self.set_buf_readable(buf_id);
                }
                return None;
            },
            (Some(slice), _) => {
                CUR_BUF.with( |v| {
                    *v.borrow_mut() = buf_id; 
                });

                return Some(Slice {
                    slice,
                    parent: self,
                    buf_id,
                });
            }
        }
    }

    /// Reserve slice for write.
    pub fn reserve_slice(&self, reserve_size: usize, relaxed: bool) -> Result<Slice<T>, ()> {

        let mut cur_buf = 0;

        CUR_BUF.with( |v| {
            cur_buf = *v.borrow(); 
        });

        let mut appendable = 0;

        loop {

            if let Some(slice) = self.try_reserve(cur_buf, reserve_size, relaxed) {

                return Ok(slice);

            } else if let Some(slice) = self.try_reserve(1 - cur_buf, reserve_size, relaxed) {

                return Ok(slice);

            } else {

                if appendable > 0 {

                    #[cfg(feature = "metrics")] 
                    let now = std::time::Instant::now();

                    std::thread::yield_now();

                    if appendable > 10000 {
                        std::thread::sleep(std::time::Duration::new(0,10_000_000));
                        appendable = 0;
                    }

                    #[cfg(feature = "metrics")] 
                    self.inc_metrics(1, std::time::Instant::now().duration_since(now).as_nanos() as u64);
                }

                let (buf_id, state) = self.wait_for(BufState::Appendable as u32 | BufState::Terminated as u32);

                if state == BufState::Terminated {
                    return Err(());
                }

                cur_buf = buf_id;

                appendable += 1;
            }
        }
    }


    /// Return buffer slice with data and bffer id.
    /// If the buffer is not full/ready the method blocks until buffer is ready for processing.
    pub fn reserve_for_read(&self) -> (&mut [T], usize) {

        let (buf_id, _) = self.wait_for(BufState::Readable as u32);

        return (self.bufs[buf_id].acquire_for_read(), buf_id);
    }


    /// Wait until one of the buffers has certain `state`.
    /// Return buffer id with that state.
    fn wait_for(&self, state: u32) -> (usize, BufState) {

        let (ref lock, ref cvar_a, ref cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        let cvar = DoubleBuf::<T>::determine_cvar(state, cvar_a, cvar_r);

        loop {

            for i in 0..cur_state.len() {

                if 0 != (cur_state[i] as u32 & state) {

                    return (i, cur_state[i]);
                }
            }

            #[cfg(feature = "metrics")] {
                let now = std::time::Instant::now();
                cur_state = cvar.wait(cur_state).unwrap();
                self.inc_metrics(1, std::time::Instant::now().duration_since(now).as_nanos() as u64);
            }

            #[cfg(not(feature = "metrics"))] {
                cur_state = cvar.wait(cur_state).unwrap();
            }
        }
    }


    #[cfg(feature = "metrics")]
    #[inline]
    fn inc_metrics(&self, wait_cnt: u64, wait_time: u64) {
        self.metrics.wait_time.0.fetch_add(wait_time, Ordering::Relaxed);
        self.metrics.wait_count.0.fetch_add(wait_cnt, Ordering::Relaxed);
    }


    #[cfg(feature = "metrics")]
    pub fn get_metrics(&self) -> Metrics {
        #[cfg(feature = "metrics")] {
            Metrics {
                wait_time: self.metrics.wait_time.0.load(Ordering::Relaxed),
                wait_count:  self.metrics.wait_count.0.load(Ordering::Relaxed),
            }
        }

        #[cfg(not(feature = "metrics"))] {
            Metrics {
                wait_time:  0,
                wait_count: 0,
            }
        }
    }


    /// Wait until i'th buffer has certain `state`.
    fn wait_for_buf(&self, state: u32, buf_id: usize) -> BufState {

        let (ref lock, ref cvar_a, ref cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        let cvar = DoubleBuf::<T>::determine_cvar(state, cvar_a, cvar_r);

        loop {

            if 0 != (cur_state[buf_id] as u32 & state) {

                return cur_state[buf_id];
            }

            cur_state = cvar.wait(cur_state).unwrap();
        }
    }

    fn set_buf_readable(&self, buf_id: usize) {

        let (ref lock, ref _cvar_a, ref cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        cur_state[buf_id] = BufState::Readable;

        cvar_r.notify_all();
    }


    pub fn set_buf_terminated(&self, buf_id: usize) {

        let (ref lock, ref cvar_a, ref _cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        cur_state[buf_id] = BufState::Terminated;

        cvar_a.notify_all();
    }


    pub fn set_buf_appendable(&self, buf_id: usize) {

        let (ref lock, ref cvar_a, ref _cvar_r) = *self.buf_state;

        let mut cur_state = lock.lock().unwrap();

        compiler_fence(Ordering::SeqCst);
        self.bufs[buf_id].reset();
        compiler_fence(Ordering::SeqCst);

        cur_state[buf_id] = BufState::Appendable;

        cvar_a.notify_all();
    }


    #[inline]
    fn determine_cvar<'a>(state: u32, cvar_a: &'a Condvar, cvar_r: &'a Condvar) -> &'a Condvar {

        if 0 != state & (BufState::Readable as u32) { cvar_r } else { cvar_a }
    }


    /// Flush the last used in current thread buffer
    pub fn flush(&self) {

        let mut buf_id = 0;

        CUR_BUF.with( |v| {
            buf_id = *v.borrow(); 
        });

        self.flush_buf(buf_id);

        CUR_BUF.with( |v| {
            *v.borrow_mut() = 1 - buf_id;
        });
    }

    fn flush_buf(&self, buf_id: usize) {

        if self.bufs[buf_id].reserve_rest() {

            self.set_buf_readable(buf_id);

            self.wait_for_buf(BufState::Appendable as u32, buf_id);
        }
    }

    /// Prevent buffers from writing
    pub fn seal_buffers(&self) {

        let mut sealed = [false; 2];

        loop {

            for buf_id in 0..2 {

                if ! sealed[buf_id] {
                    if self.bufs[buf_id].reserve_rest() {
                        self.set_buf_readable(buf_id);
                    }
                }
            }

            for buf_id in 0..2 {

                if ! sealed[buf_id] {

                    let state = self.wait_for_buf(BufState::Terminated as u32 | BufState::Appendable as u32, buf_id);

                    sealed[buf_id] = state == BufState::Terminated;
                }
            }

            if sealed[0] && sealed[1] {

                break;

            } else {

                std::thread::sleep(std::time::Duration::new(0,10_000_000));
            }
        }
    }
}


impl<T> Clone for DoubleBuf<T> {
    fn clone(&self) -> Self {
        DoubleBuf {
            bufs: self.bufs.clone(),
            #[cfg(feature = "metrics")]
            metrics: self.metrics.clone(),
            buf_state: self.buf_state.clone(),
            size: self.size,
        }
    }
}

/// Buffer states for buffer tracking
#[derive(Copy, Clone, PartialEq, Debug)]
enum BufState {
    Appendable = 0b001,
    Readable = 0b010,
    Terminated = 0b100,
}


/// Wrapper for writable slice of [T].
pub struct Slice<'a, T> {
    slice:  &'a mut [T],
    parent: &'a DoubleBuf<T>,
    buf_id: usize,
}


impl<'a, T> Deref for Slice<'a, T> {

    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.slice
    }
}

impl<'a, T> DerefMut for Slice<'a, T> {

    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slice
    }
}

impl<'a, T> Drop for Slice<'a, T> {

    fn drop(&mut self) {

        let buf = &self.parent.bufs[self.buf_id];
        let total_done = buf.inc_done_size(self.slice.len());
        if total_done == self.parent.size {
            self.parent.set_buf_readable(self.buf_id);
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use std;

    #[test]
    fn too_big_size() {
        if let Ok(_) = Buf::<u8>::new(std::isize::MAX as usize) {
            panic!("Buf::new takes size value more than expected");
        }

        if let Ok(_) = Buf::<u32>::new((std::isize::MAX as usize) / std::mem::size_of::<u32>() + 1) {
            panic!("Buf::new takes size value more than expected");
        }
    }

    #[test]
    fn zero_size() {
        if let Ok(_) = Buf::<u8>::new(0) {
            panic!("Buf::new takes zero size");
        }

        struct Tst {}

        if let Ok(_) = Buf::<Tst>::new(123) {
            panic!("Buf::new takes zero size");
        }
    }
}
