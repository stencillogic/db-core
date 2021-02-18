//! Different utility functions and other stuff

use std::convert::TryInto;
use std::time::SystemTime;
use crate::common::errors::Error;


pub const BYTE_BITS: [u8; 8] = [0b00000001,0b00000010,0b00000100,0b00001000,0b00010000,0b00100000,0b01000000,0b10000000,];


pub trait SliceToIntConverter<T> {
    fn slice_to_int(slice: &[u8]) -> Result<T, Error>;
}

macro_rules! impl_for {
    ( $t:ty ) => {
        impl SliceToIntConverter<$t> for $t {

            #[inline]
            fn slice_to_int(slice: &[u8]) -> Result<$t, Error> {
                let val = slice.try_into()?;
                Ok(<$t>::from_ne_bytes(val))
            }
        }
    };
}

impl_for!(u8);
impl_for!(u16);
impl_for!(u32);
impl_for!(u64);
impl_for!(u128);

impl_for!(i8);
impl_for!(i16);
impl_for!(i32);
impl_for!(i64);
impl_for!(i128);


/// Return Unix epoch time in seconds.
pub fn epoch_as_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Current time is earlier than Unix epoch. Check time settings.")
        .as_secs()
}


/// Allocate byte buffer on heap
pub fn alloc_buf(size: usize) -> Result<*mut u8, Error> {

    if size == 0 || size > (std::isize::MAX as usize) {
        return Err(Error::incorrect_allocation_size());
    }

    let ptr: *mut u8;

    unsafe {
        let align = std::mem::align_of::<u8>();
        ptr = std::alloc::alloc(
            std::alloc::Layout::from_size_align(size, align)
            .map_err(|e| { Error::incorrect_layout(e) })?
        );
    }

    if ptr.is_null() {
        Err(Error::allocation_failure())
    } else {
        Ok(ptr)
    }
}


/// Deallocate byte buffer
pub fn dealloc_buf(ptr: *mut u8, size: usize) {
    let align = std::mem::align_of::<u8>();
    unsafe {
        std::alloc::dealloc(ptr as *mut u8, std::alloc::Layout::from_size_align(size, align).unwrap());
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn run_conversions() {
        let arr: [u8; 16] = if cfg!(target_endian = "big") {
            [0x10, 0xF, 0xE, 0xD, 0xC, 0xB, 0xA, 9, 8, 7, 6, 5, 4, 3, 2, 1]
        } else {
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x10]
        };

        let res = u8::slice_to_int(&arr[0..1]).unwrap();
        assert_eq!(res, 1, "expect 1, got {:x?}", res);

        let res = u16::slice_to_int(&arr[0..2]).unwrap();
        assert_eq!(res, 0x201, "expect 0x201, got {:x?}", res);

        let res = u32::slice_to_int(&arr[0..4]).unwrap();
        assert_eq!(res, 0x4030201, "expect 0x4030201, got {:x?}", res);

        let res = u64::slice_to_int(&arr[0..8]).unwrap();
        assert_eq!(res, 0x807060504030201, "expect 0x807060504030201, got {:x?}", res);

        let res = u128::slice_to_int(&arr[..]).unwrap();
        assert_eq!(res, 0x100F0E0D0C0B0A090807060504030201, "expect 0x100F0E0D0C0B0A0807060504030201, got {:x?}", res);

        let res = i8::slice_to_int(&arr[0..1]).unwrap();
        assert_eq!(res, 1, "expect 1, got {:x?}", res);

        let res = i16::slice_to_int(&arr[0..2]).unwrap();
        assert_eq!(res, 0x201, "expect 0x201, got {:x?}", res);

        let res = i32::slice_to_int(&arr[0..4]).unwrap();
        assert_eq!(res, 0x4030201, "expect 0x4030201, got {:x?}", res);

        let res = i64::slice_to_int(&arr[0..8]).unwrap();
        assert_eq!(res, 0x807060504030201, "expect 0x807060504030201, got {:x?}", res);

        let res = i128::slice_to_int(&arr[..]).unwrap();
        assert_eq!(res, 0x100F0E0D0C0B0A090807060504030201, "expect 0x100F0E0D0C0B0A0807060504030201, got {:x?}", res);
    }
}
