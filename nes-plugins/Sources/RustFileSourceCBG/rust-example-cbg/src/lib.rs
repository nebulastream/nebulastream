mod implementation;

use std::slice;
use std::ffi::{c_char, CStr};
use implementation::RustFileSourceImpl;
use crate::implementation::Error;

type ErrCode = i32;

pub const ALRIGHT: ErrCode = 0;
pub const ALREADY_OPEN: ErrCode = -10;
pub const NOT_OPEN: ErrCode = -11;
pub const NULL_PTR: ErrCode = -12;
pub const IO_ERROR: ErrCode = -13;
pub const FILE_NOT_FOUND: ErrCode = -14;

#[unsafe(no_mangle)]
pub extern "C" fn new_rust_file_source(path: *const c_char) -> *mut RustFileSourceImpl {
    if path.is_null() {
        panic!("Null ptr received for file path data.");
    }
    // SAFETY: C++ side must supply a valid length and pointer
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy().to_string();
    Box::into_raw(Box::new(RustFileSourceImpl::new(path)))
}

#[unsafe(no_mangle)]
pub extern "C" fn openn(rfs: *mut RustFileSourceImpl) -> ErrCode {
    let rfs = if let Some(rfs) = unsafe { rfs.as_mut() } {
        rfs
    } else {
        return NULL_PTR;
    };
    match rfs.open() {
        Ok(()) => ALRIGHT,
        Err(e) => error_code(e)
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn closee(rfs: *mut RustFileSourceImpl) {
    if let Some(rfs) = unsafe { rfs.as_mut() } {
        rfs.close();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fill_tuple_bufferr(rfs: *mut RustFileSourceImpl, tuple_buffer: *mut u8, buf_len: u64) -> i64 {
    if tuple_buffer.is_null() {
        return NULL_PTR as i64;
    }
    let tuple_buffer: &mut [u8] = unsafe {
        // TODO what if buf_len overflows usize?
        slice::from_raw_parts_mut(tuple_buffer, buf_len as usize)
    };
    let rfs = if let Some(rfs) = unsafe { rfs.as_mut() } {
        rfs
    } else {
        return NULL_PTR as i64;
    };
    match rfs.fill_tuple_buffer(tuple_buffer) {
        Ok(result) => result as i64,
        Err(e) => error_code(e) as i64
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn free_rust_file_source(rfs: *mut RustFileSourceImpl) {
    if !rfs.is_null() {
        drop(
            // SAFETY: RustFileSourceImpl can only be constructed with new_rust_file_source()
            // and can therefore only live on the heap.
            unsafe { Box::from_raw(rfs) }
        );
    }
}

fn error_code(e: Error) -> ErrCode {
    match e {
        Error::AlreadyOpen => ALREADY_OPEN,
        Error::NotOpen => NOT_OPEN,
        Error::NullPtr => NULL_PTR,
        Error::IoError => IO_ERROR,
    }
}
