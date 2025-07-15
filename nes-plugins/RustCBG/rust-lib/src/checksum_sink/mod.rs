use std::ffi::{c_char, CStr};

use crate::{
    checksum_sink::implementation::{Error, RustChecksumSinkImpl},
    IO_ERROR, NULL_PTR, OK,
};

mod implementation;

type ErrCode = i32;
pub const CHECKSUM_SINK_ALREADY_OPEN: ErrCode = -20;
pub const STOP_WITHOUT_STARTED: ErrCode = -21;

#[unsafe(no_mangle)]
pub extern "C" fn new_rust_checksum_sink(path: *const c_char) -> *mut RustChecksumSinkImpl {
    if path.is_null() {
        panic!("Null ptr received for file path data.");
    }
    // SAFETY: C++ side must supply a valid length and pointer
    let path = unsafe { CStr::from_ptr(path) }
        .to_string_lossy()
        .to_string();
    Box::into_raw(Box::new(RustChecksumSinkImpl::new(path)))
}

#[unsafe(no_mangle)]
pub extern "C" fn start_rust_checksum_sink(rcs: *mut RustChecksumSinkImpl) -> ErrCode {
    let rcs = if let Some(rcs) = unsafe { rcs.as_mut() } {
        rcs
    } else {
        return NULL_PTR;
    };
    match rcs.start() {
        Ok(()) => OK,
        Err(e) => error_code(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn execute_rust_checksum_sink(
    rcs: *mut RustChecksumSinkImpl,
    path: *const c_char,
) -> ErrCode {
    let rcs = if let Some(rcs) = unsafe { rcs.as_mut() } {
        rcs
    } else {
        return NULL_PTR;
    };
    let path = unsafe { CStr::from_ptr(path) };
    rcs.execute(
        path.to_str()
            .expect("execute_rust_checksum_sink received invalid UTF-8 from outside."),
    );
    OK
}

#[unsafe(no_mangle)]
pub extern "C" fn stop_rust_checksum_sink(rcs: *mut RustChecksumSinkImpl) -> ErrCode {
    let rcs = if let Some(rcs) = unsafe { rcs.as_mut() } {
        rcs
    } else {
        return NULL_PTR;
    };
    match rcs.stop() {
        Ok(()) => OK,
        Err(e) => error_code(e),
    }
}

fn error_code(e: Error) -> ErrCode {
    match e {
        Error::AlreadyOpen => CHECKSUM_SINK_ALREADY_OPEN,
        Error::StopWithoutStarted => STOP_WITHOUT_STARTED,
        Error::IoError => IO_ERROR,
    }
}
