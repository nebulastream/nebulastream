use std::ffi::{c_char, CStr};

use implementation::RustFileSinkImpl;

use crate::{file_sink::implementation::Error, ErrCode, NULL_PTR, OK};

mod implementation;

#[unsafe(no_mangle)]
pub extern "C" fn new_rust_file_sink(path: *const c_char, append: bool) -> *mut RustFileSinkImpl {
    if path.is_null() {
        panic!("Null ptr received for file path data.");
    }
    // SAFETY: C++ side must supply a valid length and pointer
    let path = unsafe { CStr::from_ptr(path) }
        .to_str()
        .expect("Got invalid UTF-8 from C++.");

    Box::into_raw(Box::new(RustFileSinkImpl::new(path, append)))
}

#[unsafe(no_mangle)]
pub extern "C" fn start_rust_file_source_cbg(
    rfs: *mut RustFileSinkImpl,
    formatted_schema: *const c_char,
) -> ErrCode {
    let rfs = if let Some(rfs) = unsafe { rfs.as_mut() } {
        rfs
    } else {
        return NULL_PTR;
    };
    if formatted_schema.is_null() {
        panic!("Null ptr received for file path data.");
    }
    // SAFETY: C++ side must supply a valid length and pointer
    let formatted_schema = unsafe { CStr::from_ptr(formatted_schema) }
        .to_str()
        .expect("Got invalid UTF-8 from C++.");

    match rfs.start(formatted_schema) {
        Ok(()) => OK,
        Err(e) => errcode(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn execute_rust_file_source_cbg(
    rfs: *mut RustFileSinkImpl,
    tuple_buffer: *const c_char,
) -> ErrCode {
    let rfs = if let Some(rfs) = unsafe { rfs.as_mut() } {
        rfs
    } else {
        return NULL_PTR;
    };
    if tuple_buffer.is_null() {
        panic!("Null ptr received for file path data.");
    }
    // SAFETY: C++ side must supply a valid length and pointer
    let tuple_buffer = unsafe { CStr::from_ptr(tuple_buffer) }
        .to_str()
        .expect("Got invalid UTF-8 from C++.");

    match rfs.execute(tuple_buffer) {
        Ok(()) => OK,
        Err(e) => errcode(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn stop_rust_file_source_cbg(rfs: *mut RustFileSinkImpl) -> ErrCode {
    let rfs = if let Some(rfs) = unsafe { rfs.as_mut() } {
        rfs
    } else {
        return NULL_PTR;
    };
    rfs.stop();
    OK
}

fn errcode(error: Error) -> ErrCode {
    match error {
        Error::ExecuteWithoutStart => 30,
        Error::Io(_) => 31,
    }
}
