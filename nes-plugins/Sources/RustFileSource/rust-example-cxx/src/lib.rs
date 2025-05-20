use std::fs::File;
use std::path::Path;
use anyhow::anyhow;
use std::slice;
use std::io::Read;

#[cxx::bridge]
mod ffi {
    #[namespace = "NES::Rust"]
    extern "Rust" {
        type RustFileSourceImpl;
        // SAFETY: The C++ side (class RustFileSource) doesn't free the path String
        //         as long as the RustFileSourceImpl lives
        unsafe fn new_rust_file_source<'a>(path: &'a str) -> Box<RustFileSourceImpl>;
        fn open(&mut self) -> Result<()>;
        fn close(&mut self) -> Result<()>;
        unsafe fn fill_tuple_buffer(&mut self, tuple_buffer: *mut u8, buf_len: u64) -> Result<u64>;
        fn to_string(&self) -> String;
        fn free_string(s: String);
        fn free_rust_file_source(_: Box<RustFileSourceImpl>);
    }
}

pub struct RustFileSourceImpl {
    path: String,
    file: Option<File>,
    num_bytes_read: usize
}

impl RustFileSourceImpl {
    pub fn open(&mut self) -> anyhow::Result<()> {
        if self.file.is_some() {
            return Err(anyhow!("RustFileSourceImpl already open."));
        }
        self.file = Some(File::open(Path::new(&self.path))?);
        Ok(())
    }

    pub fn close(&mut self) -> anyhow::Result<()> {
        if self.file.is_none() {
            return Err(anyhow!("RustFileSourceImpl wasn't open."));
        }
        self.file = None;
        Ok(())
    }

    pub fn fill_tuple_buffer(&mut self, tuple_buffer: *mut u8, buf_len: u64) -> anyhow::Result<u64> {
        let file = if let Some(ref mut f) = self.file {
            f
        } else {
            return Err(anyhow!(
                "fill_tuple_buffer() was called on RustFileSource, that was not open."
            ));
        };
        if tuple_buffer.is_null() {
            return Err(anyhow!("fill_tuple_buffer received null ptr."));
        }
        let tuple_buffer: &mut [u8] = unsafe {
            // TODO what if buf_len overflows usize?
            slice::from_raw_parts_mut(tuple_buffer, buf_len as usize)
        };

        let mut read_bytes: usize = 0;
        while (read_bytes as u64) < buf_len {
            let just_read = file.read(&mut tuple_buffer[read_bytes..])?;
            if just_read == 0 { break; }
            read_bytes += just_read;
        }
        Ok(read_bytes as u64)
    }

    pub fn to_string(&self) -> String {
        format!("RustFileSourceImpl(filepath: {}, totalNumBytesRead: {})", self.path, self.num_bytes_read)
    }
}

pub fn new_rust_file_source(path: &str) -> Box<RustFileSourceImpl> {
    Box::new(RustFileSourceImpl {
        path: path.to_string(),
        file: None,
        num_bytes_read: 0
    })
}

pub fn free_rust_file_source(_: Box<RustFileSourceImpl>) {
    // Gets freed automatically when going out of scope
}

pub fn free_string(_: String) {
    // Gets freed automatically when going out of scope
}