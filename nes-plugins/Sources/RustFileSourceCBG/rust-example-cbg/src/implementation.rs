use std::fs::File;
use std::io;
use io::Read;
use std::path::Path;
use crate::implementation::Error::IoError;

pub struct RustFileSourceImpl {
    path: String,
    file: Option<File>,
    num_bytes_read: usize
}

impl RustFileSourceImpl {
    pub fn new(path: String) -> Self {
        Self {
            path,
            file: None,
            num_bytes_read: 0,
        }
    }

    pub fn open(&mut self) -> Result<(), Error> {
        if self.file.is_some() {
            return Err(Error::AlreadyOpen);
        }
        self.file = Some(File::open(Path::new(&self.path))?);
        Ok(())
    }

    pub fn close(&mut self) {
        self.file = None;
    }

    pub fn fill_tuple_buffer(&mut self, tuple_buffer: &mut [u8]) -> Result<u64, Error> {
        let file = if let Some(ref mut f) = self.file {
            f
        } else {
            return Err(Error::NotOpen);
        };
        let mut read_bytes: usize = 0;
        while read_bytes < tuple_buffer.len() {
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

pub enum Error {
    AlreadyOpen,
    NotOpen,
    NullPtr,
    IoError,
}

impl From<io::Error> for Error {
    fn from(_: io::Error) -> Self {
        IoError
    }
}
