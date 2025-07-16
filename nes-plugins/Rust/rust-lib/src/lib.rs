// re-export the names, so CXX finds them in this parent module of the ffi module
pub use checksum_sink::{free_checksum_sink, new_checksum_sink, ChecksumSink as ChecksumSinkImpl};
pub use file_sink::{new_file_sink, FileSink as FileSinkImpl};
pub use file_source::{
    free_file_source, free_string, new_file_source, FileSource as FileSourceImpl,
};
mod checksum_sink;
mod file_sink;
mod file_source;

#[cxx::bridge]
mod ffi {
    #[namespace = "NES::Rust"]
    extern "Rust" {
        type FileSourceImpl;
        // SAFETY: The C++ side (class RustFileSource) doesn't free the path String
        //         as long as the RustFileSourceImpl lives
        unsafe fn new_file_source(path: &str) -> Box<FileSourceImpl>;
        fn open(&mut self) -> Result<()>;
        fn close(&mut self) -> Result<()>;
        unsafe fn fill_tuple_buffer(&mut self, tuple_buffer: *mut u8, buf_len: u64) -> Result<u64>;
        fn to_string(&self) -> String;
        fn free_string(s: String);
        fn free_file_source(_: Box<FileSourceImpl>);
    }

    #[namespace = "NES::Rust"]
    extern "Rust" {
        type FileSinkImpl;
        unsafe fn new_file_sink(path: &str, append: bool) -> Box<FileSinkImpl>;
        fn start(&mut self, formatted_schema: &str) -> Result<()>;
        unsafe fn execute(&mut self, tuple_buffer: &str) -> Result<()>;
        fn stop(&mut self);
    }

    #[namespace = "NES::Rust"]
    extern "Rust" {
        type ChecksumSinkImpl;
        // SAFETY: The C++ side (class RustChecksumSink) doesn't free the path String
        //         as long as the RustChecksumImpl lives
        unsafe fn new_checksum_sink(path: &str) -> Box<ChecksumSinkImpl>;
        fn start(&mut self) -> Result<()>;
        fn execute(&mut self, formatted: &str);
        fn stop(&mut self) -> Result<()>;
        fn free_checksum_sink(_: Box<ChecksumSinkImpl>);
    }
}
