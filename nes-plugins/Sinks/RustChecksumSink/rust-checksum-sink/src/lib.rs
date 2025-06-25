use anyhow::anyhow;
use std::{
    fs::File,
    io::Write,
    sync::atomic::{AtomicUsize, Ordering},
};

#[cxx::bridge]
mod ffi {
    #[namespace = "NES::Rust"]
    extern "Rust" {
        type ChecksumSink;
        // SAFETY: The C++ side (class RustChecksumSink) doesn't free the path String
        //         as long as the RustChecksumImpl lives
        unsafe fn new_rust_checksum_sink(path: &str) -> Box<ChecksumSink>;
        fn start(&mut self) -> Result<()>;
        fn execute(&mut self, formatted: &str);
        fn stop(&mut self) -> Result<()>;
        fn free_rust_checksum_sink(_: Box<ChecksumSink>);
    }
}

pub struct ChecksumSink {
    path: String,
    output_file: Option<File>,
    checksum: AtomicUsize,
    tuple_count: AtomicUsize,
}

pub fn new_rust_checksum_sink(path: &str) -> Box<ChecksumSink> {
    Box::new(ChecksumSink {
        path: path.to_string(),
        output_file: None,
        checksum: AtomicUsize::new(0),
        tuple_count: AtomicUsize::new(0),
    })
}

impl ChecksumSink {
    pub fn start(&mut self) -> anyhow::Result<()> {
        if self.output_file.is_some() {
            return Err(anyhow!("RustFileSourceImpl already open."));
        }
        self.output_file = Some(File::open(self.path.as_str())?);
        Ok(())
    }

    pub fn execute(&mut self, formatted: &str) {
        let checksum = formatted.chars().map(|c| c as usize).sum();
        let tupel_count = formatted.matches('\n').count();
        self.checksum.fetch_add(checksum, Ordering::Relaxed);
        self.tuple_count.fetch_add(tupel_count, Ordering::Relaxed);
    }

    pub fn stop(&mut self) -> anyhow::Result<()> {
        let file = if let Some(ref mut f) = self.output_file {
            f
        } else {
            return Err(anyhow!(
                "stop() was called on RustChecksumSink, that has not been started."
            ));
        };
        writeln!(file, "S$Count:UINT64,S$Checksum:UINT64")?;
        writeln!(
            file,
            "{},{}",
            self.tuple_count.load(Ordering::Relaxed),
            self.checksum.load(Ordering::Relaxed)
        )?;
        self.output_file = None;
        Ok(())
    }
}

pub fn free_rust_checksum_sink(_: Box<ChecksumSink>) {
    // Gets freed automatically here
}
