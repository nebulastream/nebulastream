use std::{
    fs::File,
    io,
    io::Write,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct RustChecksumSinkImpl {
    path: String,
    output_file: Option<File>,
    checksum: AtomicUsize,
    tuple_count: AtomicUsize,
}

impl RustChecksumSinkImpl {
    pub fn new(path: String) -> RustChecksumSinkImpl {
        Self {
            path,
            output_file: None,
            checksum: AtomicUsize::new(0),
            tuple_count: AtomicUsize::new(0),
        }
    }

    pub fn start(&mut self) -> Result<(), Error> {
        if self.output_file.is_some() {
            return Err(Error::AlreadyOpen);
        }
        self.output_file = Some(File::create(self.path.as_str())?);
        Ok(())
    }

    pub fn execute(&mut self, formatted: &str) {
        let checksum = formatted.chars().map(|c| c as usize).sum();
        let tupel_count = formatted.matches('\n').count();
        self.checksum.fetch_add(checksum, Ordering::Relaxed);
        self.tuple_count.fetch_add(tupel_count, Ordering::Relaxed);
    }

    pub fn stop(&mut self) -> Result<(), Error> {
        let file = if let Some(ref mut f) = self.output_file {
            f
        } else {
            return Err(Error::StopWithoutStarted);
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

pub enum Error {
    AlreadyOpen,
    IoError,
    StopWithoutStarted,
}

impl From<io::Error> for Error {
    fn from(_: io::Error) -> Self {
        Self::IoError
    }
}
