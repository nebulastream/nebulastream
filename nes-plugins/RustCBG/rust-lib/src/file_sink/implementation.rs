use std::{
    fs::File,
    io::{self, Write},
};

pub struct RustFileSinkImpl {
    file: Option<File>,
    path: String,
    append: bool,
}

impl RustFileSinkImpl {
    pub fn new(path: &str, append: bool) -> Self {
        Self {
            file: None,
            path: path.to_string(),
            append,
        }
    }

    pub fn start(&mut self, formatted_schema: &str) -> Result<(), Error> {
        if self.file.is_none() {
            let mut file_options = File::options();
            file_options.create(true);
            if self.append {
                file_options.append(true);
            } else {
                file_options.write(true);
            }
            let mut file = file_options.open(&self.path)?;
            if file.metadata()?.len() == 0 {
                file.write_all(formatted_schema.as_bytes())?;
            }
            self.file = Some(file);
        }
        Ok(())
    }

    pub fn execute(&mut self, tuple_buffer: &str) -> Result<(), Error> {
        if let Some(ref mut file) = self.file {
            file.write_all(tuple_buffer.as_bytes())?;
            file.flush()?;
            Ok(())
        } else {
            Err(Error::ExecuteWithoutStart)
        }
    }

    pub fn stop(&mut self) {
        self.file = None;
    }
}

pub enum Error {
    ExecuteWithoutStart,
    Io(std::io::Error),
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
