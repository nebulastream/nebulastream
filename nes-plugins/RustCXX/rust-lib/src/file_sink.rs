use std::{fs::File, io::Write};

pub struct FileSink {
    file: Option<File>,
    path: String,
    append: bool,
}

pub fn new_file_sink(path: &str, append: bool) -> Box<FileSink> {
    Box::new(FileSink::new(path, append))
}

impl FileSink {
    fn new(path: &str, append: bool) -> Self {
        Self {
            file: None,
            path: path.to_string(),
            append,
        }
    }

    pub fn start(&mut self, formatted_schema: &str) -> anyhow::Result<()> {
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

    pub fn execute(&mut self, tuple_buffer: &str) -> anyhow::Result<()> {
        if let Some(ref mut file) = self.file {
            file.write_all(tuple_buffer.as_bytes())?;
            file.flush()?;
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Cannot execute() RustFileSink, that was not start()ed yet."
            ))
        }
    }

    pub fn stop(&mut self) {
        self.file = None;
    }
}
