use async_trait::async_trait;
use linkme::distributed_slice;
use nes_buffer_runtime::BufferProvider;
use nes_source_runtime::{
    AsyncSource, Result, SOURCE_CREATION_FUNCTIONS, SourceCreateFn, SourceResult,
};
use nes_source_validation::{
    ConfigDefinition, ConfigOptions, ConfigOptionsTypeTag, SOURCE_VALIDATOR,
};
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

const SOURCE_NAME: &'static str = "FILE";
const FILE_PATH_OPTION: &'static str = "FILE_PATH";

#[distributed_slice(SOURCE_VALIDATOR)]
static FILE_SOURCE_VALIDATOR: (&'static str, &'static [ConfigDefinition]) = (
    SOURCE_NAME,
    &[ConfigDefinition::with_type(
        FILE_PATH_OPTION,
        ConfigOptionsTypeTag::Text,
    )],
);

#[distributed_slice(SOURCE_CREATION_FUNCTIONS)]
static FILE_SOURCE: (&'static str, &'static SourceCreateFn) = (SOURCE_NAME, &create_file_source);
struct FileSource {
    path: PathBuf,
    file: Option<tokio::fs::File>,
}

#[async_trait]
impl AsyncSource for FileSource {
    async fn start(&mut self) -> Result<()> {
        let _ = self.file.insert(
            tokio::fs::File::open(&self.path)
                .await
                .map_err(|e| e.to_string())?,
        );
        Ok(())
    }

    async fn receive(&mut self, provider: &mut BufferProvider) -> Result<SourceResult> {
        let mut buffer = provider.allocate().await;
        let result = self
            .file
            .as_mut()
            .unwrap()
            .read(buffer.get_data_mut())
            .await
            .map_err(|e| e.to_string())?;

        if result == 0 {
            Ok(SourceResult::EoS)
        } else {
            Ok(SourceResult::Data(buffer, result))
        }
    }

    async fn stop(&mut self) -> Result<()> {
        let _ = self.file.take();
        Ok(())
    }
}

impl From<&ConfigOptions> for FileSource {
    fn from(value: &ConfigOptions) -> Self {
        let path = value
            .get(FILE_PATH_OPTION)
            .expect("Missing required config option 'FILE_PATH'");
        FileSource {
            path: path.into(),
            file: None,
        }
    }
}

fn create_file_source(config_options: &ConfigOptions) -> Box<dyn AsyncSource + Send> {
    Box::new(FileSource::from(config_options))
}
