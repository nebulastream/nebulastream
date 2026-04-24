const SINK_NAME: &'static str = "FILE";
const FILE_PATH_OPTION: &'static str = "FILE_PATH";
const HEADER_OPTION: &'static str = "HEADER";
const OUTPUT_FORMAT_OPTION: &'static str = "OUTPUT_FORMAT";

#[cfg(feature = "validation")]
mod validation {
    use super::*;
    use linkme::distributed_slice;
    use nes_sink_validation::{
        ConfigDefinition, ConfigOptionsType, ConfigOptionsTypeTag, SINK_VALIDATOR,
    };

    #[distributed_slice(SINK_VALIDATOR)]
    static FILE_SINK_VALIDATOR: (&'static str, &'static [ConfigDefinition]) = (
        SINK_NAME,
        &[
            ConfigDefinition::with_type(FILE_PATH_OPTION, ConfigOptionsTypeTag::Text),
            ConfigDefinition::with_default(OUTPUT_FORMAT_OPTION, ConfigOptionsType::Text("CSV")),
            ConfigDefinition::with_default(HEADER_OPTION, ConfigOptionsType::Text("")),
        ],
    );
}

#[cfg(feature = "runtime")]
mod runtime {
    use super::*;
    use async_trait::async_trait;
    use linkme::distributed_slice;
    use nes_buffer_runtime::{ChildBufferIndex, TupleBuffer};
    use nes_sink_runtime::{AsyncSink, Result, SINK_CREATION_FUNCTIONS, SinkCreateFn};
    use nes_sink_validation::ConfigOptions;
    use std::path::PathBuf;
    use tokio::io::AsyncWriteExt;
    use tracing::info;

    struct FileSink {
        path: PathBuf,
        header: String,
        file: Option<tokio::fs::File>,
    }

    #[async_trait]
    impl AsyncSink for FileSink {
        async fn start(&mut self) -> Result<()> {
            info!("Creating new file: {:?}", self.path);
            let file = self.file.insert(
                tokio::fs::File::create(&self.path)
                    .await
                    .map_err(|e| e.to_string())?,
            );
            if !self.header.is_empty() {
                file.write_all(self.header.as_bytes())
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        }

        async fn execute(&mut self, buffer: TupleBuffer) -> Result<()> {
            let file = self.file.as_mut().unwrap();
            file.write_all(&buffer.get_data()[0..buffer.get_number_of_tuples()])
                .await
                .map_err(|e| e.to_string())?;
            for index in 0..buffer.num_children() {
                let child = buffer.load_child(ChildBufferIndex(index));
                file.write_all(&child.get_data()[0..child.get_number_of_tuples()])
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        }

        async fn stop(&mut self) -> Result<()> {
            //noop
            Ok(())
        }
    }

    impl From<&ConfigOptions> for FileSink {
        fn from(value: &ConfigOptions) -> Self {
            let path = value
                .get(FILE_PATH_OPTION)
                .expect("Missing required config option 'FILE_PATH'");
            let header = value.get(HEADER_OPTION).cloned().unwrap_or_default();
            FileSink {
                path: path.into(),
                header,
                file: None,
            }
        }
    }

    fn create_file_sink(config_options: &ConfigOptions) -> Box<dyn AsyncSink + Send> {
        Box::new(FileSink::from(config_options))
    }

    #[distributed_slice(SINK_CREATION_FUNCTIONS)]
    static FILE_SINK: (&'static str, &'static SinkCreateFn) = (SINK_NAME, &create_file_sink);
}
