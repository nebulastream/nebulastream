const SINK_NAME: &'static str = "VOID";
const INGESTION_OPTION: &'static str = "INGESTION";
const HEADER_OPTION: &'static str = "HEADER";
const OUTPUT_FORMAT_OPTION: &'static str = "OUTPUT_FORMAT";

#[cfg(feature = "validation")]
mod validation {
    use super::*;
    use linkme::distributed_slice;
    use nes_sink_validation::{
        ConfigDefinition, ConfigOptionsType, ConfigValue, Error, SINK_VALIDATOR,
    };

    fn validate_ingestion(value: &ConfigValue) -> core::result::Result<(), Error> {
        let ConfigValue::Text(ingestion) = value else {
            return Err("Expected a string".into());
        };
        duration_str::parse(ingestion)?;
        Ok(())
    }

    #[distributed_slice(SINK_VALIDATOR)]
    static VOID_SINK_VALIDATOR: (&'static str, &'static [ConfigDefinition]) = (
        SINK_NAME,
        &[
            ConfigDefinition::with_default_and_check(
                INGESTION_OPTION,
                ConfigOptionsType::Text("0s"),
                &validate_ingestion,
            ),
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
    use nes_buffer_runtime::TupleBuffer;
    use nes_sink_runtime::{AsyncSink, Result, SINK_CREATION_FUNCTIONS, SinkCreateFn};
    use nes_sink_validation::ConfigOptions;

    #[allow(dead_code)]
    struct VoidSink {
        last_write: std::time::Instant,
        ingestion: std::time::Duration,
        header: String,
    }

    #[async_trait]
    impl AsyncSink for VoidSink {
        async fn start(&mut self) -> Result<()> {
            self.last_write = std::time::Instant::now();
            Ok(())
        }

        async fn execute(&mut self, _buffer: TupleBuffer) -> Result<()> {
            let now = std::time::Instant::now();
            if now.duration_since(self.last_write) < self.ingestion {
                tokio::time::sleep_until((self.last_write + self.ingestion).into()).await;
            }
            self.last_write = now;
            Ok(())
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        async fn stop(&mut self) -> Result<()> {
            //noop
            Ok(())
        }
    }

    impl From<&ConfigOptions> for VoidSink {
        fn from(value: &ConfigOptions) -> Self {
            let ingestion = value
                .get(INGESTION_OPTION)
                .map(|v| duration_str::parse(v).expect("Should have been validated"));

            let header = value.get(HEADER_OPTION).cloned().unwrap_or_default();
            VoidSink {
                header,
                ingestion: ingestion.unwrap_or_default(),
                last_write: std::time::Instant::now(),
            }
        }
    }

    fn create_void_sink(config_options: &ConfigOptions) -> Box<dyn AsyncSink + Send> {
        Box::new(VoidSink::from(config_options))
    }

    #[distributed_slice(SINK_CREATION_FUNCTIONS)]
    static VOID_SINK: (&'static str, &'static SinkCreateFn) = (SINK_NAME, &create_void_sink);
}
