use async_trait::async_trait;
use linkme::distributed_slice;
use nes_buffer_runtime::{ChildBufferIndex, TupleBuffer};
use nes_sink_runtime::{AsyncSink, Result, SINK_CREATION_FUNCTIONS, SinkCreateFn};
use nes_sink_validation::{
    ConfigDefinition, ConfigOptions, ConfigOptionsType, ConfigOptionsTypeTag, ConfigValue, Error,
    SINK_VALIDATOR,
};

const SINK_NAME: &'static str = "PRINT";
const INGESTION_OPTION: &'static str = "INGESTION";
const HEADER_OPTION: &'static str = "HEADER";
const OUTPUT_FORMAT_OPTION: &'static str = "OUTPUT_FORMAT";

fn validate_ingestion(value: &ConfigValue) -> core::result::Result<(), Error> {
    let ConfigValue::Text(ingestion) = value else {
        return Err("Expected a string".into());
    };
    duration_str::parse(ingestion)?;
    Ok(())
}
#[distributed_slice(SINK_VALIDATOR)]
static PRINT_SINK_VALIDATOR: (&'static str, &'static [ConfigDefinition]) = (
    SINK_NAME,
    &[
        ConfigDefinition::with_default_and_validator(
            INGESTION_OPTION,
            ConfigOptionsType::Text("0s"),
            &validate_ingestion,
        ),
        ConfigDefinition::with_default(OUTPUT_FORMAT_OPTION, ConfigOptionsType::Text("CSV")),
        ConfigDefinition::with_default(HEADER_OPTION, ConfigOptionsType::Text("")),
    ],
);

#[distributed_slice(SINK_CREATION_FUNCTIONS)]
static PRINT_SINK: (&'static str, &'static SinkCreateFn) = (SINK_NAME, &create_print_sink);
struct PrintSink {
    last_write: std::time::Instant,
    ingestion: std::time::Duration,
    header: String,
}

#[async_trait]
impl AsyncSink for PrintSink {
    async fn start(&mut self) -> Result<()> {
        if !self.header.is_empty() {
            println!("{}", self.header);
        }
        self.last_write = std::time::Instant::now();
        Ok(())
    }

    async fn execute(&mut self, buffer: TupleBuffer) -> Result<()> {
        let now = std::time::Instant::now();
        if now.duration_since(self.last_write) < self.ingestion {
            tokio::time::sleep_until((self.last_write + self.ingestion).into()).await;
        }
        self.last_write = now;

        println!(
            "{}",
            str::from_utf8(&buffer.get_data()[..buffer.get_number_of_tuples()])
                .map_err(|_| "Invalid UTF-8")?
        );
        for index in 0..buffer.num_children() {
            let child = buffer.load_child(ChildBufferIndex(index));
            println!(
                "{}",
                str::from_utf8(&child.get_data()[..child.get_number_of_tuples()])
                    .map_err(|_| "Invalid UTF-8")?
            );
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        //noop
        Ok(())
    }
}

impl From<&ConfigOptions> for PrintSink {
    fn from(value: &ConfigOptions) -> Self {
        let ingestion = value
            .get(INGESTION_OPTION)
            .map(|v| duration_str::parse(v).expect("Should have been validated"));

        let header = value.get(HEADER_OPTION).cloned().unwrap_or_default();
        PrintSink {
            header,
            ingestion: ingestion.unwrap_or_default(),
            last_write: std::time::Instant::now(),
        }
    }
}

fn create_print_sink(config_options: &ConfigOptions) -> Box<dyn AsyncSink + Send> {
    Box::new(PrintSink::from(config_options))
}
