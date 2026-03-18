use std::collections::HashMap;
use std::path::PathBuf;

use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use nes_sink_runtime::schema::{SchemaField, format_schema_header};
use nes_sink_runtime::plugin::{SinkPluginEntry, SINK_PLUGINS};
use nes_sink_runtime::{AsyncSink, ConfigParam, ParamType, SinkContext, SinkError, SinkMessage};

pub const CONFIG_SCHEMA: &[ConfigParam] = &[
    ConfigParam {
        name: "file_path",
        param_type: ParamType::String,
        default: None,
    },
];

pub fn create_file_sink(config: &HashMap<String, String>) -> Result<AsyncFileSink, String> {
    let file_path = config
        .get("file_path")
        .ok_or("missing required config key: file_path")?;
    let schema = decode_schema_from_config(config);
    Ok(AsyncFileSink::new(PathBuf::from(file_path), schema))
}

fn decode_schema_from_config(config: &HashMap<String, String>) -> Vec<SchemaField> {
    let Some(raw) = config.get("__schema_fields") else {
        return Vec::new();
    };
    if raw.is_empty() {
        return Vec::new();
    }
    raw.split(',')
        .filter_map(|entry| {
            let parts: Vec<&str> = entry.splitn(3, ':').collect();
            if parts.len() == 3 {
                let nullable = parts[2] == "IS_NULLABLE" || parts[2] == "true";
                Some(SchemaField::new(
                    parts[0].to_string(),
                    parts[1].to_string(),
                    nullable,
                ))
            } else {
                None
            }
        })
        .collect()
}

pub struct AsyncFileSink {
    path: PathBuf,
    schema: Vec<SchemaField>,
}

impl AsyncFileSink {
    pub fn new(path: PathBuf, schema: Vec<SchemaField>) -> Self {
        Self { path, schema }
    }
}

fn spawn_file_sink(
    config: &HashMap<String, String>,
    sink_id: u64,
    channel_capacity: usize,
    error_fn: nes_sink_runtime::sink::ErrorFnPtr,
    error_ctx: *mut std::ffi::c_void,
) -> Result<Box<nes_sink_runtime::SinkTaskHandle>, String> {
    let sink = create_file_sink(config)?;
    Ok(nes_sink_runtime::sink::spawn_sink(
        sink_id, sink, channel_capacity, error_fn, error_ctx,
    ))
}

#[used]
#[linkme::distributed_slice(SINK_PLUGINS)]
static FILE_SINK_PLUGIN: SinkPluginEntry = SinkPluginEntry {
    name: "TokioFileSink",
    schema: CONFIG_SCHEMA,
    spawn: spawn_file_sink,
};

impl AsyncSink for AsyncFileSink {
    async fn run(&mut self, ctx: &SinkContext) -> Result<(), SinkError> {
        let mut file = File::create(&self.path)
            .await
            .map_err(|e| SinkError::new(format!("failed to open file: {}", e)))?;
        if !self.schema.is_empty() {
            let header = format_schema_header(&self.schema);
            file.write_all(header.as_bytes())
                .await
                .map_err(|e| SinkError::new(format!("failed to write schema header: {}", e)))?;
        }
        loop {
            match ctx.recv().await {
                SinkMessage::Data(mut iter) => {
                    while let Some(segment) = iter.next_segment() {
                        file.write_all(segment)
                            .await
                            .map_err(|e| SinkError::new(format!("write failed: {}", e)))?;
                    }
                }
                SinkMessage::Flush => {
                    file.flush()
                        .await
                        .map_err(|e| SinkError::new(format!("flush failed: {}", e)))?;
                }
                SinkMessage::Close => {
                    file.flush()
                        .await
                        .map_err(|e| SinkError::new(format!("final flush failed: {}", e)))?;
                    return Ok(());
                }
            }
        }
    }
}
