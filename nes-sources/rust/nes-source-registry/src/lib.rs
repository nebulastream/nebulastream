// Source and sink registry.
//
// Dispatches spawn/validate calls by name using the linkme distributed slices
// from nes_source_runtime::plugin::SOURCE_PLUGINS and nes_sink_runtime::plugin::SINK_PLUGINS.
// Plugin crates register themselves at link time — no manual registry editing needed.

use std::collections::HashMap;
use std::ffi::c_void;

use nes_source_runtime::config::ConfigParam;
use nes_source_runtime::plugin::SOURCE_PLUGINS;
use nes_sink_runtime::plugin::SINK_PLUGINS;

// ===========================================================================
// Source registry
// ===========================================================================

pub fn get_source_names() -> Vec<String> {
    SOURCE_PLUGINS.iter().map(|e| e.name.to_string()).collect()
}

pub fn spawn_source(
    name: &str,
    config: &HashMap<String, String>,
    source_id: u64,
    buffer_provider: nes_source_runtime::BufferProviderHandle,
    inflight_limit: u32,
    emit_fn: nes_source_runtime::bridge::EmitFnPtr,
    emit_ctx: *mut c_void,
    error_fn: nes_source_runtime::source::ErrorFnPtr,
    error_ctx: *mut c_void,
) -> Result<Box<nes_source_runtime::handle::SourceTaskHandle>, String> {
    let entry = SOURCE_PLUGINS.iter()
        .find(|e| e.name == name)
        .ok_or_else(|| format!("unknown tokio source type: {name}"))?;
    (entry.spawn)(config, source_id, buffer_provider, inflight_limit, emit_fn, emit_ctx, error_fn, error_ctx)
}

pub fn validate_source(
    name: &str,
    config: &HashMap<String, String>,
) -> Result<(Vec<String>, Vec<String>, Vec<String>), String> {
    let entry = SOURCE_PLUGINS.iter()
        .find(|e| e.name == name)
        .ok_or_else(|| format!("unknown tokio source type: {name}"))?;
    let validated = nes_source_runtime::config::validate_config(entry.schema, config)?;
    Ok(encode_validated(entry.schema, &validated))
}

// ===========================================================================
// Sink registry
// ===========================================================================

pub fn get_sink_names() -> Vec<String> {
    SINK_PLUGINS.iter().map(|e| e.name.to_string()).collect()
}

pub fn spawn_sink(
    name: &str,
    config: &HashMap<String, String>,
    sink_id: u64,
    channel_capacity: usize,
    error_fn: nes_sink_runtime::sink::ErrorFnPtr,
    error_ctx: *mut c_void,
) -> Result<Box<nes_sink_runtime::SinkTaskHandle>, String> {
    let entry = SINK_PLUGINS.iter()
        .find(|e| e.name == name)
        .ok_or_else(|| format!("unknown tokio sink type: {name}"))?;
    (entry.spawn)(config, sink_id, channel_capacity, error_fn, error_ctx)
}

pub fn validate_sink(
    name: &str,
    config: &HashMap<String, String>,
) -> Result<(Vec<String>, Vec<String>, Vec<String>), String> {
    let entry = SINK_PLUGINS.iter()
        .find(|e| e.name == name)
        .ok_or_else(|| format!("unknown tokio sink type: {name}"))?;
    let validated = nes_source_runtime::config::validate_config(entry.schema, config)?;
    Ok(encode_validated(entry.schema, &validated))
}

// ===========================================================================
// Shared helpers
// ===========================================================================

fn encode_validated(
    schema: &[ConfigParam],
    validated: &HashMap<String, String>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut types = Vec::new();
    for param in schema {
        if let Some(value) = validated.get(param.name) {
            keys.push(param.name.to_string());
            values.push(value.clone());
            types.push(param.param_type.as_str().to_string());
        }
    }
    (keys, values, types)
}
