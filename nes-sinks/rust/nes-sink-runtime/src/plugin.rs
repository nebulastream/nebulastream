// Plugin registration for AsyncSink implementations.
//
// Plugin crates use `#[linkme::distributed_slice(SINK_PLUGINS)]` to register
// themselves at link time. The registry discovers all entries without compile-time
// knowledge of plugin crates.

use std::collections::HashMap;
use std::ffi::c_void;

use nes_source_runtime::config::ConfigParam;

use crate::handle::SinkTaskHandle;
use crate::sink::ErrorFnPtr;

/// Type-erased spawn function for a sink plugin.
pub type SinkSpawnFn = fn(
    config: &HashMap<String, String>,
    sink_id: u64,
    channel_capacity: usize,
    error_fn: ErrorFnPtr,
    error_ctx: *mut c_void,
) -> Result<Box<SinkTaskHandle>, String>;

/// Registration entry for a sink plugin.
pub struct SinkPluginEntry {
    pub name: &'static str,
    pub schema: &'static [ConfigParam],
    pub spawn: SinkSpawnFn,
}

/// Distributed slice collecting all sink plugin entries at link time.
#[linkme::distributed_slice]
pub static SINK_PLUGINS: [SinkPluginEntry];
