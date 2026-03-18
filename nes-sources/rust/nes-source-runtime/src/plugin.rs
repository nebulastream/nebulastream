// Plugin registration for AsyncSource implementations.
//
// Plugin crates use `#[linkme::distributed_slice(SOURCE_PLUGINS)]` to register
// themselves at link time. The registry discovers all entries without compile-time
// knowledge of plugin crates.

use std::collections::HashMap;
use std::ffi::c_void;

use crate::bridge::EmitFnPtr;
use crate::buffer::BufferProviderHandle;
use crate::config::ConfigParam;
use crate::handle::SourceTaskHandle;
use crate::source::ErrorFnPtr;

/// Type-erased spawn function for a source plugin.
///
/// Each plugin provides a function that creates its concrete AsyncSource
/// and calls `spawn_source()` internally. This avoids the need for trait
/// objects or dynamic dispatch at the registry level.
pub type SourceSpawnFn = fn(
    config: &HashMap<String, String>,
    source_id: u64,
    buffer_provider: BufferProviderHandle,
    inflight_limit: u32,
    emit_fn: EmitFnPtr,
    emit_ctx: *mut c_void,
    error_fn: ErrorFnPtr,
    error_ctx: *mut c_void,
) -> Result<Box<SourceTaskHandle>, String>;

/// Registration entry for a source plugin.
pub struct SourcePluginEntry {
    pub name: &'static str,
    pub schema: &'static [ConfigParam],
    pub spawn: SourceSpawnFn,
}

/// Distributed slice collecting all source plugin entries at link time.
#[linkme::distributed_slice]
pub static SOURCE_PLUGINS: [SourcePluginEntry];
