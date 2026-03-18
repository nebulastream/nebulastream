// nes_source_bindings: CXX bridge for source lifecycle management.
//
// Bridges Rust source spawn/stop/validate to C++ via CXX. Depends on
// nes_source_registry for name-based dispatch and nes_source_runtime
// for the AsyncSource framework.

pub use nes_buffer_bindings::*;
pub use nes_source_runtime::*;

/// Source lifecycle CXX bridge.
#[cxx::bridge]
pub mod ffi_source {
    unsafe extern "C++" {
        include!("SourceBindings.hpp");
    }

    extern "Rust" {
        type SourceHandle;

        fn get_registered_source_names() -> Vec<String>;

        fn on_backpressure_applied(source_id: u64);
        fn on_backpressure_released(source_id: u64);

        fn stop_source(handle: &SourceHandle);

        fn spawn_source_by_name(
            source_name: &CxxString,
            source_id: u64,
            config_keys: &CxxVector<CxxString>,
            config_values: &CxxVector<CxxString>,
            buffer_provider_ptr: usize,
            inflight_limit: u32,
            emit_fn_ptr: usize,
            emit_ctx_ptr: usize,
            error_fn_ptr: usize,
            error_ctx_ptr: usize,
        ) -> Result<Box<SourceHandle>>;

        fn validate_source_config(
            source_name: &CxxString,
            config_keys: &CxxVector<CxxString>,
            config_values: &CxxVector<CxxString>,
            out_keys: &mut Vec<String>,
            out_values: &mut Vec<String>,
            out_types: &mut Vec<String>,
        ) -> Result<()>;
    }
}

pub struct SourceHandle {
    inner: nes_source_runtime::handle::SourceTaskHandle,
}

// --- FFI wrapper functions ---

fn get_registered_source_names() -> Vec<String> {
    nes_source_registry::get_source_names()
}

fn ensure_buffer_notification() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        nes_source_runtime::buffer::install_buffer_notification();
    });
}

fn zip_config(
    keys: &cxx::CxxVector<cxx::CxxString>,
    values: &cxx::CxxVector<cxx::CxxString>,
) -> std::collections::HashMap<String, String> {
    keys.iter()
        .zip(values.iter())
        .map(|(k, v)| (k.to_string_lossy().into_owned(), v.to_string_lossy().into_owned()))
        .collect()
}

/// Spawn any registered Tokio source by name.
fn spawn_source_by_name(
    source_name: &cxx::CxxString,
    source_id: u64,
    config_keys: &cxx::CxxVector<cxx::CxxString>,
    config_values: &cxx::CxxVector<cxx::CxxString>,
    buffer_provider_ptr: usize,
    inflight_limit: u32,
    emit_fn_ptr: usize,
    emit_ctx_ptr: usize,
    error_fn_ptr: usize,
    error_ctx_ptr: usize,
) -> Result<Box<SourceHandle>, String> {
    ensure_buffer_notification();

    let name = source_name.to_str().map_err(|e| format!("invalid source name: {e}"))?;
    let config = zip_config(config_keys, config_values);

    let emit_fn: nes_source_runtime::bridge::EmitFnPtr = unsafe {
        std::mem::transmute::<usize, nes_source_runtime::bridge::EmitFnPtr>(emit_fn_ptr)
    };
    let emit_ctx = emit_ctx_ptr as *mut std::ffi::c_void;
    let error_fn: nes_source_runtime::source::ErrorFnPtr = unsafe {
        std::mem::transmute::<usize, nes_source_runtime::source::ErrorFnPtr>(error_fn_ptr)
    };
    let error_ctx = error_ctx_ptr as *mut std::ffi::c_void;
    let buffer_provider = unsafe {
        nes_source_runtime::BufferProviderHandle::from_raw(
            buffer_provider_ptr as *mut nes_buffer_bindings::ffi::AbstractBufferProvider,
        )
    };

    let task_handle = nes_source_registry::spawn_source(
        name,
        &config,
        source_id,
        buffer_provider,
        inflight_limit,
        emit_fn,
        emit_ctx,
        error_fn,
        error_ctx,
    )?;

    Ok(Box::new(SourceHandle { inner: *task_handle }))
}

fn validate_source_config(
    source_name: &cxx::CxxString,
    config_keys: &cxx::CxxVector<cxx::CxxString>,
    config_values: &cxx::CxxVector<cxx::CxxString>,
    out_keys: &mut Vec<String>,
    out_values: &mut Vec<String>,
    out_types: &mut Vec<String>,
) -> Result<(), String> {
    let name = source_name.to_str().map_err(|e| format!("invalid source name: {e}"))?;
    let config = zip_config(config_keys, config_values);

    let (keys, values, types) = nes_source_registry::validate_source(name, &config)?;

    *out_keys = keys;
    *out_values = values;
    *out_types = types;
    Ok(())
}

fn stop_source(handle: &SourceHandle) {
    nes_source_runtime::handle::stop_source(&handle.inner);
}

fn on_backpressure_applied(source_id: u64) {
    nes_source_runtime::backpressure::on_backpressure_applied(source_id);
}

fn on_backpressure_released(source_id: u64) {
    nes_source_runtime::backpressure::on_backpressure_released(source_id);
}
