// nes_source_lib: Static library crate linking CXX bridge to Rust runtime.
//
// This crate defines Phase 2+ CXX bridge declarations that reference types
// from nes_source_runtime (which depends on nes_io_bindings). Placing
// the bridge here avoids a circular dependency between bindings and runtime.
//
// The Phase 1 bridge (init_source_runtime) remains in nes_io_bindings
// because it has no runtime crate dependencies.

pub use nes_io_bindings::*;
pub use nes_source_runtime::*;

// ---------------------------------------------------------------------------
// Link-time registry interface.
//
// The generated registry crate (nes_source_registry, built in the CMake build
// directory) provides these symbols via #[no_mangle] extern "C". The umbrella
// crate links both this crate and the registry crate, resolving the symbols.
//
// This means nes_source_lib has ZERO compile-time knowledge of plugin crates.
// ---------------------------------------------------------------------------
unsafe extern "C" {
    /// Spawn a registered source by name.
    ///
    /// `config_encoded` is a sequence of null-terminated key-value pairs:
    ///   "key1\0val1\0key2\0val2\0"
    /// `config_encoded_len` is the total byte length (including all \0s).
    ///
    /// On success: writes a `*mut SourceTaskHandle` into `handle_out`, returns 0.
    /// On error:   writes a malloc'd C string into `error_out`, returns -1.
    ///             Caller must free the error string via `nes_registry_free_error`.
    fn nes_registry_spawn(
        name_ptr: *const u8, name_len: usize,
        config_encoded: *const u8, config_encoded_len: usize,
        source_id: u64,
        buffer_provider_ptr: usize,
        inflight_limit: u32,
        emit_fn_ptr: usize, emit_ctx_ptr: usize,
        error_fn_ptr: usize, error_ctx_ptr: usize,
        handle_out: *mut *mut std::ffi::c_void,
        error_out: *mut *mut u8, error_len_out: *mut usize,
    ) -> i32;

    /// Validate config for a named source.
    ///
    /// On success: writes encoded validated config into `result_out` / `result_len_out`,
    ///   format: "key\0value\0type\0key\0value\0type\0..."
    ///   Caller must free via `nes_registry_free_error`.
    ///   Returns 0.
    /// On error: writes error string into `error_out` / `error_len_out`, returns -1.
    fn nes_registry_validate(
        name_ptr: *const u8, name_len: usize,
        config_encoded: *const u8, config_encoded_len: usize,
        result_out: *mut *mut u8, result_len_out: *mut usize,
        error_out: *mut *mut u8, error_len_out: *mut usize,
    ) -> i32;

    /// Free a buffer allocated by the registry crate.
    fn nes_registry_free_buf(ptr: *mut u8, len: usize);
}

/// Encode a HashMap as "key\0value\0key\0value\0..." bytes.
fn encode_config(config: &std::collections::HashMap<String, String>) -> Vec<u8> {
    let mut buf = Vec::new();
    for (k, v) in config {
        buf.extend_from_slice(k.as_bytes());
        buf.push(0);
        buf.extend_from_slice(v.as_bytes());
        buf.push(0);
    }
    buf
}

/// Decode "key\0value\0type\0..." into parallel vecs.
fn decode_triples(data: &[u8]) -> (Vec<String>, Vec<String>, Vec<String>) {
    let parts: Vec<&str> = if data.is_empty() {
        vec![]
    } else {
        // Remove trailing \0 if present, then split
        let s = if data.last() == Some(&0) { &data[..data.len()-1] } else { data };
        std::str::from_utf8(s).unwrap_or("").split('\0').collect()
    };
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut types = Vec::new();
    for chunk in parts.chunks(3) {
        if chunk.len() == 3 {
            keys.push(chunk[0].to_string());
            values.push(chunk[1].to_string());
            types.push(chunk[2].to_string());
        }
    }
    (keys, values, types)
}

/// Read an error string from the registry's output pointers.
unsafe fn read_registry_error(error_ptr: *mut u8, error_len: usize) -> String {
    if error_ptr.is_null() {
        return "unknown registry error".to_string();
    }
    let msg = unsafe { std::str::from_utf8(std::slice::from_raw_parts(error_ptr, error_len)) }
        .unwrap_or("invalid utf8 in error")
        .to_string();
    unsafe { nes_registry_free_buf(error_ptr, error_len) };
    msg
}

/// Phase 2 CXX bridge for source lifecycle management.
#[cxx::bridge]
pub mod ffi_source {
    unsafe extern "C++" {
        include!("IoBindings.hpp");
    }

    extern "Rust" {
        type SourceHandle;

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
/// Calls into the generated registry crate via extern "C" (resolved at link time).
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
    let encoded = encode_config(&config);

    let mut handle_out: *mut std::ffi::c_void = std::ptr::null_mut();
    let mut error_ptr: *mut u8 = std::ptr::null_mut();
    let mut error_len: usize = 0;

    let rc = unsafe {
        nes_registry_spawn(
            name.as_ptr(), name.len(),
            encoded.as_ptr(), encoded.len(),
            source_id,
            buffer_provider_ptr,
            inflight_limit,
            emit_fn_ptr, emit_ctx_ptr,
            error_fn_ptr, error_ctx_ptr,
            &mut handle_out,
            &mut error_ptr, &mut error_len,
        )
    };

    if rc != 0 {
        let msg = unsafe { read_registry_error(error_ptr, error_len) };
        return Err(msg);
    }

    // The registry created a Box<SourceTaskHandle> and returned it as *mut c_void.
    // Reconstruct it here.
    let task_handle = unsafe { Box::from_raw(handle_out as *mut nes_source_runtime::handle::SourceTaskHandle) };
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
    let encoded = encode_config(&config);

    let mut result_ptr: *mut u8 = std::ptr::null_mut();
    let mut result_len: usize = 0;
    let mut error_ptr: *mut u8 = std::ptr::null_mut();
    let mut error_len: usize = 0;

    let rc = unsafe {
        nes_registry_validate(
            name.as_ptr(), name.len(),
            encoded.as_ptr(), encoded.len(),
            &mut result_ptr, &mut result_len,
            &mut error_ptr, &mut error_len,
        )
    };

    if rc != 0 {
        let msg = unsafe { read_registry_error(error_ptr, error_len) };
        return Err(msg);
    }

    let result_data = unsafe { std::slice::from_raw_parts(result_ptr, result_len) };
    let (keys, values, types) = decode_triples(result_data);
    unsafe { nes_registry_free_buf(result_ptr, result_len); }

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

// --- Phase 5: Sink CXX bridge (unchanged) ---

#[cxx::bridge]
pub mod ffi_sink {
    #[derive(Debug, PartialEq)]
    enum SendResult {
        Success,
        Full,
        Closed,
    }

    unsafe extern "C++" {
        include!("IoBindings.hpp");
    }

    extern "Rust" {
        type SinkHandle;

        fn sink_send_buffer(handle: &SinkHandle, buffer_ptr: usize) -> SendResult;
        fn stop_sink_bridge(handle: &SinkHandle) -> bool;
        fn is_sink_done_bridge(handle: &SinkHandle) -> bool;

        fn spawn_devnull_sink(
            sink_id: u64,
            channel_capacity: u32,
            error_fn_ptr: usize,
            error_ctx_ptr: usize,
        ) -> Result<Box<SinkHandle>>;

        fn spawn_file_sink(
            sink_id: u64,
            file_path: &str,
            channel_capacity: u32,
            error_fn_ptr: usize,
            error_ctx_ptr: usize,
        ) -> Result<Box<SinkHandle>>;
    }
}

pub struct SinkHandle {
    inner: nes_source_runtime::sink_handle::SinkTaskHandle,
}

fn sink_send_buffer(handle: &SinkHandle, buffer_ptr: usize) -> ffi_sink::SendResult {
    use nes_source_runtime::sink_context::SinkMessage;

    let raw_ptr = buffer_ptr as *mut nes_io_bindings::ffi::TupleBuffer;
    let unique = unsafe { cxx::UniquePtr::from_raw(raw_ptr) };
    let buf_handle = nes_source_runtime::buffer::TupleBufferHandle::new(unique);
    let msg = SinkMessage::Data(buf_handle);

    match handle.inner.sender().try_send(msg) {
        Ok(()) => ffi_sink::SendResult::Success,
        Err(async_channel::TrySendError::Full(msg)) => {
            std::mem::forget(msg);
            ffi_sink::SendResult::Full
        }
        Err(async_channel::TrySendError::Closed(msg)) => {
            std::mem::forget(msg);
            ffi_sink::SendResult::Closed
        }
    }
}

fn stop_sink_bridge(handle: &SinkHandle) -> bool {
    nes_source_runtime::sink_handle::stop_sink(&handle.inner)
}

fn is_sink_done_bridge(handle: &SinkHandle) -> bool {
    nes_source_runtime::sink_handle::is_sink_done(&handle.inner)
}

fn spawn_devnull_sink(
    sink_id: u64,
    channel_capacity: u32,
    error_fn_ptr: usize,
    error_ctx_ptr: usize,
) -> Result<Box<SinkHandle>, String> {
    let error_fn: nes_source_runtime::sink::ErrorFnPtr = unsafe {
        std::mem::transmute::<usize, nes_source_runtime::sink::ErrorFnPtr>(error_fn_ptr)
    };
    let error_ctx = error_ctx_ptr as *mut std::ffi::c_void;

    let sink = DevNullSink;
    let inner = nes_source_runtime::sink::spawn_sink(
        sink_id, sink, channel_capacity as usize, error_fn, error_ctx,
    );
    Ok(Box::new(SinkHandle { inner: *inner }))
}

fn spawn_file_sink(
    sink_id: u64,
    file_path: &str,
    channel_capacity: u32,
    error_fn_ptr: usize,
    error_ctx_ptr: usize,
) -> Result<Box<SinkHandle>, String> {
    let error_fn: nes_source_runtime::sink::ErrorFnPtr = unsafe {
        std::mem::transmute::<usize, nes_source_runtime::sink::ErrorFnPtr>(error_fn_ptr)
    };
    let error_ctx = error_ctx_ptr as *mut std::ffi::c_void;

    let sink = nes_source_runtime::AsyncFileSink::new(std::path::PathBuf::from(file_path));
    let inner = nes_source_runtime::sink::spawn_sink(
        sink_id, sink, channel_capacity as usize, error_fn, error_ctx,
    );
    Ok(Box::new(SinkHandle { inner: *inner }))
}

struct DevNullSink;

impl nes_source_runtime::AsyncSink for DevNullSink {
    async fn run(
        &mut self,
        ctx: &nes_source_runtime::SinkContext,
    ) -> Result<(), nes_source_runtime::SinkError> {
        loop {
            match ctx.recv().await {
                nes_source_runtime::SinkMessage::Data(_buf) => {}
                nes_source_runtime::SinkMessage::Flush => {}
                nes_source_runtime::SinkMessage::Close => return Ok(()),
            }
        }
    }
}
