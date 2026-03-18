// nes_sink_bindings: CXX bridge for sink lifecycle management.

pub use nes_buffer_bindings::*;
pub use nes_sink_runtime::*;

#[cxx::bridge]
pub mod ffi_sink {
    #[derive(Debug, PartialEq)]
    enum SendResult {
        Success,
        Full,
        Closed,
    }

    unsafe extern "C++" {
        include!("SinkBindings.hpp");
    }

    extern "Rust" {
        type SinkHandle;

        fn get_registered_sink_names() -> Vec<String>;

        fn sink_send_buffer(handle: &SinkHandle, buffer_ptr: usize) -> SendResult;
        fn stop_sink_bridge(handle: &SinkHandle) -> bool;
        fn is_sink_done_bridge(handle: &SinkHandle) -> bool;

        fn validate_sink_config(
            sink_name: &CxxString,
            config_keys: &CxxVector<CxxString>,
            config_values: &CxxVector<CxxString>,
            out_keys: &mut Vec<String>,
            out_values: &mut Vec<String>,
            out_types: &mut Vec<String>,
        ) -> Result<()>;

        fn spawn_sink_by_name(
            sink_name: &CxxString,
            sink_id: u64,
            config_keys: &CxxVector<CxxString>,
            config_values: &CxxVector<CxxString>,
            channel_capacity: u32,
            error_fn_ptr: usize,
            error_ctx_ptr: usize,
        ) -> Result<Box<SinkHandle>>;
    }
}

pub struct SinkHandle {
    inner: nes_sink_runtime::SinkTaskHandle,
}

fn sink_send_buffer(handle: &SinkHandle, buffer_ptr: usize) -> ffi_sink::SendResult {
    use nes_sink_runtime::SinkMessage;

    let raw_ptr = buffer_ptr as *mut nes_buffer_bindings::ffi::TupleBuffer;
    let unique = unsafe { cxx::UniquePtr::from_raw(raw_ptr) };
    let buf_handle = nes_buffer_runtime::TupleBufferHandle::new(unique);
    let iter = nes_sink_runtime::BufferIterator::new(buf_handle);
    let msg = SinkMessage::Data(iter);

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
    nes_sink_runtime::stop_sink(&handle.inner)
}

fn is_sink_done_bridge(handle: &SinkHandle) -> bool {
    nes_sink_runtime::is_sink_done(&handle.inner)
}

fn get_registered_sink_names() -> Vec<String> {
    nes_source_registry::get_sink_names()
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

fn validate_sink_config(
    sink_name: &cxx::CxxString,
    config_keys: &cxx::CxxVector<cxx::CxxString>,
    config_values: &cxx::CxxVector<cxx::CxxString>,
    out_keys: &mut Vec<String>,
    out_values: &mut Vec<String>,
    out_types: &mut Vec<String>,
) -> Result<(), String> {
    let name = sink_name.to_str().map_err(|e| format!("invalid sink name: {e}"))?;
    let config = zip_config(config_keys, config_values);

    let (keys, values, types) = nes_source_registry::validate_sink(name, &config)?;

    *out_keys = keys;
    *out_values = values;
    *out_types = types;
    Ok(())
}

fn spawn_sink_by_name(
    sink_name: &cxx::CxxString,
    sink_id: u64,
    config_keys: &cxx::CxxVector<cxx::CxxString>,
    config_values: &cxx::CxxVector<cxx::CxxString>,
    channel_capacity: u32,
    error_fn_ptr: usize,
    error_ctx_ptr: usize,
) -> Result<Box<SinkHandle>, String> {
    let name = sink_name.to_str().map_err(|e| format!("invalid sink name: {e}"))?;
    let config = zip_config(config_keys, config_values);

    let error_fn: nes_sink_runtime::sink::ErrorFnPtr = unsafe {
        std::mem::transmute::<usize, nes_sink_runtime::sink::ErrorFnPtr>(error_fn_ptr)
    };
    let error_ctx = error_ctx_ptr as *mut std::ffi::c_void;

    let task_handle = nes_source_registry::spawn_sink(
        name, &config, sink_id, channel_capacity as usize, error_fn, error_ctx,
    )?;

    Ok(Box::new(SinkHandle { inner: *task_handle }))
}
