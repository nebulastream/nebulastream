// nes_sink_bindings: CXX bridge for sink lifecycle management.
//
// Bridges Rust sink spawn/stop/send to C++ via CXX. Depends on
// nes_buffer_bindings (buffer types) and nes_source_runtime (AsyncSink framework).

pub use nes_buffer_bindings::*;
pub use nes_source_runtime::*;

/// Sink CXX bridge.
#[cxx::bridge]
pub mod ffi_sink {
    /// Schema field passed from C++ to Rust at sink spawn time.
    #[derive(Debug, Clone)]
    struct FfiSchemaField {
        name: String,
        data_type: String,
        nullable: bool,
    }

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
            schema: &[FfiSchemaField],
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

    let raw_ptr = buffer_ptr as *mut nes_buffer_bindings::ffi::TupleBuffer;
    let unique = unsafe { cxx::UniquePtr::from_raw(raw_ptr) };
    let buf_handle = nes_source_runtime::buffer::TupleBufferHandle::new(unique);
    let iter = nes_source_runtime::BufferIterator::new(buf_handle);
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
    schema: &[ffi_sink::FfiSchemaField],
    channel_capacity: u32,
    error_fn_ptr: usize,
    error_ctx_ptr: usize,
) -> Result<Box<SinkHandle>, String> {
    let error_fn: nes_source_runtime::sink::ErrorFnPtr = unsafe {
        std::mem::transmute::<usize, nes_source_runtime::sink::ErrorFnPtr>(error_fn_ptr)
    };
    let error_ctx = error_ctx_ptr as *mut std::ffi::c_void;

    let schema_fields: Vec<nes_source_runtime::SchemaField> = schema
        .iter()
        .map(|f| nes_source_runtime::SchemaField::new(
            f.name.clone(),
            f.data_type.clone(),
            f.nullable,
        ))
        .collect();
    let sink = nes_source_runtime::AsyncFileSink::new(std::path::PathBuf::from(file_path), schema_fields);
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
