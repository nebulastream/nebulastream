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

/// Phase 2 CXX bridge for source lifecycle management.
///
/// Declares FFI entry points that C++ (TokioSource) calls to manage
/// source tasks. Function pointers and void* contexts are passed as
/// usize because CXX cannot bridge `unsafe extern "C" fn(...)` or
/// `*mut c_void` directly.
#[cxx::bridge]
pub mod ffi_source {
    unsafe extern "C++" {
        include!("IoBindings.hpp");
    }

    extern "Rust" {
        // Phase 2: Source lifecycle
        //
        // SourceHandle is a local wrapper around nes_source_runtime::SourceTaskHandle.
        // CXX requires opaque types to be defined in the declaring crate (orphan rule).
        type SourceHandle;

        // Phase 2: Backpressure callbacks (called from C++, safe signatures)
        fn on_backpressure_applied(source_id: u64);
        fn on_backpressure_released(source_id: u64);

        fn stop_source(handle: &SourceHandle);

        /// Spawn a source task.
        ///
        /// All pointer arguments are passed as usize because CXX cannot bridge
        /// raw pointers in extern "Rust" blocks or function pointer types.
        /// C++ casts its pointers to uintptr_t / size_t before calling.
        fn spawn_source(
            source_id: u64,
            buffer_provider_ptr: usize,
            inflight_limit: u32,
            emit_fn_ptr: usize,
            emit_ctx_ptr: usize,
            error_fn_ptr: usize,
            error_ctx_ptr: usize,
        ) -> Result<Box<SourceHandle>>;

        /// Spawn a GeneratorSource that emits `count` sequential u64 values.
        ///
        /// All pointer arguments are passed as usize (CXX bridge limitation).
        /// C++ casts its pointers to uintptr_t / size_t before calling.
        fn spawn_generator_source(
            source_id: u64,
            count: u64,
            interval_ms: u64,
            buffer_provider_ptr: usize,
            inflight_limit: u32,
            emit_fn_ptr: usize,
            emit_ctx_ptr: usize,
            error_fn_ptr: usize,
            error_ctx_ptr: usize,
        ) -> Result<Box<SourceHandle>>;
    }
}

/// Local wrapper around SourceTaskHandle for CXX orphan rule compliance.
///
/// CXX generates trait implementations for opaque types declared in
/// `extern "Rust"` blocks. The orphan rule requires the type to be defined
/// in the declaring crate. This wrapper satisfies that constraint while
/// delegating all operations to the inner SourceTaskHandle.
pub struct SourceHandle {
    inner: nes_source_runtime::handle::SourceTaskHandle,
}

// --- Phase 2 FFI wrapper functions ---
//
// These thin wrappers are defined here (not in nes_io_bindings) to avoid
// circular dependencies: nes_source_runtime depends on nes_io_bindings,
// so nes_io_bindings cannot depend on nes_source_runtime.

/// Spawn a source task. Reconstructs function pointer types from usize values.
///
/// NOTE: This function currently creates a placeholder source that immediately
/// returns EndOfStream. The actual source type dispatch (which concrete
/// AsyncSource impl to create) will be added in Phase 3 when C++ source
/// types are wired up.
/// Install async buffer notification once (on first source spawn).
fn ensure_buffer_notification() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        nes_source_runtime::buffer::install_buffer_notification();
    });
}

fn spawn_source(
    source_id: u64,
    buffer_provider_ptr: usize,
    inflight_limit: u32,
    emit_fn_ptr: usize,
    emit_ctx_ptr: usize,
    error_fn_ptr: usize,
    error_ctx_ptr: usize,
) -> Result<Box<SourceHandle>, String> {
    ensure_buffer_notification();

    // Reconstruct function pointers, void* contexts, and buffer provider
    // from usize values. CXX cannot bridge raw pointers or function pointer
    // types in extern "Rust" blocks, so C++ passes them as uintptr_t/size_t.
    //
    // SAFETY: C++ caller is responsible for passing valid function pointers
    // and context pointers. The usize encoding is a CXX bridge limitation.
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
            buffer_provider_ptr as *mut nes_io_bindings::ffi::AbstractBufferProvider
        )
    };

    let inner = nes_source_runtime::source::spawn_source(
        source_id,
        PlaceholderSource,
        buffer_provider,
        inflight_limit,
        emit_fn,
        emit_ctx,
        error_fn,
        error_ctx,
    );
    Ok(Box::new(SourceHandle { inner: *inner }))
}

/// Spawn a GeneratorSource that emits `count` sequential u64 values.
///
/// Reconstructs function pointers from usize values (same pattern as spawn_source).
fn spawn_generator_source(
    source_id: u64,
    count: u64,
    interval_ms: u64,
    buffer_provider_ptr: usize,
    inflight_limit: u32,
    emit_fn_ptr: usize,
    emit_ctx_ptr: usize,
    error_fn_ptr: usize,
    error_ctx_ptr: usize,
) -> Result<Box<SourceHandle>, String> {
    ensure_buffer_notification();
    use nes_source_runtime::generator::GeneratorSource;
    use std::time::Duration;

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
            buffer_provider_ptr as *mut nes_io_bindings::ffi::AbstractBufferProvider
        )
    };

    let generator = GeneratorSource::new(count, Duration::from_millis(interval_ms));

    let inner = nes_source_runtime::source::spawn_source(
        source_id,
        generator,
        buffer_provider,
        inflight_limit,
        emit_fn,
        emit_ctx,
        error_fn,
        error_ctx,
    );
    Ok(Box::new(SourceHandle { inner: *inner }))
}

/// Placeholder source that returns EndOfStream immediately.
/// Used until Phase 3 wires up actual source type dispatch.
struct PlaceholderSource;

impl nes_source_runtime::AsyncSource for PlaceholderSource {
    async fn run(
        &mut self,
        _ctx: &nes_source_runtime::SourceContext,
    ) -> nes_source_runtime::SourceResult {
        nes_source_runtime::SourceResult::EndOfStream
    }
}

/// Stop a running source by cancelling its CancellationToken.
fn stop_source(handle: &SourceHandle) {
    nes_source_runtime::handle::stop_source(&handle.inner);
}

/// Called from C++ when backpressure is applied to a source.
fn on_backpressure_applied(source_id: u64) {
    nes_source_runtime::backpressure::on_backpressure_applied(source_id);
}

/// Called from C++ when backpressure is released for a source.
fn on_backpressure_released(source_id: u64) {
    nes_source_runtime::backpressure::on_backpressure_released(source_id);
}

// --- Phase 5: Sink CXX bridge ---

/// Phase 5 CXX bridge for sink lifecycle management.
///
/// Declares FFI entry points that C++ (TokioSink) calls to manage
/// sink tasks. Mirrors the ffi_source pattern.
#[cxx::bridge]
pub mod ffi_sink {
    /// Result of trying to send a buffer to the sink channel.
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

        /// Try to send a buffer to the sink's async channel.
        /// buffer_ptr is a raw pointer to a C++ TupleBuffer that C++ has already retained.
        /// On Success: Rust takes ownership (will release on drop).
        /// On Full/Closed: Rust does NOT take ownership (std::mem::forget prevents drop/release).
        ///   C++ must call release() to undo the retain.
        fn sink_send_buffer(handle: &SinkHandle, buffer_ptr: usize) -> SendResult;

        /// Send Close message to the sink via try_send.
        /// Returns true if Close was sent or channel already closed.
        /// Returns false if channel is full (caller should retry via repeatTask).
        fn stop_sink_bridge(handle: &SinkHandle) -> bool;

        /// Check if the sink task has completed (receiver dropped).
        fn is_sink_done_bridge(handle: &SinkHandle) -> bool;

        /// Spawn a DevNull sink (consumes and drops all buffers).
        /// Used for testing TokioSink operator mechanics.
        fn spawn_devnull_sink(
            sink_id: u64,
            channel_capacity: u32,
            error_fn_ptr: usize,
            error_ctx_ptr: usize,
        ) -> Result<Box<SinkHandle>>;

        /// Spawn a file sink that writes raw binary data to a file.
        fn spawn_file_sink(
            sink_id: u64,
            file_path: &str,
            channel_capacity: u32,
            error_fn_ptr: usize,
            error_ctx_ptr: usize,
        ) -> Result<Box<SinkHandle>>;
    }
}

/// Local wrapper around SinkTaskHandle for CXX orphan rule compliance.
pub struct SinkHandle {
    inner: nes_source_runtime::sink_handle::SinkTaskHandle,
}

// --- Phase 5 sink FFI wrapper functions ---

fn sink_send_buffer(handle: &SinkHandle, buffer_ptr: usize) -> ffi_sink::SendResult {
    use nes_source_runtime::sink_context::SinkMessage;

    // C++ has already called retain() on the buffer before passing the raw pointer.
    // Rust wraps the raw pointer in UniquePtr::from_raw (no additional retain).
    // On Success: TupleBufferHandle::Drop calls release() (matching C++'s retain).
    // On Full/Closed: std::mem::forget prevents Drop, so no release. C++ calls
    //   release() to undo its retain.
    let raw_ptr = buffer_ptr as *mut nes_io_bindings::ffi::TupleBuffer;
    // SAFETY: C++ guarantees the pointer is valid and already retained.
    let unique = unsafe { cxx::UniquePtr::from_raw(raw_ptr) };
    let buf_handle = nes_source_runtime::buffer::TupleBufferHandle::new(unique);
    let msg = SinkMessage::Data(buf_handle);

    match handle.inner.sender().try_send(msg) {
        Ok(()) => ffi_sink::SendResult::Success,
        Err(async_channel::TrySendError::Full(msg)) => {
            // Do NOT drop -- C++ will release() to undo retain
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
        sink_id,
        sink,
        channel_capacity as usize,
        error_fn,
        error_ctx,
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
        sink_id,
        sink,
        channel_capacity as usize,
        error_fn,
        error_ctx,
    );
    Ok(Box::new(SinkHandle { inner: *inner }))
}

/// Sink that consumes all data buffers and returns Ok on Close.
/// Used for testing TokioSink operator mechanics.
struct DevNullSink;

impl nes_source_runtime::AsyncSink for DevNullSink {
    async fn run(
        &mut self,
        ctx: &nes_source_runtime::SinkContext,
    ) -> Result<(), nes_source_runtime::SinkError> {
        loop {
            match ctx.recv().await {
                nes_source_runtime::SinkMessage::Data(_buf) => {
                    // Drop buffer (release refcount)
                }
                nes_source_runtime::SinkMessage::Flush => {} // no-op
                nes_source_runtime::SinkMessage::Close => return Ok(()),
            }
        }
    }
}
