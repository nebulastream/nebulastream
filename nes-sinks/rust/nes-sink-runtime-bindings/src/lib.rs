// nes_sink_runtime_bindings: CXX bridge for sink lifecycle management.

use crate::ffi::{WriteResult, on_error, on_flush};
use cxx::SharedPtr;
pub use nes_buffer_bindings::*;
use nes_buffer_runtime::TupleBuffer;
use nes_io_runtime_bindings::current_io_runtime;
use nes_sink_runtime::{SinkCommand, SinkContext};
use nes_sink_validation::{ConfigOptions, validate};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::{Level, span};

#[cxx::bridge]
pub mod ffi {
    enum WriteResult {
        Ok,
        Full,
    }

    struct QueryContext {
        query_id: String,
        distributed_query_id: String,
        sink_id: usize,
        sink_type: String,
        sink_config: String,
    }

    unsafe extern "C++" {
        include!("BufferBindings.hpp");
        include!("SinkRuntimeBindings.hpp");
        #[namespace = "NES::detail"]
        type MemorySegment = nes_buffer_bindings::ffi::MemorySegment;
        type SinkContext;
        unsafe fn on_error(context: &SinkContext, error_msg: String);
        unsafe fn on_flush(context: &SinkContext, epoch: usize);
    }
    extern "Rust" {
        type SinkHandle;
        /// Constructs the sink handle. The IORuntime is fetched from the
        /// calling C++ thread's `WorkerLocalSingleton<NES::IORuntime>` via the
        /// `nes_current_io_runtime_address` C ABI hook in `IORuntime.cpp`.
        fn create_handle(
            query_context: QueryContext,
            context: SharedPtr<SinkContext>,
        ) -> Result<Box<SinkHandle>>;

        unsafe fn try_write(handle: &SinkHandle, buffer: *mut MemorySegment) -> WriteResult;
        unsafe fn flush(handle: &SinkHandle) -> usize;

        /// Drives the sink towards termination and reports completion.
        ///
        /// Returns `true` once the sink task has fully run `AsyncSink::stop`
        /// and the runtime is safe to drop the handle. Returns `false` if
        /// the close request still needs to be issued or has not yet been
        /// acknowledged; callers must call again later. Mirrors the
        /// `source_stop` pattern: the handle must not be dropped until this
        /// returns `true`, otherwise the rust sink task panics.
        fn sink_stop(handle: &mut SinkHandle) -> bool;
    }
}

unsafe impl Send for ffi::SinkContext {}
unsafe impl Sync for ffi::SinkContext {}

struct SinkHandle {
    controller: nes_sink_runtime::Controller,
    stop_signal: tokio::sync::oneshot::Receiver<()>,
}

fn create_handle(
    query_context: ffi::QueryContext,
    context: SharedPtr<ffi::SinkContext>,
) -> Result<Box<SinkHandle>, String> {
    let runtime = current_io_runtime().expect("Could not fetch IORuntime from C++ thread");
    let config = serde_json::from_str::<ConfigOptions>(&query_context.sink_config)
        .expect("FFI serialization error. Could not convert config options to rust representation");

    validate(&query_context.sink_type, &config).map_err(|e| e.to_string())?;
    let c1 = context.clone();
    let c2 = context.clone();
    let (controller, stop_signal) = {
        let span = span!(Level::INFO, "Sink",  distributed_query_id = %query_context.distributed_query_id, query_id = %query_context.query_id, sink_type = %query_context.sink_type, sink_id = %query_context.sink_id);
        let _entered = span.enter();
        nes_sink_runtime::start_sink(
            &query_context.sink_type,
            &config,
            runtime,
            SinkContext {
                on_error: Box::new(move |error| unsafe { on_error(c1.as_ref().unwrap(), error) }),
                on_flush: Box::new(move |epoch| unsafe { on_flush(c2.as_ref().unwrap(), epoch) }),
            },
        )?
    };
    Ok(Box::new(SinkHandle {
        controller,
        stop_signal,
    }))
}

unsafe fn try_write(handle: &SinkHandle, buffer: *mut ffi::MemorySegment) -> WriteResult {
    let buffer = unsafe { TupleBuffer::from_raw(buffer) };
    match handle.controller.try_send(SinkCommand::Data(buffer)) {
        Ok(_) => WriteResult::Ok,
        Err(TrySendError::Full(_)) => WriteResult::Full,
        Err(TrySendError::Closed(_)) => panic!("Channel should not be closed"),
    }
}

static EPOCH_COUNTER: AtomicUsize = AtomicUsize::new(1);
unsafe fn flush(handle: &SinkHandle) -> usize {
    let epoch = EPOCH_COUNTER.fetch_add(1, SeqCst);
    match handle.controller.try_send(SinkCommand::Flush(epoch)) {
        Ok(_) => epoch,
        Err(TrySendError::Full(_)) => 0,
        Err(TrySendError::Closed(_)) => panic!("Channel should not be closed"),
    }
}

fn sink_stop(handle: &mut SinkHandle) -> bool {
    match handle.stop_signal.try_recv() {
        Ok(()) => true,
        Err(TryRecvError::Empty) => match handle.controller.try_send(SinkCommand::Close) {
            // Sender accepted or queue full → close is either in flight or already pending.
            // Either way the task will terminate; the caller must keep polling.
            Ok(_) | Err(TrySendError::Full(_)) => false,
            Err(TrySendError::Closed(_)) => {
                // The task finished and dropped the receiver between our
                // try_recv and try_send; the stop signal must have been
                // sent before the task exited.
                handle
                    .stop_signal
                    .try_recv()
                    .expect("The stop signal should have been sent before closing the sink");
                true
            }
        },
        Err(TryRecvError::Closed) => {
            panic!("Stop signal should have been sent before closing the sink");
        }
    }
}
