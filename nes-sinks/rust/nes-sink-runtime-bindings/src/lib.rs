// nes_sink_runtime_bindings: CXX bridge for sink lifecycle management.

use crate::ffi::{WriteResult, on_error, on_flush};
use cxx::SharedPtr;
pub use nes_buffer_bindings::*;
use nes_buffer_runtime::TupleBuffer;
use nes_io_runtime_bindings::current_io_runtime;
use nes_sink_runtime::{SinkContext, SinkDataCommand, SinkTerminationCommand};
use nes_sink_validation::{ConfigOptions, validate};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
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
        unsafe fn sink_flush(handle: &SinkHandle) -> usize;

        /// Drives the sink towards termination and reports completion.
        ///
        /// Returns `true` once the sink task has fully run `AsyncSink::stop`
        /// and the runtime is safe to drop the handle. Returns `false` if
        /// the close request still needs to be issued or has not yet been
        /// acknowledged; callers must call again later. Mirrors the
        /// `source_stop` pattern: the handle must not be dropped until this
        /// returns `true`, otherwise the rust sink task panics.
        fn sink_stop(handle: &mut SinkHandle) -> Result<bool>;
        fn sink_fail(handle: &mut SinkHandle, message: String);
    }
}

unsafe impl Send for ffi::SinkContext {}
unsafe impl Sync for ffi::SinkContext {}

struct SinkHandle {
    controller: nes_sink_runtime::DataChannel,
    stop_signal: Option<nes_sink_runtime::TerminationSignal>,
    stop_done_signal: Option<tokio::sync::oneshot::Receiver<core::result::Result<(), String>>>,
    epoch_counter: AtomicUsize,
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
        stop_signal: Some(stop_signal),
        stop_done_signal: None,
        epoch_counter: AtomicUsize::new(0),
    }))
}

unsafe fn try_write(handle: &SinkHandle, buffer: *mut ffi::MemorySegment) -> WriteResult {
    let buffer = unsafe { TupleBuffer::from_raw(buffer) };
    match handle.controller.try_send(SinkDataCommand::Data(buffer)) {
        Ok(_) => WriteResult::Ok,
        Err(TrySendError::Full(_)) => WriteResult::Full,
        Err(TrySendError::Closed(_)) => panic!("Channel should not be closed"),
    }
}

unsafe fn sink_flush(handle: &SinkHandle) -> usize {
    let epoch = handle.epoch_counter.fetch_add(1, Relaxed);
    match handle.controller.try_send(SinkDataCommand::Flush(epoch)) {
        Ok(_) => epoch,
        Err(TrySendError::Full(_)) => 0,
        Err(TrySendError::Closed(_)) => panic!("Channel should not be closed"),
    }
}

fn sink_stop(handle: &mut SinkHandle) -> Result<bool, String> {
    match handle.stop_signal.take() {
        Some(stop_signal) => {
            let (stop_sender, mut stop_done) = tokio::sync::oneshot::channel();
            stop_signal
                .send(SinkTerminationCommand::Close(stop_sender))
                .expect("Sink channel should not be closed");
            match stop_done.try_recv() {
                Ok(r) => r.map(|_| true),
                Err(TryRecvError::Closed) => {
                    panic!("Sink channel should not be closed");
                }
                Err(TryRecvError::Empty) => {
                    handle.stop_done_signal = Some(stop_done);
                    Ok(false)
                }
            }
        }
        None => {
            match handle
                .stop_done_signal.as_mut()
                .expect("stop_signal done should have been set, if the previous call has returned false")
                .try_recv() {
                Ok(r) => r.map(|_| true),
                Err(TryRecvError::Closed) => {
                    panic!("Sink channel should not be closed");
                }
                Err(TryRecvError::Empty) => {
                    Ok(false)
                }
            }
        }
    }
}

fn sink_fail(handle: &mut SinkHandle, message: String) {
    let (stop_sender, _) = tokio::sync::oneshot::channel();
    let Some(stop_signal) = handle.stop_signal.take() else {
        return;
    };

    stop_signal
        .send(SinkTerminationCommand::Error(message, stop_sender))
        .expect("Sink channel should not be closed");
}
