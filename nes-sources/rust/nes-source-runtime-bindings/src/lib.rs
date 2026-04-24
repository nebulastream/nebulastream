// nes_source_runtime_bindings: CXX bridge for source lifecycle management.

use crate::ffi::AsyncFunctionResult;
use async_trait::async_trait;
use cxx::SharedPtr;
use nes_buffer_bindings::ffi::BufferProviderHandle;
pub use nes_buffer_bindings::*;
use nes_buffer_runtime::{BufferProvider, TupleBuffer};
use nes_io_runtime::get_runtime;
use nes_source_runtime::{AsyncEmitter, SourceCommand};
use nes_source_validation::{ConfigOptions, validate};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::{Level, span};

#[cxx::bridge]
pub mod ffi {
    #[derive(Debug)]
    enum AsyncCompletionResult {
        Success,
    }
    #[derive(Debug)]
    enum AsyncFunctionResult {
        Success,
        CallbackRegistered,
        Retry,
    }

    struct QueryContext {
        query_id: String,
        distributed_query_id: String,
        source_id: usize,
        source_type: String,
        source_config: String,
    }

    extern "Rust" {
        type AsyncCompletionContext;
    }

    unsafe extern "C++" {
        include!("BufferBindings.hpp");
        include!("SourceRuntimeBindings.hpp");
        #[namespace = "NES::detail"]
        type MemorySegment = nes_buffer_bindings::ffi::MemorySegment;
        type BufferProviderHandle = nes_buffer_bindings::ffi::BufferProviderHandle;
        type SourceContext;
        unsafe fn source_on_error(
            context: Pin<&mut SourceContext>,
            error_msg: String,
            done: fn(Box<AsyncCompletionContext>, ret: AsyncCompletionResult),
            ctx: Box<AsyncCompletionContext>,
        ) -> AsyncFunctionResult;
        unsafe fn source_on_data(
            context: Pin<&mut SourceContext>,
            buffer: *mut MemorySegment,
            done: fn(Box<AsyncCompletionContext>, ret: AsyncCompletionResult),
            ctx: Box<AsyncCompletionContext>,
        ) -> AsyncFunctionResult;
        unsafe fn source_on_eos(
            context: Pin<&mut SourceContext>,
            done: fn(Box<AsyncCompletionContext>, ret: AsyncCompletionResult),
            ctx: Box<AsyncCompletionContext>,
        ) -> AsyncFunctionResult;
    }
    extern "Rust" {
        type RustSourceHandle;
        fn source_create_handle(
            query_context: QueryContext,
            runtime_idx: usize,
            context: SharedPtr<SourceContext>,
            buffer_provider_handle: SharedPtr<BufferProviderHandle>,
        ) -> Result<Box<RustSourceHandle>>;
        fn source_stop(handle: &mut RustSourceHandle) -> bool;
    }
}

unsafe impl Send for ffi::SourceContext {}
unsafe impl Sync for ffi::SourceContext {}

struct RustSourceHandle {
    channel: nes_source_runtime::Controller,
    stop_signal: tokio::sync::oneshot::Receiver<Emitter>,
}
pub struct AsyncCompletionContext(tokio::sync::oneshot::Sender<ffi::AsyncCompletionResult>);

struct Emitter {
    context: SharedPtr<ffi::SourceContext>,
}

async fn async_operation<Op>(operation: Op)
where
    Op: FnOnce(
        fn(Box<AsyncCompletionContext>, ret: ffi::AsyncCompletionResult),
        Box<AsyncCompletionContext>,
    ) -> AsyncFunctionResult,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    let completion_context = Box::new(AsyncCompletionContext(tx));

    let result = operation(
        |context: Box<AsyncCompletionContext>, result: ffi::AsyncCompletionResult| {
            context.0.send(result).expect("should be alive")
        },
        completion_context,
    );

    match result {
        ffi::AsyncFunctionResult::Success => ffi::AsyncCompletionResult::Success,
        ffi::AsyncFunctionResult::CallbackRegistered => rx.await.expect("alive"),
        ffi::AsyncFunctionResult::Retry => todo!(),
        ffi::AsyncFunctionResult {
            repr: 3_u8..=u8::MAX,
        } => todo!(),
    };
}

#[async_trait]
impl AsyncEmitter for Emitter {
    async fn eos(&mut self) {
        async_operation(move |done, ctx| unsafe {
            ffi::source_on_eos(self.context.pin_mut_unchecked(), done, ctx)
        })
        .await
    }

    async fn error(&mut self, error_message: String) {
        async_operation(move |done, ctx| unsafe {
            ffi::source_on_error(self.context.pin_mut_unchecked(), error_message, done, ctx)
        })
        .await
    }

    async fn data(&mut self, data: TupleBuffer) -> nes_source_runtime::Result<()> {
        async_operation(move |done, ctx| unsafe {
            ffi::source_on_data(
                self.context.pin_mut_unchecked(),
                data.leak(),
                done,
                ctx,
            )
        })
        .await;
        Ok(())
    }
}

fn source_create_handle(
    query_context: ffi::QueryContext,
    runtime_idx: usize,
    context: SharedPtr<ffi::SourceContext>,
    buffer_provider_handle: SharedPtr<BufferProviderHandle>,
) -> Result<Box<RustSourceHandle>, String> {
    let provider = BufferProvider::from_raw(buffer_provider_handle);
    let runtime = get_runtime(runtime_idx).expect("non existing runtime");
    let config = serde_json::from_str::<ConfigOptions>(&query_context.source_config)
        .expect("FFI serialization error. Could not convert config options to rust representation");

    validate(&query_context.source_type, &config).map_err(|e| e.to_string())?;

    let (controller, signal) = {
        let span = span!(Level::INFO, "Source",  distributed_query_id = %query_context.distributed_query_id, query_id = %query_context.query_id, source_type = %query_context.source_type, source_id = %query_context.source_id);
        let entered_ = span.enter();
        nes_source_runtime::start_source(
            &query_context.source_type,
            query_context.source_id as u64,
            &config,
            runtime,
            Emitter { context },
            provider,
        )?
    };

    Ok(Box::new(RustSourceHandle {
        channel: controller,
        stop_signal: signal,
    }))
}

fn source_stop(handle: &mut RustSourceHandle) -> bool {
    match handle.stop_signal.try_recv() {
        Ok(emitter) => {
            let _ = emitter;
            true
        }
        Err(TryRecvError::Empty) => match handle.channel.try_send(SourceCommand::Stop) {
            // This assumes that the channel is only used for stopping the source. if its full than there is a stop request pending.
            Ok(_) | Err(TrySendError::Full(_)) => false,
            Err(TrySendError::Closed(_)) => {
                let _ = handle
                    .stop_signal
                    .try_recv()
                    .expect("The stop signal should have been received before closing the source");
                true
            }
        },
        Err(TryRecvError::Closed) => {
            panic!("Stop signal should have been send, before closing the source");
        }
    }
}
