// nes_source_runtime_bindings: CXX bridge for source lifecycle management.

use crate::ffi::{AsyncCompletionResult, AsyncFunctionResult, on_data, on_eos, on_error};
use async_trait::async_trait;
use cxx::SharedPtr;
use nes_buffer_bindings::ffi::BufferProviderHandle;
pub use nes_buffer_bindings::*;
use nes_buffer_runtime::{BufferProvider, TupleBuffer};
use nes_io_runtime::get_runtime;
use nes_source_runtime::{AsyncEmitter, SourceCommand, SourceResult};
use nes_source_validation::{ConfigOptions, validate};
use std::ops::Deref;
use std::result;

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

    extern "Rust" {
        type CompletionContext;
    }

    unsafe extern "C++" {
        include!("BufferBindings.hpp");
        include!("SourceRuntimeBindings.hpp");
        #[namespace = "NES::detail"]
        type MemorySegment = nes_buffer_bindings::ffi::MemorySegment;
        type BufferProviderHandle = nes_buffer_bindings::ffi::BufferProviderHandle;
        type SourceContext;
        unsafe fn on_error(context: &SourceContext, error_msg: String);
        unsafe fn on_data(
            context: &SourceContext,
            buffer: *mut MemorySegment,
            done: fn(Box<CompletionContext>, ret: AsyncCompletionResult),
            ctx: Box<CompletionContext>,
        ) -> AsyncFunctionResult;
        unsafe fn on_eos(context: &SourceContext);
    }
    extern "Rust" {
        type SourceHandle;
        fn create_handle(
            name: String,
            config_json: String,
            runtime_idx: usize,
            context: SharedPtr<SourceContext>,
            buffer_provider_handle: SharedPtr<BufferProviderHandle>,
        ) -> Result<Box<SourceHandle>>;
        fn stop(handle: &SourceHandle);
    }
}

unsafe impl Send for ffi::SourceContext {}
unsafe impl Sync for ffi::SourceContext {}

struct SourceHandle {
    channel: nes_source_runtime::Controller,
}
pub struct CompletionContext(tokio::sync::oneshot::Sender<AsyncCompletionResult>);

struct Emitter {
    context: SharedPtr<ffi::SourceContext>,
}
#[async_trait]
impl AsyncEmitter for Emitter {
    async fn emit(&mut self, result: SourceResult) -> nes_source_runtime::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let completion_context = Box::new(CompletionContext(tx));
        match result {
            SourceResult::Data(data) => unsafe {
                let result = on_data(
                    self.context.deref(),
                    data.leak(),
                    |context: Box<CompletionContext>, result: AsyncCompletionResult| {
                        context.0.send(result).expect("should be alive")
                    },
                    completion_context,
                );
                match result {
                    AsyncFunctionResult::Success => AsyncCompletionResult::Success,
                    AsyncFunctionResult::CallbackRegistered => rx.await.expect("alive"),
                    AsyncFunctionResult::Retry => todo!(),
                    AsyncFunctionResult {
                        repr: 3_u8..=u8::MAX,
                    } => todo!(),
                };
            },
            SourceResult::Error(msg) => unsafe { on_error(self.context.deref(), msg) },
            SourceResult::EoS => unsafe { on_eos(self.context.deref()) },
        };
        Ok(())
    }
}

fn create_handle(
    name: String,
    config_json: String,
    runtime_idx: usize,
    context: SharedPtr<ffi::SourceContext>,
    buffer_provider_handle: SharedPtr<BufferProviderHandle>,
) -> Result<Box<SourceHandle>, String> {
    let provider = BufferProvider::from_raw(buffer_provider_handle);
    let runtime = get_runtime(runtime_idx).expect("non existing runtime");
    let config = serde_json::from_str::<ConfigOptions>(&config_json)
        .expect("FFI serialization error. Could not convert config options to rust representation");

    validate(&name, &config).map_err(|e| e.to_string())?;
    let controller =
        nes_source_runtime::start_source(&name, &config, runtime, Emitter { context }, provider)?;
    Ok(Box::new(SourceHandle {
        channel: controller,
    }))
}

fn stop(handle: &SourceHandle) {
    let _ = handle.channel.send(SourceCommand::Stop);
}
