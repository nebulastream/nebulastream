// nes_source_runtime_bindings: CXX bridge for source lifecycle management.

use crate::ffi::{on_data, on_eos, on_error};
use cxx::SharedPtr;
use nes_buffer_bindings::ffi::BufferProviderHandle;
pub use nes_buffer_bindings::*;
use nes_buffer_runtime::{BufferProvider, TupleBuffer};
use nes_io_runtime::get_runtime;
use nes_source_runtime::{SourceCommand, SourceResult};
use nes_source_validation::{ConfigOptions, validate};
use std::ops::Deref;

#[cxx::bridge]
pub mod ffi {
    unsafe extern "C++" {
        include!("BufferBindings.hpp");
        include!("SourceRuntimeBindings.hpp");
        #[namespace = "NES::detail"]
        type MemorySegment = nes_buffer_bindings::ffi::MemorySegment;
        type BufferProviderHandle = nes_buffer_bindings::ffi::BufferProviderHandle;
        type SourceContext;
        unsafe fn on_error(context: &SourceContext, error_msg: String);
        unsafe fn on_data(context: &SourceContext, buffer: *mut MemorySegment);
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
    let controller = nes_source_runtime::start_source(
        &name,
        &config,
        runtime,
        Box::new(move |result: SourceResult| match result {
            SourceResult::Data(data) => unsafe { on_data(context.deref(), data.leak()) },
            SourceResult::Error(msg) => unsafe { on_error(context.deref(), msg) },
            SourceResult::EoS => unsafe { on_eos(context.deref()) },
        }),
        provider,
    )?;
    Ok(Box::new(SourceHandle {
        channel: controller,
    }))
}

fn stop(handle: &SourceHandle) {
    let _ = handle.channel.send(SourceCommand::Stop);
}
