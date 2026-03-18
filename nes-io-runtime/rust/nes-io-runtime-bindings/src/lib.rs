use crate::ffi::{ThreadInitializationContext, init_thread};
use cxx::SharedPtr;

#[cxx::bridge]
pub mod ffi {
    unsafe extern "C++" {
        include!("IORuntimeBindings.hpp");
        type ThreadInitializationContext;
        unsafe fn init_thread(context: SharedPtr<ThreadInitializationContext>);
    }
    extern "Rust" {
        fn init_io_runtime(
            thread_count: u32,
            thread_initialization_context: SharedPtr<ThreadInitializationContext>,
        ) -> Result<usize>;

        fn shutdown_runtime(runtime_idx: usize);
    }
}

unsafe impl Send for ThreadInitializationContext {}
unsafe impl Sync for ThreadInitializationContext {}

fn init_io_runtime(
    thread_count: u32,
    thread_initialization_context: SharedPtr<ThreadInitializationContext>,
) -> Result<usize, String> {
    nes_io_runtime::init_io_runtime(thread_count, move || unsafe {
        init_thread(thread_initialization_context.clone())
    })
}

fn shutdown_runtime(runtime_idx: usize) {
    nes_io_runtime::shutdown_runtime(runtime_idx)
}
