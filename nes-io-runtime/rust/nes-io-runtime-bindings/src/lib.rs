use crate::ffi::{ThreadInitializationContext, init_thread};
use cxx::SharedPtr;

/// Local newtype around [`nes_io_runtime::IORuntime`] so it can be exposed as
/// a cxx-opaque type from this bridge. cxx (orphan rules) requires the type
/// declared in `extern "Rust"` to live in the bridge's own crate.
///
/// `repr(transparent)` so the address of an `IORuntime` is exactly the
/// address of the inner `nes_io_runtime::IORuntime` — sink/source FFI bridges
/// in other crates can take a raw pointer (`*const nes_io_runtime::IORuntime`)
/// and the C++ side hands them `&*ioruntime_box` cast to that pointer.
#[repr(transparent)]
pub struct IORuntime(pub nes_io_runtime::IORuntime);

#[cxx::bridge]
pub mod ffi {
    unsafe extern "C++" {
        include!("IORuntimeBindings.hpp");
        type ThreadInitializationContext;
        unsafe fn init_thread(context: SharedPtr<ThreadInitializationContext>);
    }
    extern "Rust" {
        /// Opaque handle to a Tokio IO runtime + its per-runtime registries.
        /// C++ holds it as `rust::Box<IORuntimeHandle>`.
        #[cxx_name = "IORuntimeHandle"]
        type IORuntime;

        fn init_io_runtime(
            thread_count: u32,
            thread_initialization_context: SharedPtr<ThreadInitializationContext>,
        ) -> Result<Box<IORuntime>>;

        /// Attach an opaque config blob to the IORuntime under `name`.
        /// Services constructed on the runtime read these blobs (e.g., as JSON)
        /// when they're built on first use.
        fn attach_config(runtime: &IORuntime, name: &str, config: &str) -> Result<()>;
    }
}

unsafe impl Send for ThreadInitializationContext {}
unsafe impl Sync for ThreadInitializationContext {}

fn init_io_runtime(
    thread_count: u32,
    thread_initialization_context: SharedPtr<ThreadInitializationContext>,
) -> Result<Box<IORuntime>, String> {
    nes_io_runtime::init_io_runtime(thread_count, move || unsafe {
        init_thread(thread_initialization_context.clone())
    })
    .map(|inner| Box::new(IORuntime(inner)))
}

fn attach_config(runtime: &IORuntime, name: &str, config: &str) -> Result<(), String> {
    runtime
        .0
        .configs()
        .attach(name.to_string(), config.to_string());
    Ok(())
}

unsafe extern "C" {
    fn current_io_runtime_internal() -> *const IORuntime;
}
pub fn current_io_runtime() -> Option<nes_io_runtime::IORuntime> {
    unsafe { current_io_runtime_internal().as_ref() }.map(|handle| handle.0.clone())
}
