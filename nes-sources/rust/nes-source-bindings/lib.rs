#[cxx::bridge]
pub mod ffi {
    unsafe extern "C++" {
        include!("IoBindings.hpp");

        type TupleBuffer;

        #[allow(non_snake_case)]
        fn retain(buf: Pin<&mut TupleBuffer>);
        #[allow(non_snake_case)]
        fn release(buf: Pin<&mut TupleBuffer>);
        #[allow(non_snake_case)]
        fn getDataPtr(buf: &TupleBuffer) -> *const u8;
        #[allow(non_snake_case)]
        fn getDataPtrMut(buf: Pin<&mut TupleBuffer>) -> *mut u8;
        #[allow(non_snake_case)]
        fn getCapacity(buf: &TupleBuffer) -> u64;
        #[allow(non_snake_case)]
        fn getNumberOfTuples(buf: &TupleBuffer) -> u64;
        #[allow(non_snake_case)]
        fn setNumberOfTuples(buf: Pin<&mut TupleBuffer>, count: u64);
        #[allow(non_snake_case)]
        fn getReferenceCounter(buf: &TupleBuffer) -> u32;
        #[allow(non_snake_case)]
        fn cloneTupleBuffer(buf: &TupleBuffer) -> UniquePtr<TupleBuffer>;

        type AbstractBufferProvider;

        #[allow(non_snake_case)]
        fn getBufferBlocking(provider: Pin<&mut AbstractBufferProvider>) -> UniquePtr<TupleBuffer>;
        #[allow(non_snake_case)]
        fn tryGetBuffer(provider: Pin<&mut AbstractBufferProvider>) -> UniquePtr<TupleBuffer>;
        #[allow(non_snake_case)]
        fn getBufferSize(provider: &AbstractBufferProvider) -> u64;
        #[allow(non_snake_case)]
        fn installBufferRecycleNotification();

        type BackpressureListener;

        #[allow(non_snake_case)]
        fn waitForBackpressure(listener: &BackpressureListener);
    }

    extern "Rust" {
        fn init_source_runtime(thread_count: u32) -> Result<()>;
    }
}

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::runtime::Runtime;
use tracing::warn;

/// Process-global Tokio runtime for all Rust sources.
static SOURCE_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Tracks how many times init_source_runtime has been called.
static INIT_COUNT: AtomicU32 = AtomicU32::new(0);

/// Initialize the shared source Tokio runtime.
///
/// Creates a multi-thread runtime with `thread_count` worker threads named
/// "nes-source-rt". Returns `Ok(())` on success. The `Result` return type
/// is required by the cxx bridge so that any panic during runtime creation
/// is caught by cxx and converted to a C++ exception instead of aborting
/// the process (FFI-03).
///
/// If called more than once, subsequent calls are no-ops that log a warning.
pub fn init_source_runtime(thread_count: u32) -> Result<(), String> {
    let call_number = INIT_COUNT.fetch_add(1, Ordering::SeqCst);
    SOURCE_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_count as usize)
            .thread_name("nes-source-rt")
            .enable_all()
            .build()
            .expect("Failed to create source Tokio runtime")
    });
    if call_number > 0 {
        warn!(
            requested_threads = thread_count,
            "Source runtime already initialized, ignoring duplicate init call"
        );
    }
    Ok(())
}

/// Get a reference to the shared source Tokio runtime.
///
/// # Panics
/// Panics if called before `init_source_runtime()`.
pub fn source_runtime() -> &'static Runtime {
    SOURCE_RUNTIME
        .get()
        .expect("Source runtime not initialized. Call init_source_runtime() first.")
}
