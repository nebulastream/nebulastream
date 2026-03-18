use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::runtime::Runtime;
use tracing::warn;

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn init_io_runtime(thread_count: u32) -> Result<()>;
    }
}

static IO_RUNTIME: OnceLock<Runtime> = OnceLock::new();
static INIT_COUNT: AtomicU32 = AtomicU32::new(0);

pub fn init_io_runtime(thread_count: u32) -> Result<(), String> {
    let call_number = INIT_COUNT.fetch_add(1, Ordering::SeqCst);
    IO_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_count as usize)
            .thread_name("nes-io-rt")
            .enable_all()
            .build()
            .expect("Failed to create IO Tokio runtime")
    });
    if call_number > 0 {
        warn!(
            requested_threads = thread_count,
            "IO runtime already initialized, ignoring duplicate init call"
        );
    }
    Ok(())
}

pub fn io_runtime() -> &'static Runtime {
    IO_RUNTIME
        .get()
        .expect("IO runtime not initialized. Call init_io_runtime() first.")
}
