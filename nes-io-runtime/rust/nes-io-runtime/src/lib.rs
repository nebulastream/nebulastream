use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use tokio::runtime::Runtime;

static IO_RUNTIMES: LazyLock<Mutex<(usize, HashMap<usize, Arc<IORuntime>>)>> =
    LazyLock::new(|| Default::default());

pub struct IORuntime {
    pub runtime: Runtime,
}

pub fn init_io_runtime(
    thread_count: u32,
    on_thread_start: impl Fn() + Send + Sync + 'static,
) -> Result<usize, String> {
    let mut guard = IO_RUNTIMES.lock().unwrap();

    let id = guard.0;
    guard.0 += 1;
    guard.1.insert(
        id,
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_count as usize)
            .thread_name("nes-io-rt")
            .on_thread_start(on_thread_start)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .map(|runtime| Arc::new(IORuntime { runtime }))
            .map_err(|e| e.to_string())?,
    );
    Ok(id)
}

pub fn shutdown_runtime(runtime_idx: usize) {
    let mut guard = IO_RUNTIMES.lock().unwrap();
    guard
        .1
        .remove(&runtime_idx)
        .expect("shutdown on non existing runtime");
}

pub fn get_runtime(idx: usize) -> Option<Arc<IORuntime>> {
    let guard = IO_RUNTIMES.lock().unwrap();
    guard.1.get(&idx).cloned()
}
