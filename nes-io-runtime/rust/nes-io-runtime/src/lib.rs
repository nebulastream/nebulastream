use std::cell::RefCell;
use std::sync::{Arc, Weak};
use tokio::runtime::{Handle, Runtime};

mod configs;
mod services;

pub use configs::ConfigRegistry;
pub use services::ServiceRegistry;

thread_local! {
    static CURRENT: RefCell<Option<Weak<IORuntimeInner>>> = const { RefCell::new(None) };
}

/// Internal Tokio runtime + per-runtime registries. Always accessed through
/// the [`IORuntime`] handle, which holds an `Arc<IORuntimeInner>`.
struct IORuntimeInner {
    runtime: Runtime,
    services: Arc<ServiceRegistry>,
    configs: Arc<ConfigRegistry>,
}

/// Owning handle to an IO runtime — the only public way to refer to one.
///
/// Cheap to clone (it's an `Arc` underneath). C++ holds it as
/// `rust::Box<IORuntimeHandle>` (see the `nes_io_runtime_bindings` cxx bridge);
/// Rust code that needs shared ownership clones the handle. The underlying
/// Tokio runtime and registries live as long as the last `IORuntime` clone.
#[derive(Clone)]
pub struct IORuntime {
    inner: Arc<IORuntimeInner>,
}

impl IORuntime {
    pub fn services(&self) -> &Arc<ServiceRegistry> {
        &self.inner.services
    }

    pub fn configs(&self) -> &Arc<ConfigRegistry> {
        &self.inner.configs
    }

    /// Tokio handle for spawning tasks on this runtime. Cheap to clone.
    pub fn handle(&self) -> Handle {
        self.inner.runtime.handle().clone()
    }
}

/// Returns the `IORuntime` whose worker thread is currently executing.
///
/// Panics when called from a thread that is not an `IORuntime` worker thread.
pub fn current_io_runtime() -> IORuntime {
    try_current_io_runtime()
        .expect("current_io_runtime() called outside of an IORuntime worker thread")
}

/// Like [`current_io_runtime`] but returns `None` instead of panicking.
pub fn try_current_io_runtime() -> Option<IORuntime> {
    CURRENT.with(|c| {
        c.borrow()
            .as_ref()
            .and_then(Weak::upgrade)
            .map(|inner| IORuntime { inner })
    })
}

pub fn init_io_runtime(
    thread_count: u32,
    on_thread_start: impl Fn() + Send + Sync + 'static,
) -> Result<IORuntime, String> {
    let build_err: Arc<std::sync::Mutex<Option<String>>> =
        Arc::new(std::sync::Mutex::new(None));

    let inner: Arc<IORuntimeInner> = {
        let build_err = build_err.clone();
        Arc::new_cyclic(move |weak: &Weak<IORuntimeInner>| {
            let weak_for_threads = weak.clone();
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(thread_count as usize)
                .thread_name("nes-io-rt")
                .on_thread_start(move || {
                    let weak = weak_for_threads.clone();
                    CURRENT.with(|c| *c.borrow_mut() = Some(weak));
                    on_thread_start();
                })
                .max_blocking_threads(1)
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    *build_err.lock().unwrap() = Some(e.to_string());
                    // Must return *something* from new_cyclic. The caller checks
                    // build_err and discards this Arc immediately.
                    tokio::runtime::Builder::new_current_thread()
                        .build()
                        .expect("fallback current-thread runtime must build")
                }
            };
            IORuntimeInner {
                runtime,
                services: Arc::new(ServiceRegistry::default()),
                configs: Arc::new(ConfigRegistry::default()),
            }
        })
    };

    if let Some(err) = build_err.lock().unwrap().take() {
        return Err(err);
    }

    Ok(IORuntime { inner })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_io_runtime_set_on_worker_thread() {
        let rt = init_io_runtime(2, || {}).expect("build runtime");
        let observed = rt.handle().block_on(async {
            tokio::task::spawn(async { current_io_runtime() })
                .await
                .expect("spawn ok")
        });
        assert!(Arc::ptr_eq(&rt.inner, &observed.inner));
    }

    #[test]
    fn try_current_io_runtime_none_outside_worker() {
        assert!(try_current_io_runtime().is_none());
    }

    #[test]
    fn services_registry_accessible_from_worker() {
        let rt = init_io_runtime(1, || {}).expect("build runtime");
        let inserted = rt.handle().block_on(async {
            tokio::task::spawn(async {
                let r = current_io_runtime();
                let svc = Arc::new(42u32);
                r.services().insert::<u32>(svc.clone());
                let got = r.services().get::<u32>().expect("present");
                Arc::ptr_eq(&svc, &got)
            })
            .await
            .unwrap()
        });
        assert!(inserted);
    }

    #[test]
    fn dropping_last_runtime_clone_tears_down() {
        let rt = init_io_runtime(1, || {}).expect("build runtime");
        let weak = Arc::downgrade(&rt.inner);
        drop(rt);
        assert!(weak.upgrade().is_none());
    }
}
