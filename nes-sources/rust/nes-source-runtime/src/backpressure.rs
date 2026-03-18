// Backpressure translation: C++ BackpressureController signals -> async-aware yields.
//
// BackpressureFuture uses AtomicWaker + AtomicBool to translate synchronous C++
// backpressure signals into async Rust futures. When C++ applies backpressure,
// the source's emit() call yields without blocking a Tokio worker thread. When
// C++ releases backpressure, the future is woken and the source resumes.
//
// A global DashMap registry stores per-source BackpressureState instances,
// keyed by source_id. C++ callbacks dispatch to the correct source via this map.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use dashmap::DashMap;

/// Global registry of per-source backpressure state.
///
/// Keyed by source_id (u64). Each source registers its BackpressureState when
/// spawned and unregisters on shutdown. C++ callbacks use this map to dispatch
/// backpressure signals to the correct source.
static BACKPRESSURE_REGISTRY: LazyLock<DashMap<u64, Arc<BackpressureState>>> =
    LazyLock::new(DashMap::new);

/// Per-source backpressure state.
///
/// Contains an `AtomicWaker` for thread-safe waker storage and an `AtomicBool`
/// flag indicating whether backpressure is released (source can proceed).
///
/// Initial state: `released = true` (no pressure -- source can emit immediately).
pub struct BackpressureState {
    waker: AtomicWaker,
    /// `true` = no pressure (source can proceed), `false` = pressure applied.
    pub(crate) released: AtomicBool,
}

impl BackpressureState {
    /// Create a new BackpressureState with no pressure (released = true).
    pub fn new() -> Self {
        Self {
            waker: AtomicWaker::new(),
            released: AtomicBool::new(true),
        }
    }

    /// Called from C++ via FFI when backpressure is released.
    ///
    /// Sets the released flag and wakes the registered waker (if any).
    pub fn signal_released(&self) {
        self.released.store(true, Ordering::Release);
        self.waker.wake();
    }

    /// Called from C++ via FFI when backpressure is applied.
    ///
    /// Clears the released flag. The next poll of BackpressureFuture will
    /// return Pending.
    pub fn signal_applied(&self) {
        self.released.store(false, Ordering::Release);
    }

    /// Returns a future that resolves when backpressure is released.
    ///
    /// If backpressure is not currently applied, the future resolves
    /// immediately on the first poll.
    pub fn wait_for_release(&self) -> BackpressureFuture<'_> {
        BackpressureFuture { state: self }
    }
}

/// Future that resolves when backpressure is released.
///
/// Poll implementation uses the store-waker-then-check pattern to prevent
/// lost-wake races:
/// 1. Fast path: check released flag, return Ready if true
/// 2. Slow path: register waker via AtomicWaker, re-check flag, return
///    Ready or Pending
pub struct BackpressureFuture<'a> {
    state: &'a BackpressureState,
}

impl<'a> Future for BackpressureFuture<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // Fast path: check if already released (avoids waker registration overhead)
        if self.state.released.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        // Register waker BEFORE checking condition (prevents lost-wake race).
        // If signal_released() fires between register and the re-check below,
        // the waker is still registered, so the wake() call will re-poll us.
        self.state.waker.register(cx.waker());

        // Re-check after registration
        if self.state.released.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// Register a source's backpressure state in the global registry.
///
/// Returns an `Arc<BackpressureState>` that can be stored in the source's
/// context for direct access without registry lookups on the hot path.
pub fn register_source(source_id: u64) -> Arc<BackpressureState> {
    let state = Arc::new(BackpressureState::new());
    BACKPRESSURE_REGISTRY.insert(source_id, state.clone());
    state
}

/// Remove a source's backpressure state from the global registry.
pub fn unregister_source(source_id: u64) {
    BACKPRESSURE_REGISTRY.remove(&source_id);
}

/// Called from C++ when backpressure is released for a specific source.
///
/// Dispatches to the correct source's BackpressureState via the global
/// registry. No-op if the source is not registered (e.g., already shut down).
pub fn on_backpressure_released(source_id: u64) {
    if let Some(state) = BACKPRESSURE_REGISTRY.get(&source_id) {
        state.signal_released();
    }
}

/// Called from C++ when backpressure is applied to a specific source.
///
/// Dispatches to the correct source's BackpressureState via the global
/// registry. No-op if the source is not registered.
pub fn on_backpressure_applied(source_id: u64) {
    if let Some(state) = BACKPRESSURE_REGISTRY.get(&source_id) {
        state.signal_applied();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    /// Minimal waker that does nothing (for manual poll tests).
    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn noop_waker() -> Waker {
        Waker::from(Arc::new(NoopWaker))
    }

    #[test]
    fn backpressure_future_resolves_immediately_when_released() {
        // Initial state: released=true (no pressure)
        let state = BackpressureState::new();
        let mut fut = state.wait_for_release();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(
            std::pin::Pin::new(&mut fut).poll(&mut cx),
            Poll::Ready(())
        );
    }

    #[test]
    fn backpressure_future_pending_when_pressure_applied() {
        let state = BackpressureState::new();
        state.signal_applied();
        let mut fut = state.wait_for_release();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(
            std::pin::Pin::new(&mut fut).poll(&mut cx),
            Poll::Pending
        );
    }

    #[test]
    fn signal_released_wakes_future() {
        let state = Arc::new(BackpressureState::new());
        state.signal_applied();

        let state2 = state.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let handle = tokio::spawn(async move {
                state2.wait_for_release().await;
            });
            // Give the task a chance to park
            tokio::task::yield_now().await;
            // Now release
            state.signal_released();
            handle.await.unwrap();
        });
    }

    #[test]
    fn poll_ordering_prevents_lost_wake() {
        // Simulate: signal_released happens between register and re-check.
        // The future must still return Ready, not Pending.
        let state = BackpressureState::new();
        state.signal_applied();

        // First poll -> registers waker, returns Pending
        let mut fut = state.wait_for_release();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(std::pin::Pin::new(&mut fut).poll(&mut cx), Poll::Pending);

        // Release happens (simulating C++ callback between polls)
        state.signal_released();

        // Second poll -> should see released=true and return Ready
        assert_eq!(std::pin::Pin::new(&mut fut).poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn backpressure_state_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BackpressureState>();
    }

    #[test]
    fn register_and_unregister_source() {
        let id = 42u64;
        let state = register_source(id);
        assert!(BACKPRESSURE_REGISTRY.contains_key(&id));
        // Returned Arc should match the one in the registry
        assert!(Arc::ptr_eq(&state, &BACKPRESSURE_REGISTRY.get(&id).unwrap()));
        unregister_source(id);
        assert!(!BACKPRESSURE_REGISTRY.contains_key(&id));
    }

    #[test]
    fn on_backpressure_callbacks_dispatch_to_correct_source() {
        let id_a = 100u64;
        let id_b = 200u64;
        let state_a = register_source(id_a);
        let state_b = register_source(id_b);

        // Apply pressure to source A only
        on_backpressure_applied(id_a);

        // Source A should be under pressure, B should not
        assert!(!state_a.released.load(std::sync::atomic::Ordering::Acquire));
        assert!(state_b.released.load(std::sync::atomic::Ordering::Acquire));

        // Release source A
        on_backpressure_released(id_a);
        assert!(state_a.released.load(std::sync::atomic::Ordering::Acquire));

        // Cleanup
        unregister_source(id_a);
        unregister_source(id_b);
    }
}
