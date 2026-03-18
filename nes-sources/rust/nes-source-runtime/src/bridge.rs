// Bridge thread: single shared thread bridging async Tokio tasks to sync C++ emit calls.
//
// The bridge receives BridgeMessages via a bounded async_channel, each carrying
// an origin_id, TupleBufferHandle, and OwnedSemaphorePermit. It dispatches each
// message to the correct per-source emit callback via the EMIT_REGISTRY (DashMap).
//
// The bridge thread is lazily initialized via OnceLock on the first call to
// ensure_bridge(). All sources share one channel and one thread.
//
// NOTE on testing: bridge_loop and BridgeMessage involve TupleBufferHandle which
// requires C++ linker symbols (CXX bridge). Pure Rust unit tests use
// TestBridgeMessage + test_bridge_loop to validate the identical dispatch and
// semaphore logic without the C++ dependency. Full integration tests run under
// CMake where the C++ library is linked.

use std::sync::LazyLock;
#[cfg(not(test))]
use std::sync::OnceLock;

use dashmap::DashMap;

use crate::buffer::TupleBufferHandle;

/// C-style emit function pointer type for FFI callbacks.
///
/// Matches the C++ `extern "C"` emit function signature:
/// - `ctx`: opaque pointer to EmitContext (owned by C++ TokioSource)
/// - `origin_id`: identifies which source produced this buffer
/// - `buffer`: raw pointer to the TupleBuffer (bridge transfers ownership via retain)
///
/// Returns: 0 on success, non-zero on error.
pub type EmitFnPtr =
    unsafe extern "C" fn(ctx: *mut std::ffi::c_void, origin_id: u64, buffer: *mut nes_io_bindings::ffi::TupleBuffer, semaphore_ptr: usize) -> u8;

/// Per-source emit callback entry in the global registry.
///
/// Stores the C-style function pointer and its void* context. The context
/// pointer is guaranteed valid for the source's lifetime by C++ (EmitContext
/// is owned by TokioSource which outlives the Rust source task).
pub struct EmitEntry {
    pub(crate) emit_fn: EmitFnPtr,
    pub(crate) emit_ctx: *mut std::ffi::c_void,
}

// SAFETY: The emit_ctx pointer is guaranteed valid for the source's lifetime
// by the C++ TokioSource. The EmitEntry is only accessed from the bridge
// thread (single reader) and the register/unregister calls (which synchronize
// via DashMap).
unsafe impl Send for EmitEntry {}
unsafe impl Sync for EmitEntry {}

/// Global registry mapping source_id -> emit callback entry.
///
/// Each source registers its EmitFunction + context when spawned. The bridge
/// thread looks up the correct callback for each message by origin_id.
/// Using DashMap for concurrent access from registration (Tokio tasks) and
/// lookup (bridge thread).
pub(crate) static EMIT_REGISTRY: LazyLock<DashMap<u64, EmitEntry>> =
    LazyLock::new(DashMap::new);

/// Register a source's emit callback in the global registry.
///
/// Must be called before sending any BridgeMessages for this source_id.
pub fn register_emit(source_id: u64, emit_fn: EmitFnPtr, emit_ctx: *mut std::ffi::c_void) {
    EMIT_REGISTRY.insert(source_id, EmitEntry { emit_fn, emit_ctx });
}

/// Get a copy of a source's emit entry (for EOS signaling from monitoring task).
pub fn get_emit(source_id: u64) -> Option<EmitEntry> {
    EMIT_REGISTRY.get(&source_id).map(|entry| EmitEntry {
        emit_fn: entry.emit_fn,
        emit_ctx: entry.emit_ctx,
    })
}

/// Remove a source's emit callback from the global registry.
///
/// Called during source shutdown. After this, any remaining messages for this
/// source_id will be logged as warnings and dropped by the bridge thread.
pub fn unregister_emit(source_id: u64) {
    EMIT_REGISTRY.remove(&source_id);
}

/// Message sent through the bridge channel from Tokio tasks to the bridge thread.
///
/// Data messages carry a buffer + semaphore permit. Eos signals end-of-stream
/// for a source — sent by the source task after run() returns, through the same
/// channel as data to guarantee ordering (all data delivered before EOS).
pub enum BridgeMessage {
    /// A data buffer to emit to C++.
    Data {
        /// Identifies which source produced this buffer.
        origin_id: u64,
        /// The filled tuple buffer to emit to C++.
        buffer: TupleBufferHandle,
        /// Arc<Semaphore> raw pointer for inflight tracking.
        /// Created via Arc::into_raw() in emit(). Passed to C++ bridge_emit
        /// which sets it as the onComplete callback for pipeline processing.
        /// Released via nes_release_semaphore_slot when the pipeline is done.
        semaphore_ptr: usize,
    },
    /// End-of-stream signal for a source. Dispatched as bridge_emit(null) to C++.
    /// The bridge thread handles cleanup (unregister emit + backpressure) after
    /// dispatching, since it's the last message for this source in the channel.
    Eos {
        origin_id: u64,
    },
}

/// Handle to the bridge thread and its channel sender.
///
/// Created once via OnceLock on the first call to `ensure_bridge()`.
///
/// Not available in test builds because BridgeMessage contains TupleBufferHandle
/// which requires C++ linker symbols not available in pure Rust unit tests.
#[cfg(not(test))]
pub struct BridgeHandle {
    sender: async_channel::Sender<BridgeMessage>,
    #[allow(dead_code)]
    thread: Option<std::thread::JoinHandle<()>>,
}

// SAFETY: BridgeHandle is Send + Sync because:
// - async_channel::Sender is Send + Sync
// - JoinHandle is Send (wrapped in Option for Drop flexibility)
// The OnceLock ensures single initialization; after that, only the Sender
// is used concurrently (which is designed for that).
#[cfg(not(test))]
unsafe impl Sync for BridgeHandle {}

/// Test-only BridgeHandle stub for compile-time type assertions.
///
/// The real BridgeHandle contains Sender<BridgeMessage> which triggers C++ linker
/// symbols via TupleBufferHandle's Drop impl. This stub provides Send + Sync
/// bounds checking without the linker dependency.
#[cfg(test)]
pub struct BridgeHandle {
    _private: (),
}

#[cfg(test)]
unsafe impl Send for BridgeHandle {}
#[cfg(test)]
unsafe impl Sync for BridgeHandle {}

/// Channel capacity for the bridge. Small fixed size since the channel is
/// primarily a sync/async adapter, not a deep buffer. Backpressure is handled
/// by the semaphore, not channel depth.
pub const BRIDGE_CHANNEL_CAPACITY: usize = 64;

/// Process-global bridge instance, lazily initialized.
///
/// Not available in test builds because BridgeMessage contains TupleBufferHandle
/// which requires C++ linker symbols. Tests use TestBridgeMessage + bridge_loop_for_test.
#[cfg(not(test))]
static BRIDGE: OnceLock<BridgeHandle> = OnceLock::new();

/// Get or create the shared bridge, returning a reference to the channel sender.
///
/// The bridge is initialized on first call: creates a bounded(64) async_channel
/// and spawns the "nes-bridge" thread running `bridge_loop`. Subsequent calls
/// return the existing sender.
///
/// Takes no emit callback parameters -- emit dispatch is handled by EMIT_REGISTRY.
///
/// Not available in test builds -- see module doc comment for rationale.
#[cfg(not(test))]
pub fn ensure_bridge() -> &'static async_channel::Sender<BridgeMessage> {
    let handle = BRIDGE.get_or_init(|| {
        let (sender, receiver) = async_channel::bounded::<BridgeMessage>(BRIDGE_CHANNEL_CAPACITY);
        let thread = std::thread::Builder::new()
            .name("nes-bridge".into())
            .spawn(move || {
                bridge_loop(receiver);
            })
            .expect("Failed to spawn bridge thread");
        BridgeHandle {
            sender,
            thread: Some(thread),
        }
    });
    &handle.sender
}

/// Dispatch a single message to the correct source's emit callback.
///
/// Returns `true` if the source was found and the callback was invoked,
/// `false` if the source was not registered (message dropped with warning).
///
/// Extracted from `bridge_loop` for testability without C++ linker dependencies.
pub(crate) fn dispatch_message(origin_id: u64, buffer_ptr: *mut nes_io_bindings::ffi::TupleBuffer, semaphore_ptr: usize) -> bool {
    if let Some(entry) = EMIT_REGISTRY.get(&origin_id) {
        unsafe {
            (entry.emit_fn)(
                entry.emit_ctx,
                origin_id,
                buffer_ptr,
                semaphore_ptr,
            );
        }
        true
    } else {
        // Source already unregistered (race during shutdown). This is
        // expected and not an error -- just drop the message.
        tracing::warn!(
            origin_id = origin_id,
            "Bridge received message for unregistered source, dropping"
        );
        false
    }
}

/// Main loop of the bridge thread.
///
/// Calls `recv_blocking()` in a loop, dispatching each message to the correct
/// source's emit callback via EMIT_REGISTRY lookup. On channel closure (all
/// senders dropped), the loop exits and the thread terminates.
///
/// Not available in test builds -- see module doc comment for rationale.
#[cfg(not(test))]
pub(crate) fn bridge_loop(receiver: async_channel::Receiver<BridgeMessage>) {
    while let Ok(msg) = receiver.recv_blocking() {
        match msg {
            BridgeMessage::Data { origin_id, mut buffer, semaphore_ptr } => {
                // Extract the raw TupleBuffer pointer from the handle.
                // C++ bridge_emit calls rawBuffer->retain() synchronously during
                // dispatch_message. buffer (TupleBufferHandle) is dropped at end
                // of this arm, calling release(). Net effect: refcount stays correct.
                // C++ owns its copy, Rust releases its copy.
                // semaphore_ptr is passed to C++ which stores it in the onComplete
                // callback — the semaphore slot is released when the pipeline finishes.
                let buffer_ptr = buffer.as_raw_ptr();
                dispatch_message(origin_id, buffer_ptr, semaphore_ptr);
                // buffer is dropped here, releasing C++ refcount via release().
            }
            BridgeMessage::Eos { origin_id } => {
                // Dispatch EOS to C++ (bridge_emit with null buffer, no semaphore).
                // This is the last message for this source — all data was
                // processed before this point (FIFO channel ordering).
                dispatch_message(origin_id, std::ptr::null_mut(), 0);
                // Cleanup: unregister emit callback and backpressure state.
                // Safe because no more messages for this source will arrive.
                unregister_emit(origin_id);
                crate::backpressure::unregister_source(origin_id);
                tracing::info!(origin_id = origin_id, "Bridge: EOS processed, cleanup complete");
            }
        }
    }
    // Channel closed -- all senders dropped. Bridge shuts down cleanly.
    tracing::info!("Bridge thread exiting: channel closed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    // ---- Compile-time type checks ----
    //
    // These tests use function signatures that are verified at compile time
    // without instantiating types that require C++ linker symbols.

    #[test]
    fn bridge_message_data_has_expected_field_types() {
        // BridgeMessage::Data must carry origin_id (u64), buffer (TupleBufferHandle),
        // and semaphore_ptr (usize). We verify field types at compile time.
        fn _assert_field_types(msg: &BridgeMessage) {
            match msg {
                BridgeMessage::Data { origin_id, buffer, semaphore_ptr } => {
                    let _origin_id: &u64 = origin_id;
                    let _buffer: &crate::buffer::TupleBufferHandle = buffer;
                    let _semaphore_ptr: &usize = semaphore_ptr;
                }
                BridgeMessage::Eos { origin_id } => {
                    let _origin_id: &u64 = origin_id;
                }
            }
        }
    }

    #[test]
    fn bridge_handle_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BridgeHandle>();
    }

    // ---- Functional tests ----
    //
    // These tests use TestBridgeMessage (no TupleBufferHandle) to avoid C++
    // linker dependencies. The dispatch logic is identical -- it goes through
    // dispatch_message and EMIT_REGISTRY the same way as bridge_loop.
    //
    // TestBridgeMessage mirrors BridgeMessage but omits the TupleBufferHandle
    // field. The bridge_loop_for_test function provides the same recv_blocking
    // + dispatch + drop pattern.

    /// Test-only message type that avoids TupleBufferHandle (no C++ linker needed).
    struct TestBridgeMessage {
        origin_id: u64,
        _permit: tokio::sync::OwnedSemaphorePermit,
    }

    /// Test-only bridge loop that mirrors bridge_loop behavior.
    fn bridge_loop_for_test(receiver: async_channel::Receiver<TestBridgeMessage>) {
        while let Ok(msg) = receiver.recv_blocking() {
            dispatch_message(msg.origin_id, std::ptr::null_mut(), 0);
            // msg._permit is dropped here, releasing the semaphore slot.
        }
    }

    /// Helper: create a mock EmitEntry that records origin_id calls into a shared vec.
    fn mock_emit_entry(log: Arc<Mutex<Vec<u64>>>) -> EmitEntry {
        let ctx = Box::into_raw(Box::new(log)) as *mut std::ffi::c_void;
        EmitEntry {
            emit_fn: mock_emit_callback,
            emit_ctx: ctx,
        }
    }

    /// C-style callback that records origin_id into the log stored in ctx.
    unsafe extern "C" fn mock_emit_callback(
        ctx: *mut std::ffi::c_void,
        origin_id: u64,
        _buffer: *mut nes_io_bindings::ffi::TupleBuffer,
        _semaphore_ptr: usize,
    ) -> u8 {
        // SAFETY: ctx was created from Box::into_raw(Box::new(Arc<Mutex<Vec>>))
        let log = unsafe { &*(ctx as *const Arc<Mutex<Vec<u64>>>) };
        log.lock().unwrap().push(origin_id);
        0 // success
    }

    /// Clean up mock context (must be called to avoid leak in tests).
    unsafe fn cleanup_mock_ctx(entry: &EmitEntry) {
        let _ = unsafe { Box::from_raw(entry.emit_ctx as *mut Arc<Mutex<Vec<u64>>>) };
    }

    #[test]
    fn sender_is_cloneable() {
        // Verify async_channel::Sender is Clone -- compile-time check.
        // Uses TestBridgeMessage to avoid CXX linker symbols.
        let (sender, _receiver) = async_channel::bounded::<TestBridgeMessage>(64);
        let _clone = sender.clone();
    }

    #[tokio::test]
    async fn bridge_processes_messages_in_order() {
        let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let entry = mock_emit_entry(log.clone());

        let (sender, receiver) = async_channel::bounded::<TestBridgeMessage>(64);

        let source_id = 9001u64;
        EMIT_REGISTRY.insert(source_id, entry);

        let handle = std::thread::Builder::new()
            .name("test-bridge".into())
            .spawn(move || bridge_loop_for_test(receiver))
            .unwrap();

        let sem = Arc::new(tokio::sync::Semaphore::new(10));

        for _i in 0..3u64 {
            let permit = sem.clone().acquire_owned().await.unwrap();
            sender.send(TestBridgeMessage {
                origin_id: source_id,
                _permit: permit,
            }).await.unwrap();
        }

        sender.close();
        handle.join().unwrap();

        let calls = log.lock().unwrap();
        assert_eq!(calls.len(), 3);
        assert!(calls.iter().all(|&id| id == source_id));

        let entry = EMIT_REGISTRY.remove(&source_id).unwrap().1;
        unsafe { cleanup_mock_ctx(&entry) };
    }

    #[tokio::test]
    async fn semaphore_permit_released_after_processing() {
        let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let entry = mock_emit_entry(log.clone());

        let (sender, receiver) = async_channel::bounded::<TestBridgeMessage>(64);
        let source_id = 9002u64;
        EMIT_REGISTRY.insert(source_id, entry);

        let handle = std::thread::Builder::new()
            .name("test-bridge-permit".into())
            .spawn(move || bridge_loop_for_test(receiver))
            .unwrap();

        let sem = Arc::new(tokio::sync::Semaphore::new(1));

        let permit = sem.clone().acquire_owned().await.unwrap();
        assert_eq!(sem.available_permits(), 0);

        sender.send(TestBridgeMessage {
            origin_id: source_id,
            _permit: permit,
        }).await.unwrap();

        sender.close();
        handle.join().unwrap();

        // After processing, the permit should be released
        assert_eq!(sem.available_permits(), 1);

        let entry = EMIT_REGISTRY.remove(&source_id).unwrap().1;
        unsafe { cleanup_mock_ctx(&entry) };
    }

    #[test]
    fn channel_closes_when_senders_dropped() {
        let (sender, receiver) = async_channel::bounded::<TestBridgeMessage>(64);
        drop(sender);
        assert!(receiver.recv_blocking().is_err());
    }

    #[tokio::test]
    async fn multi_source_dispatch() {
        let log_a: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let log_b: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let entry_a = mock_emit_entry(log_a.clone());
        let entry_b = mock_emit_entry(log_b.clone());

        let source_a = 7001u64;
        let source_b = 7002u64;
        EMIT_REGISTRY.insert(source_a, entry_a);
        EMIT_REGISTRY.insert(source_b, entry_b);

        let (sender, receiver) = async_channel::bounded::<TestBridgeMessage>(64);
        let handle = std::thread::Builder::new()
            .name("test-bridge-multi".into())
            .spawn(move || bridge_loop_for_test(receiver))
            .unwrap();

        let sem = Arc::new(tokio::sync::Semaphore::new(10));

        // Interleave messages from both sources: A, B, A
        for &src in &[source_a, source_b, source_a] {
            let permit = sem.clone().acquire_owned().await.unwrap();
            sender.send(TestBridgeMessage {
                origin_id: src,
                _permit: permit,
            }).await.unwrap();
        }

        sender.close();
        handle.join().unwrap();

        let calls_a = log_a.lock().unwrap();
        let calls_b = log_b.lock().unwrap();
        assert_eq!(calls_a.len(), 2, "Source A should receive 2 messages");
        assert_eq!(calls_b.len(), 1, "Source B should receive 1 message");

        let entry_a = EMIT_REGISTRY.remove(&source_a).unwrap().1;
        let entry_b = EMIT_REGISTRY.remove(&source_b).unwrap().1;
        unsafe {
            cleanup_mock_ctx(&entry_a);
            cleanup_mock_ctx(&entry_b);
        }
    }

    #[tokio::test]
    async fn unregistered_source_message_logged_not_panicked() {
        let (sender, receiver) = async_channel::bounded::<TestBridgeMessage>(64);
        let handle = std::thread::Builder::new()
            .name("test-bridge-unreg".into())
            .spawn(move || bridge_loop_for_test(receiver))
            .unwrap();

        let sem = Arc::new(tokio::sync::Semaphore::new(10));
        let unregistered_id = 99999u64;

        let permit = sem.clone().acquire_owned().await.unwrap();
        sender.send(TestBridgeMessage {
            origin_id: unregistered_id,
            _permit: permit,
        }).await.unwrap();

        sender.close();
        // This should NOT panic
        handle.join().unwrap();

        // Permit should still be released (message was dropped)
        assert_eq!(sem.available_permits(), 10);
    }

    #[test]
    fn dispatch_message_returns_true_for_registered_source() {
        let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let entry = mock_emit_entry(log.clone());
        let source_id = 5001u64;
        EMIT_REGISTRY.insert(source_id, entry);

        assert!(dispatch_message(source_id, std::ptr::null_mut(), 0));

        let calls = log.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], source_id);

        let entry = EMIT_REGISTRY.remove(&source_id).unwrap().1;
        unsafe { cleanup_mock_ctx(&entry) };
    }

    #[test]
    fn dispatch_message_returns_false_for_unregistered_source() {
        assert!(!dispatch_message(88888, std::ptr::null_mut(), 0));
    }
}
