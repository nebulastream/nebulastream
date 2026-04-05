// Bridge thread: single shared thread bridging async Tokio tasks to sync C++ emit calls.
//
// The bridge receives BridgeMessages via a bounded async_channel, each carrying
// the emit callback, origin_id, TupleBufferHandle, and semaphore pointer. It
// invokes the per-source C++ emit callback directly from the message — no global
// registry lookup needed.
//
// The bridge thread is lazily initialized via OnceLock on the first call to
// ensure_bridge(). All sources share one channel and one thread.
//
// NOTE on testing: bridge_loop and BridgeMessage involve TupleBufferHandle which
// requires C++ linker symbols (CXX bridge). Pure Rust unit tests use
// TestBridgeMessage + test_bridge_loop to validate the identical dispatch and
// semaphore logic without the C++ dependency. Full integration tests run under
// CMake where the C++ library is linked.

#[cfg(not(test))]
use std::sync::OnceLock;

use crate::buffer::TupleBufferHandle;

/// C-style emit function pointer type for FFI callbacks.
///
/// Matches the C++ `extern "C"` emit function signature:
/// - `ctx`: opaque pointer to EmitContext (owned by C++ TokioSource)
/// - `origin_id`: identifies which source produced this buffer
/// - `buffer`: raw pointer to the TupleBuffer (bridge transfers ownership via retain)
/// - `semaphore_ptr`: Arc<Semaphore> raw pointer for inflight tracking
///
/// Returns: 0 on success, non-zero on error.
pub type EmitFnPtr =
    unsafe extern "C" fn(ctx: *mut std::ffi::c_void, origin_id: u64, buffer: *mut nes_buffer_bindings::ffi::TupleBuffer, semaphore_ptr: usize) -> u8;

/// Per-source emit callback info carried in each BridgeMessage.
///
/// The emit_ctx pointer is guaranteed valid for the source's lifetime by C++
/// (EmitContext is owned by TokioSource which outlives the Rust source task).
#[derive(Clone, Copy)]
pub struct EmitCallback {
    pub(crate) emit_fn: EmitFnPtr,
    pub(crate) emit_ctx: *mut std::ffi::c_void,
}

// SAFETY: The emit_ctx pointer is guaranteed valid for the source's lifetime
// by the C++ TokioSource. EmitCallback is only invoked from the bridge thread
// (single reader) after being sent through the channel.
unsafe impl Send for EmitCallback {}
unsafe impl Sync for EmitCallback {}

/// Message sent through the bridge channel from Tokio tasks to the bridge thread.
///
/// Data messages carry a buffer + semaphore permit. Eos signals end-of-stream
/// for a source — sent by the source task after run() returns, through the same
/// channel as data to guarantee ordering (all data delivered before EOS).
pub enum BridgeMessage {
    /// A data buffer to emit to C++.
    Data {
        /// The C++ emit callback for this source.
        callback: EmitCallback,
        /// Identifies which source produced this buffer (for C++ pipeline routing).
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
    Eos {
        /// The C++ emit callback for this source.
        callback: EmitCallback,
        /// Identifies which source (for C++ pipeline routing).
        origin_id: u64,
    },
}

/// Bridge channel handle: sender + thread join handle.
///
/// Created once by `ensure_bridge()`. The sender is cloned into each
/// SourceContext. The thread runs until the last sender is dropped.
struct BridgeHandle {
    sender: async_channel::Sender<BridgeMessage>,
    // Thread handle kept alive so the bridge thread isn't detached.
    // It will exit when all senders are dropped (channel closes).
    _thread: std::thread::JoinHandle<()>,
}

/// Lazily-initialized bridge singleton.
///
/// ensure_bridge() creates the channel + thread on first call, returns the
/// sender on all subsequent calls.
#[cfg(not(test))]
static BRIDGE: OnceLock<BridgeHandle> = OnceLock::new();

/// Initialize the bridge (if not already running) and return a reference to
/// the shared channel sender.
///
/// First call:
/// 1. Creates a bounded async_channel
/// 2. Spawns the bridge thread (std::thread, NOT a Tokio task)
/// 3. Stores the handle in the OnceLock
///
/// Subsequent calls return the existing sender immediately.
#[cfg(not(test))]
pub fn ensure_bridge() -> &'static async_channel::Sender<BridgeMessage> {
    let handle = BRIDGE.get_or_init(|| {
        let (sender, receiver) = async_channel::bounded(4096);
        let thread = std::thread::Builder::new()
            .name("nes-bridge".into())
            .spawn(move || {
                bridge_loop(receiver);
            })
            .expect("Failed to spawn bridge thread");
        BridgeHandle {
            sender,
            _thread: thread,
        }
    });
    &handle.sender
}

/// Dispatch a single message by invoking the callback directly.
///
/// The callback is carried in the message itself — no registry lookup needed.
fn dispatch_message(callback: EmitCallback, origin_id: u64, buffer_ptr: *mut nes_buffer_bindings::ffi::TupleBuffer, semaphore_ptr: usize) {
    unsafe {
        (callback.emit_fn)(
            callback.emit_ctx,
            origin_id,
            buffer_ptr,
            semaphore_ptr,
        );
    }
}

/// Main loop of the bridge thread.
///
/// Calls `recv_blocking()` in a loop, dispatching each message to its
/// source's emit callback. On channel closure (all senders dropped), the
/// loop exits and the thread terminates.
///
/// Not available in test builds -- see module doc comment for rationale.
#[cfg(not(test))]
pub(crate) fn bridge_loop(receiver: async_channel::Receiver<BridgeMessage>) {
    while let Ok(msg) = receiver.recv_blocking() {
        match msg {
            BridgeMessage::Data { callback, origin_id, mut buffer, semaphore_ptr } => {
                let buffer_ptr = buffer.as_raw_ptr();
                dispatch_message(callback, origin_id, buffer_ptr, semaphore_ptr);
                // buffer is dropped here, releasing C++ refcount via release().
            }
            BridgeMessage::Eos { callback, origin_id } => {
                // Dispatch EOS to C++ (bridge_emit with null buffer, no semaphore).
                dispatch_message(callback, origin_id, std::ptr::null_mut(), 0);
                // Cleanup backpressure state.
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

    // ---- Compile-time type checks ----
    //
    // These tests verify that key types satisfy their required trait bounds.
    // If any bound is broken by a refactor, these tests fail at compile time.

    #[test]
    fn emit_callback_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EmitCallback>();
    }

    #[test]
    fn emit_callback_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<EmitCallback>();
    }
}
