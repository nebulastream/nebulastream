// SourceContext provides the API surface for source authors.
//
// Source authors interact with the runtime framework through SourceContext,
// which provides async buffer allocation, emission, and cooperative
// cancellation.
//
// emit() implements a three-step gating pattern:
// 1. Await backpressure release (yields if C++ has applied pressure)
// 2. Acquire semaphore permit (yields if inflight limit reached)
// 3. Send BridgeMessage through channel (buffer + permit to bridge thread)
//
// NOTE on testing: SourceContext holds Sender<BridgeMessage> which contains
// TupleBufferHandle, triggering CXX linker symbols on drop. In test builds,
// the sender field uses a dummy unit channel type to avoid CXX dependencies.
// Tests validate the emit() gating logic (backpressure + semaphore) through
// direct BackpressureState and Semaphore tests in source.rs. The actual
// BridgeMessage send path is tested in integration tests under CMake.

use std::sync::Arc;

use nes_source_bindings::ffi;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::backpressure::BackpressureState;
use crate::buffer::{BufferProviderHandle, TupleBufferHandle};
use crate::error::SourceError;

// Production sender type (BridgeMessage channel)
#[cfg(not(test))]
use crate::bridge::BridgeMessage;

/// Wrapper to mark a raw pointer as Send for use in spawn_blocking closures.
///
/// SAFETY: This is only used for `*mut AbstractBufferProvider`, which is
/// thread-safe (BufferManager uses folly::MPMCQueue + atomics). The pointer
/// validity is guaranteed by the C++ SourceHandle's lifetime.
struct SendPtr(*mut ffi::AbstractBufferProvider);

// SAFETY: See SendPtr doc comment above.
unsafe impl Send for SendPtr {}

impl SendPtr {
    fn into_raw(self) -> *mut ffi::AbstractBufferProvider {
        self.0
    }
}

/// Context provided to source `run()` implementations.
///
/// Provides the core source API:
/// - `allocate_buffer().await` -- get a buffer from the C++ buffer pool
/// - `emit(buf).await` -- send a filled buffer downstream (backpressure + semaphore gated)
/// - `cancellation_token()` -- for cooperative shutdown via `tokio::select!`
pub struct SourceContext {
    buffer_provider: BufferProviderHandle,
    #[cfg(not(test))]
    sender: async_channel::Sender<BridgeMessage>,
    #[cfg(test)]
    sender: async_channel::Sender<()>,
    cancellation_token: CancellationToken,
    backpressure: Arc<BackpressureState>,
    semaphore: Arc<Semaphore>,
    origin_id: u64,
}

// SAFETY: SourceContext is Send because:
// - BufferProviderHandle is Send (unsafe impl, validated: BufferManager is thread-safe)
// - Sender is Send (async_channel guarantee)
// - CancellationToken is Send (tokio_util guarantee)
// - Arc<BackpressureState> is Send (Arc + Send+Sync inner)
// - Arc<Semaphore> is Send (Arc + Semaphore is Send+Sync)
// - u64 is Send
// SourceContext is NOT automatically Send/Sync because BufferProviderHandle
// contains a raw pointer, which prevents auto-derivation.
unsafe impl Send for SourceContext {}
// SAFETY: SourceContext is Sync because:
// - BufferProviderHandle wraps a raw pointer to BufferManager which uses
//   folly::MPMCQueue + atomics (thread-safe for concurrent access)
// - Sender is Sync (async_channel guarantee)
// - CancellationToken is Sync (tokio_util guarantee)
// - Arc types are Sync when inner is Send+Sync
// Sync is required so that `&SourceContext` is Send, allowing async fns
// that borrow SourceContext (allocate_buffer, emit) to produce Send futures.
unsafe impl Sync for SourceContext {}

impl SourceContext {
    /// Create a new SourceContext (production).
    #[cfg(not(test))]
    pub fn new(
        buffer_provider: BufferProviderHandle,
        sender: async_channel::Sender<BridgeMessage>,
        cancellation_token: CancellationToken,
        backpressure: Arc<BackpressureState>,
        semaphore: Arc<Semaphore>,
        origin_id: u64,
    ) -> Self {
        Self {
            buffer_provider,
            sender,
            cancellation_token,
            backpressure,
            semaphore,
            origin_id,
        }
    }

    /// Create a new SourceContext (test build -- uses dummy sender to avoid CXX linker).
    #[cfg(test)]
    pub fn new(
        buffer_provider: BufferProviderHandle,
        _sender: async_channel::Sender<()>,
        cancellation_token: CancellationToken,
        backpressure: Arc<BackpressureState>,
        semaphore: Arc<Semaphore>,
        origin_id: u64,
    ) -> Self {
        Self {
            buffer_provider,
            sender: _sender,
            cancellation_token,
            backpressure,
            semaphore,
            origin_id,
        }
    }

    /// Allocate a buffer from the C++ BufferProvider.
    ///
    /// Uses `spawn_blocking` because the C++ `getBufferBlocking()` can block
    /// indefinitely waiting for a buffer to be recycled. Without
    /// `spawn_blocking`, this would starve Tokio worker threads.
    #[cfg(not(test))]
    pub async fn allocate_buffer(&self) -> TupleBufferHandle {
        let send_ptr = SendPtr(self.buffer_provider.as_raw_ptr());
        tokio::task::spawn_blocking(move || {
            // SAFETY: ptr is valid for the source's lifetime (guaranteed by
            // the C++ SourceHandle that created BufferProviderHandle).
            // getBufferBlocking is thread-safe (BufferManager uses atomics).
            let raw = send_ptr.into_raw();
            let pin = unsafe { std::pin::Pin::new_unchecked(&mut *raw) };
            let buf_ptr = ffi::getBufferBlocking(pin);
            TupleBufferHandle::new(buf_ptr)
        })
        .await
        .expect("spawn_blocking task panicked in allocate_buffer")
    }

    /// Allocate a buffer (test build).
    ///
    /// Returns a test TupleBufferHandle backed by a Vec<u8> scratch buffer.
    /// No C++ FFI calls are made.
    #[cfg(test)]
    pub async fn allocate_buffer(&self) -> TupleBufferHandle {
        TupleBufferHandle::new_test()
    }

    /// Emit a filled buffer downstream through the bridge channel.
    ///
    /// Implements a three-step gating pattern:
    /// 1. **Backpressure gate:** Awaits `wait_for_release()` -- yields if C++ has
    ///    applied backpressure, resumes when pressure is released.
    /// 2. **Semaphore gate:** Acquires an `OwnedSemaphorePermit` -- yields if the
    ///    per-source inflight limit is reached, resumes when a permit is freed
    ///    (bridge thread drops permit after processing).
    /// 3. **Channel send:** Sends `BridgeMessage { origin_id, buffer, permit }`
    ///    through the async channel to the bridge thread.
    ///
    /// Returns `Ok(())` on success, or `Err` if the channel is closed or
    /// semaphore is closed (source shutting down).
    #[cfg(not(test))]
    pub async fn emit(&self, buffer: TupleBufferHandle) -> Result<(), SourceError> {
        // Step 1: Wait for backpressure release
        self.backpressure.wait_for_release().await;

        // Step 2: Acquire semaphore permit (yields when inflight limit reached)
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| SourceError::new("semaphore closed -- source is shutting down"))?;

        // Step 3: Send message through bridge channel
        self.sender
            .send(BridgeMessage {
                origin_id: self.origin_id,
                buffer,
                permit,
            })
            .await
            .map_err(|_| SourceError::new("emit channel closed -- source is shutting down"))
    }

    /// Emit a filled buffer (test build).
    ///
    /// Implements the same three-step gating pattern but skips the actual
    /// BridgeMessage send (which would require C++ linker symbols).
    /// The backpressure and semaphore gates are exercised identically.
    #[cfg(test)]
    pub async fn emit(&self, _buffer: TupleBufferHandle) -> Result<(), SourceError> {
        // Step 1: Wait for backpressure release
        self.backpressure.wait_for_release().await;

        // Step 2: Acquire semaphore permit (yields when inflight limit reached)
        let _permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| SourceError::new("semaphore closed -- source is shutting down"))?;

        // Step 3: In test builds, we signal the unit channel instead of
        // sending a BridgeMessage (avoids TupleBufferHandle drop -> CXX linker).
        // The permit is dropped here, releasing the semaphore slot.
        self.sender
            .send(())
            .await
            .map_err(|_| SourceError::new("emit channel closed -- source is shutting down"))
    }

    /// Get a clone of the cancellation token for cooperative shutdown.
    ///
    /// Source authors use this in `tokio::select!` to handle cancellation:
    /// ```ignore
    /// tokio::select! {
    ///     _ = ctx.cancellation_token().cancelled() => break,
    ///     // ... other branches
    /// }
    /// ```
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn _assert_send<T: Send>() {}

    #[test]
    fn source_context_is_send() {
        _assert_send::<SourceContext>();
    }

    #[test]
    fn source_context_has_cancellation_token() {
        // Verify cancellation_token() returns CancellationToken.
        // This is a compile-time signature check.
        fn _check_signature(ctx: &SourceContext) -> CancellationToken {
            ctx.cancellation_token()
        }
    }

    #[test]
    fn emit_signature_returns_result() {
        // Compile-time check: emit() takes TupleBufferHandle, returns Result.
        fn _check_emit_signature(
            ctx: &SourceContext,
            buf: TupleBufferHandle,
        ) -> impl std::future::Future<Output = Result<(), SourceError>> + '_ {
            ctx.emit(buf)
        }
    }
}
