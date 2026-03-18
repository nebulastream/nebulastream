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

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::backpressure::BackpressureState;
use crate::buffer::{BufferProviderHandle, TupleBufferHandle};
use crate::error::SourceError;

// Production sender type (BridgeMessage channel)
#[cfg(not(test))]
use crate::bridge::BridgeMessage;

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
    sender: async_channel::Sender<tokio::sync::OwnedSemaphorePermit>,
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
        sender: async_channel::Sender<tokio::sync::OwnedSemaphorePermit>,
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

    /// Allocate a buffer from the C++ BufferProvider.
    ///
    /// Uses non-blocking `tryGetBuffer` + async notification instead of
    /// `spawn_blocking`. When no buffer is available, the task awaits
    /// `BUFFER_NOTIFY` which is signaled by C++ whenever a buffer is recycled.
    /// This avoids spawning blocking threads and scales to many concurrent sources.
    #[cfg(not(test))]
    pub async fn allocate_buffer(&self) -> TupleBufferHandle {
        use crate::buffer::BUFFER_NOTIFY;
        loop {
            if let Some(buf) = self.buffer_provider.try_get_buffer() {
                return buf;
            }
            BUFFER_NOTIFY.notified().await;
        }
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

        // Forget the permit so it doesn't auto-release on drop.
        // Instead, we pass an Arc<Semaphore> raw pointer to C++ which calls
        // nes_release_semaphore_slot (add_permits(1)) when the pipeline is done.
        std::mem::forget(permit);
        let semaphore_ptr = std::sync::Arc::into_raw(self.semaphore.clone()) as usize;

        // Step 3: Send message through bridge channel
        self.sender
            .send(BridgeMessage::Data {
                origin_id: self.origin_id,
                buffer,
                semaphore_ptr,
            })
            .await
            .map_err(|_| SourceError::new("emit channel closed -- source is shutting down"))
    }

    /// Send end-of-stream through the bridge channel.
    ///
    /// Called by the framework after `AsyncSource::run()` returns EndOfStream.
    /// Sent through the same channel as data to guarantee ordering: all data
    /// buffers are delivered before EOS reaches the C++ pipeline.
    #[cfg(not(test))]
    pub(crate) async fn emit_eos(&self) {
        let _ = self.sender
            .send(BridgeMessage::Eos {
                origin_id: self.origin_id,
            })
            .await;
    }

    /// Send end-of-stream (test build — no-op since test channel uses unit type).
    #[cfg(test)]
    pub(crate) async fn emit_eos(&self) {
        // In test builds, EOS is not sent through the channel.
        // The test infrastructure handles lifecycle differently.
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
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| SourceError::new("semaphore closed -- source is shutting down"))?;

        // Step 3: Send the permit through the channel. The permit stays alive
        // until the receiver drops it — this mirrors production where the permit
        // lives inside BridgeMessage until the bridge thread processes it.
        self.sender
            .send(permit)
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
