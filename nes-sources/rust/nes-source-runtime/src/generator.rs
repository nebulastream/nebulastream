// GeneratorSource: a concrete AsyncSource that emits N u64 values.
//
// Used for validation and testing. Emits sequential u64 values (0..count),
// each written as little-endian bytes into a TupleBuffer, with a configurable
// interval between emissions. Respects CancellationToken for cooperative shutdown.

use std::collections::HashMap;
use std::time::Duration;

use crate::config::{ConfigParam, ParamType};
use crate::context::SourceContext;
use crate::error::SourceResult;
use crate::source::AsyncSource;

/// Declarative config schema for GeneratorSource.
pub const CONFIG_SCHEMA: &[ConfigParam] = &[
    ConfigParam { name: "generator_count", param_type: ParamType::U64, default: Some("10") },
    ConfigParam { name: "generator_interval_ms", param_type: ParamType::U64, default: Some("0") },
];

/// Factory function: create a GeneratorSource from a validated config map.
///
/// The config is expected to have been validated against `CONFIG_SCHEMA` already,
/// so parse errors here indicate a bug in the validation layer.
pub fn create_generator_source(config: &HashMap<String, String>) -> Result<GeneratorSource, String> {
    let count = config.get("generator_count")
        .unwrap_or(&"10".to_string())
        .parse::<u64>()
        .map_err(|e| format!("invalid generator_count: {e}"))?;
    let interval_ms = config.get("generator_interval_ms")
        .unwrap_or(&"0".to_string())
        .parse::<u64>()
        .map_err(|e| format!("invalid generator_interval_ms: {e}"))?;
    Ok(GeneratorSource::new(count, Duration::from_millis(interval_ms)))
}

/// A source that emits `count` sequential u64 values with a configurable interval.
///
/// Each emission:
/// 1. Allocates a buffer via `ctx.allocate_buffer()`
/// 2. Writes the current index as little-endian u64 into the first 8 bytes
/// 3. Sets `number_of_tuples(1)`
/// 4. Calls `ctx.emit(buf).await`
/// 5. Sleeps for `interval` before the next emission
///
/// Cooperative cancellation is checked on every iteration via `tokio::select!`.
/// Returns `SourceResult::EndOfStream` on normal completion or cancellation.
pub struct GeneratorSource {
    count: u64,
    interval: Duration,
}

impl GeneratorSource {
    /// Create a new GeneratorSource.
    ///
    /// - `count`: number of u64 values to emit (0 = immediate EndOfStream)
    /// - `interval`: delay between emissions
    pub fn new(count: u64, interval: Duration) -> Self {
        Self { count, interval }
    }
}

impl AsyncSource for GeneratorSource {
    async fn run(&mut self, ctx: &SourceContext) -> SourceResult {
        let cancel = ctx.cancellation_token();

        for i in 0..self.count {
            // Check cancellation before each emission
            if cancel.is_cancelled() {
                return SourceResult::EndOfStream;
            }

            let mut buf = ctx.allocate_buffer().await;
            // Write CSV text into the buffer — the pipeline's InputFormatter parses CSV.
            let csv_line = format!("{}\n", i);
            let bytes = csv_line.as_bytes();
            let slice = buf.as_mut_slice();
            let len = bytes.len().min(slice.len());
            slice[..len].copy_from_slice(&bytes[..len]);
            // numberOfTuples = byte count for raw/CSV sources (InputFormatter convention)
            buf.set_number_of_tuples(len as u64);

            if let Err(_) = ctx.emit(buf).await {
                // Channel closed during shutdown
                return SourceResult::EndOfStream;
            }

            // Use select! for the sleep to allow cancellation during interval
            tokio::select! {
                _ = cancel.cancelled() => {
                    return SourceResult::EndOfStream;
                }
                _ = tokio::time::sleep(self.interval) => {}
            }
        }

        SourceResult::EndOfStream
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backpressure;
    use crate::buffer::BufferProviderHandle;
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;

    use tokio::sync::OwnedSemaphorePermit;

    /// Spawn a test GeneratorSource.
    ///
    /// Returns the receiver so tests can count emits and control permit lifetime.
    /// Each emit() sends an OwnedSemaphorePermit through the channel — the permit
    /// stays alive until the receiver drops it, mirroring production behavior.
    fn spawn_generator_test(
        source_id: u64,
        source: GeneratorSource,
    ) -> (
        tokio::task::JoinHandle<SourceResult>,
        CancellationToken,
        async_channel::Receiver<OwnedSemaphorePermit>,
    ) {
        spawn_generator_test_with_limit(source_id, source, 10)
    }

    /// Spawn a test GeneratorSource with a custom inflight limit.
    fn spawn_generator_test_with_limit(
        source_id: u64,
        source: GeneratorSource,
        inflight_limit: usize,
    ) -> (
        tokio::task::JoinHandle<SourceResult>,
        CancellationToken,
        async_channel::Receiver<OwnedSemaphorePermit>,
    ) {
        let bp_state = backpressure::register_source(source_id);
        let semaphore = Arc::new(Semaphore::new(inflight_limit));
        let cancel_token = CancellationToken::new();

        // Bounded channel: each emit() sends its semaphore permit through this
        // channel. The permit stays alive until the receiver drops it.
        let (sender, receiver) = async_channel::bounded::<OwnedSemaphorePermit>(1024);

        let buffer_provider = unsafe { BufferProviderHandle::from_raw(std::ptr::null_mut()) };
        let ctx = SourceContext::new(
            buffer_provider,
            sender,
            cancel_token.clone(),
            bp_state,
            semaphore,
            source_id,
        );

        let mut source = source;
        let handle = tokio::spawn(async move {
            source.run(&ctx).await
        });

        (handle, cancel_token, receiver)
    }

    // Compile-time check: GeneratorSource implements AsyncSource
    #[test]
    fn generator_source_implements_async_source() {
        fn _assert_source<T: AsyncSource>() {}
        _assert_source::<GeneratorSource>();
    }

    #[tokio::test]
    async fn generator_emits_exact_count() {
        let source = GeneratorSource::new(5, Duration::from_millis(1));
        let (handle, _cancel, receiver) = spawn_generator_test(30001, source);

        // Wait for source to complete
        let result = handle.await.unwrap();
        assert!(matches!(result, SourceResult::EndOfStream));

        // Count received emit signals
        let mut count = 0;
        while receiver.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 5, "Expected exactly 5 emits, got {}", count);
    }

    #[tokio::test]
    async fn generator_zero_count_returns_immediately() {
        let source = GeneratorSource::new(0, Duration::from_millis(1));
        let (handle, _cancel, receiver) = spawn_generator_test(30002, source);

        let result = handle.await.unwrap();
        assert!(matches!(result, SourceResult::EndOfStream));

        // No emits should have occurred
        assert!(receiver.try_recv().is_err(), "Expected no emits for count=0");
    }

    #[tokio::test]
    async fn generator_cancellation_exits_early() {
        let source = GeneratorSource::new(1000, Duration::from_millis(5));
        let (handle, cancel, receiver) = spawn_generator_test(30003, source);

        // Let it emit a few values then cancel
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        let result = handle.await.unwrap();
        assert!(matches!(result, SourceResult::EndOfStream));

        // Should have emitted fewer than 1000 values
        let mut count = 0;
        while receiver.try_recv().is_ok() {
            count += 1;
        }
        assert!(count < 1000, "Expected fewer than 1000 emits after cancellation, got {}", count);
        assert!(count > 0, "Expected at least some emits before cancellation");
    }

    #[tokio::test]
    async fn inflight_limit_blocks_source() {
        // Source wants to emit 100 values, but inflight limit is 3.
        // If we never drain the receiver, the source should block after 3 emits.
        let source = GeneratorSource::new(100, Duration::from_millis(0));
        let (handle, _cancel, receiver) = spawn_generator_test_with_limit(30004, source, 3);

        // Give the source time to hit the limit
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Source should have emitted exactly 3 values (filling the semaphore)
        // and then blocked on the 4th semaphore acquire.
        // Channel has capacity 1024, so the channel itself is not the bottleneck.
        assert_eq!(receiver.len(), 3, "Expected exactly 3 emits (inflight limit), got {}", receiver.len());

        // Now drain one permit — source should emit one more and re-fill to 3
        let permit = receiver.recv().await.unwrap();
        drop(permit); // release the semaphore slot
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(receiver.len(), 3, "Expected 3 in channel after draining one (source should fill the slot)");

        // Clean up: close channel + drain permits to unblock the source.
        // Source is stuck on semaphore.acquire inside emit(). Draining permits
        // releases semaphore slots; then channel send fails (closed) → source returns.
        receiver.close();
        while let Ok(p) = receiver.try_recv() {
            drop(p);
        }
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("source should exit after channel closed")
            .unwrap();
        assert!(matches!(result, SourceResult::EndOfStream));
    }
}
