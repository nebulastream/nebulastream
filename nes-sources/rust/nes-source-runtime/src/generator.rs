// GeneratorSource: a concrete AsyncSource that emits N u64 values.
//
// Used for validation and testing. Emits sequential u64 values (0..count),
// each written as little-endian bytes into a TupleBuffer, with a configurable
// interval between emissions. Respects CancellationToken for cooperative shutdown.

use std::time::Duration;

use crate::context::SourceContext;
use crate::error::SourceResult;
use crate::source::AsyncSource;

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
    async fn run(&mut self, ctx: SourceContext) -> SourceResult {
        let cancel = ctx.cancellation_token();

        for i in 0..self.count {
            // Check cancellation before each emission
            if cancel.is_cancelled() {
                return SourceResult::EndOfStream;
            }

            let mut buf = ctx.allocate_buffer().await;
            let slice = buf.as_mut_slice();
            if slice.len() >= 8 {
                slice[..8].copy_from_slice(&i.to_le_bytes());
            }
            buf.set_number_of_tuples(1);

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

    /// Spawn a test GeneratorSource using a unit channel to count emits.
    ///
    /// Returns the receiver so tests can count how many emit() calls happened.
    fn spawn_generator_test(
        source_id: u64,
        source: GeneratorSource,
    ) -> (
        tokio::task::JoinHandle<SourceResult>,
        CancellationToken,
        async_channel::Receiver<()>,
    ) {
        let bp_state = backpressure::register_source(source_id);
        let semaphore = Arc::new(Semaphore::new(10));
        let cancel_token = CancellationToken::new();

        // Bounded channel: each emit() sends () through this channel.
        // The receiver counts messages to verify emit count.
        let (sender, receiver) = async_channel::bounded::<()>(1024);

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
            source.run(ctx).await
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
}
