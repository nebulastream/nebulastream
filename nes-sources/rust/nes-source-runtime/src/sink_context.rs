// SinkContext provides the API surface for sink authors.
//
// Sink authors interact with the runtime framework through SinkContext,
// which provides async message reception and cooperative cancellation.
//
// SinkMessage is the message type delivered through the per-sink bounded
// channel. Data carries a buffer to process, Close signals end of stream.
//
// NOTE on cfg(test): Unlike SourceContext which needs cfg(test) because
// BridgeMessage contains semaphore_ptr types, SinkMessage only contains
// TupleBufferHandle which already has cfg(test) variants in buffer.rs.
// So SinkContext does NOT need cfg(test) substitution.

use tokio_util::sync::CancellationToken;

use crate::buffer_iterator::BufferIterator;
use crate::sink_error::SinkError;

/// Message delivered through the per-sink bounded channel.
///
/// Data carries a BufferIterator that yields byte slices of valid data
/// from the main buffer and any child buffers. Sink authors iterate
/// segments without knowing about TupleBuffer internals.
///
/// Flush is an advisory request to flush internal state.
/// Close signals no more data -- the sink should flush and return from run().
/// Close is sent through the same channel as Data to guarantee
/// ordering: all data is delivered before Close.
pub enum SinkMessage {
    /// Data segments to process. Use `iter.next_segment()` to get each `&[u8]`.
    Data(BufferIterator),
    /// Advisory flush request. Sinks should flush internal state but this is not blocking/required.
    Flush,
    /// No more data. Sink should flush and return.
    Close,
}

/// Context provided to AsyncSink::run() implementations.
///
/// Provides:
/// - `recv().await` -- receive the next SinkMessage (Data or Close)
/// - `cancellation_token()` -- for cooperative shutdown via tokio::select!
///
/// The context is borrowed by the sink's run() method. The framework
/// retains ownership for lifecycle management.
pub struct SinkContext {
    receiver: async_channel::Receiver<SinkMessage>,
    cancellation_token: CancellationToken,
}

// SAFETY: SinkContext is Send because:
// - async_channel::Receiver is Send
// - CancellationToken is Send (tokio_util guarantee)
unsafe impl Send for SinkContext {}

// SAFETY: SinkContext is Sync because:
// - async_channel::Receiver is Sync
// - CancellationToken is Sync (tokio_util guarantee)
// Sync is required so that &SinkContext is Send, allowing the async
// run() future (which borrows &SinkContext) to be Send for tokio::spawn.
unsafe impl Sync for SinkContext {}

impl SinkContext {
    /// Create a new SinkContext.
    pub(crate) fn new(
        receiver: async_channel::Receiver<SinkMessage>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            receiver,
            cancellation_token,
        }
    }

    /// Receive the next message from the per-sink channel.
    ///
    /// Returns SinkMessage::Data(buf) for each buffer, or SinkMessage::Close
    /// when no more data will arrive. If the channel is broken (sender dropped
    /// without sending Close), returns SinkMessage::Close to allow clean shutdown.
    pub async fn recv(&self) -> SinkMessage {
        match self.receiver.recv().await {
            Ok(msg) => msg,
            Err(_) => {
                // Channel closed without explicit Close message.
                // This means the sender was dropped (e.g., C++ crashed or
                // SinkTaskHandle was dropped without calling stop_sink).
                // Treat as implicit close -- sink should shut down cleanly.
                tracing::warn!(
                    "Sink channel closed without Close message, treating as implicit close"
                );
                SinkMessage::Close
            }
        }
    }

    /// Get a clone of the cancellation token for cooperative shutdown.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::TupleBufferHandle;

    fn make_test_iter(data: &[u8], content_len: u64) -> BufferIterator {
        let mut buf = TupleBufferHandle::new_test();
        buf.as_mut_slice()[..data.len()].copy_from_slice(data);
        buf.set_number_of_tuples(content_len);
        BufferIterator::new(buf)
    }

    #[test]
    fn sink_context_is_send() {
        fn _assert_send<T: Send>() {}
        _assert_send::<SinkContext>();
    }

    #[test]
    fn sink_context_is_sync() {
        fn _assert_sync<T: Sync>() {}
        _assert_sync::<SinkContext>();
    }

    #[test]
    fn sink_context_has_cancellation_token() {
        fn _check_signature(ctx: &SinkContext) -> CancellationToken {
            ctx.cancellation_token()
        }
    }

    #[test]
    fn sink_message_data_contains_buffer_iterator() {
        fn _assert_data_variant(msg: &SinkMessage) {
            match msg {
                SinkMessage::Data(iter) => {
                    let _: &BufferIterator = iter;
                }
                SinkMessage::Flush => {}
                SinkMessage::Close => {}
            }
        }
    }

    #[tokio::test]
    async fn sink_recv_returns_data_then_close() {
        let (sender, receiver) = async_channel::bounded::<SinkMessage>(8);
        let token = CancellationToken::new();
        let ctx = SinkContext::new(receiver, token);

        sender
            .send(SinkMessage::Data(make_test_iter(&[], 0)))
            .await
            .unwrap();
        sender.send(SinkMessage::Close).await.unwrap();

        match ctx.recv().await {
            SinkMessage::Data(_) => {}
            other => panic!("expected Data, got {:?}", std::mem::discriminant(&other)),
        }
        match ctx.recv().await {
            SinkMessage::Close => {}
            other => panic!("expected Close, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[tokio::test]
    async fn sink_recv_returns_close_on_broken_channel() {
        let (sender, receiver) = async_channel::bounded::<SinkMessage>(8);
        let token = CancellationToken::new();
        let ctx = SinkContext::new(receiver, token);

        drop(sender);

        match ctx.recv().await {
            SinkMessage::Close => {}
            other => panic!("expected Close on broken channel, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[tokio::test]
    async fn sink_channel_fifo_ordering() {
        let (sender, receiver) = async_channel::bounded::<SinkMessage>(8);
        let token = CancellationToken::new();
        let ctx = SinkContext::new(receiver, token);

        for i in 0u64..3 {
            sender.send(SinkMessage::Data(make_test_iter(&i.to_le_bytes(), 8))).await.unwrap();
        }
        sender.send(SinkMessage::Close).await.unwrap();

        for expected_index in 0u64..3 {
            match ctx.recv().await {
                SinkMessage::Data(mut iter) => {
                    let segment = iter.next_segment().expect("should have main segment");
                    assert!(segment.len() >= 8);
                    let actual_index = u64::from_le_bytes(segment[..8].try_into().unwrap());
                    assert_eq!(
                        actual_index, expected_index,
                        "FIFO violation: expected index {}, got {}",
                        expected_index, actual_index
                    );
                }
                other => {
                    panic!(
                        "expected Data with index {}, got {:?}",
                        expected_index, std::mem::discriminant(&other)
                    );
                }
            }
        }

        match ctx.recv().await {
            SinkMessage::Close => {}
            other => panic!("expected final Close, got {:?}", std::mem::discriminant(&other)),
        }
    }
}
