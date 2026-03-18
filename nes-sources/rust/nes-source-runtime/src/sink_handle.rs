// SinkTaskHandle: opaque handle returned to C++ from spawn_sink.
//
// Contains the channel Sender for sending Close and detecting completion,
// the CancellationToken for cooperative shutdown, and the sink_id for
// identification.
//
// Unlike SourceTaskHandle (which only has CancellationToken), SinkTaskHandle
// holds the Sender because:
// 1. stop_sink sends Close through the data channel (ordering guarantee)
// 2. is_sink_done checks sender.is_closed() (sink dropped the receiver)

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::sink_context::SinkMessage;

/// Opaque handle to a running sink task.
///
/// Returned from spawn_sink via Box<SinkTaskHandle>.
/// C++ uses this to send Close and poll for completion.
pub struct SinkTaskHandle {
    sender: async_channel::Sender<SinkMessage>,
    cancellation_token: CancellationToken,
    sink_id: u64,
}

// SAFETY: SinkTaskHandle is Send because:
// - async_channel::Sender is Send
// - CancellationToken is Send (tokio_util guarantee)
// - u64 is Send
unsafe impl Send for SinkTaskHandle {}

impl SinkTaskHandle {
    /// Create a new SinkTaskHandle.
    pub(crate) fn new(
        sender: async_channel::Sender<SinkMessage>,
        cancellation_token: CancellationToken,
        sink_id: u64,
    ) -> Self {
        Self {
            sender,
            cancellation_token,
            sink_id,
        }
    }

    /// Get the sink_id for this handle.
    pub fn sink_id(&self) -> u64 {
        self.sink_id
    }

    /// Get a reference to the channel sender (for C++ try_send in Phase 5).
    pub fn sender(&self) -> &async_channel::Sender<SinkMessage> {
        &self.sender
    }
}

/// Send Close through the data channel to request orderly shutdown.
///
/// Uses try_send (non-blocking). If the channel is full, returns false
/// and the caller should retry (Phase 5 repeatTask pattern). If the
/// channel is closed (sink already finished), returns true.
pub fn stop_sink(handle: &SinkTaskHandle) -> bool {
    info!(sink_id = handle.sink_id, "Stop requested for sink");
    match handle.sender.try_send(SinkMessage::Close) {
        Ok(()) => {
            // Close message sent successfully
            true
        }
        Err(async_channel::TrySendError::Full(_)) => {
            // Channel full, caller should retry
            info!(
                sink_id = handle.sink_id,
                "Sink channel full, Close will be retried"
            );
            false
        }
        Err(async_channel::TrySendError::Closed(_)) => {
            // Channel already closed (sink finished or errored)
            info!(
                sink_id = handle.sink_id,
                "Sink channel already closed"
            );
            true
        }
    }
}

/// Check if the sink task has completed.
///
/// Returns true when the sink task has dropped the Receiver (either by
/// returning from run() normally or on error). C++ uses this in Phase 5
/// tryStop polling.
pub fn is_sink_done(handle: &SinkTaskHandle) -> bool {
    handle.sender.is_closed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::TupleBufferHandle;

    fn _assert_send<T: Send>() {}

    #[test]
    fn sink_task_handle_is_send() {
        _assert_send::<SinkTaskHandle>();
    }

    #[test]
    fn sink_task_handle_stores_sink_id() {
        let (sender, _receiver) = async_channel::bounded::<SinkMessage>(8);
        let token = CancellationToken::new();
        let handle = SinkTaskHandle::new(sender, token, 123);
        assert_eq!(handle.sink_id(), 123);
    }

    #[tokio::test]
    async fn stop_sink_sends_close() {
        let (sender, receiver) = async_channel::bounded::<SinkMessage>(8);
        let token = CancellationToken::new();
        let handle = SinkTaskHandle::new(sender, token, 42);

        let sent = stop_sink(&handle);
        assert!(sent);

        // Verify receiver gets SinkMessage::Close
        match receiver.recv().await.unwrap() {
            SinkMessage::Close => {} // expected
            other => panic!("expected Close, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[tokio::test]
    async fn stop_sink_returns_false_when_full() {
        // Create bounded(1) channel and fill it
        let (sender, _receiver) = async_channel::bounded::<SinkMessage>(1);
        let token = CancellationToken::new();

        // Fill the channel with a Data message
        sender
            .send(SinkMessage::Data(TupleBufferHandle::new_test()))
            .await
            .unwrap();

        let handle = SinkTaskHandle::new(sender, token, 42);
        let sent = stop_sink(&handle);
        assert!(!sent, "stop_sink should return false when channel is full");
    }

    #[test]
    fn is_sink_done_returns_false_while_running() {
        let (sender, _receiver) = async_channel::bounded::<SinkMessage>(8);
        let token = CancellationToken::new();
        let handle = SinkTaskHandle::new(sender, token, 42);
        assert!(!is_sink_done(&handle));
    }

    #[test]
    fn is_sink_done_returns_true_after_receiver_dropped() {
        let (sender, receiver) = async_channel::bounded::<SinkMessage>(8);
        let token = CancellationToken::new();
        let handle = SinkTaskHandle::new(sender, token, 42);

        // Drop receiver to simulate sink task completing
        drop(receiver);

        assert!(is_sink_done(&handle));
    }
}
