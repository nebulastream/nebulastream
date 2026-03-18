// SourceTaskHandle: opaque handle returned to C++ from spawn_source.
//
// Contains the CancellationToken for cooperative shutdown and the source_id
// for identification. C++ calls stop_source(handle) to request cancellation.

use tokio_util::sync::CancellationToken;
use tracing::info;

/// Opaque handle to a running source task, returned to C++ via Box<SourceTaskHandle>.
///
/// Contains:
/// - `cancellation_token`: for cooperative shutdown via stop_source()
/// - `source_id`: for identification/logging
pub struct SourceTaskHandle {
    cancellation_token: CancellationToken,
    source_id: u64,
}

// SAFETY: SourceTaskHandle is Send because:
// - CancellationToken is Send (tokio_util guarantee)
// - u64 is Send
// Required for Box<SourceTaskHandle> across FFI boundary.
unsafe impl Send for SourceTaskHandle {}

impl SourceTaskHandle {
    /// Create a new SourceTaskHandle.
    pub(crate) fn new(cancellation_token: CancellationToken, source_id: u64) -> Self {
        Self {
            cancellation_token,
            source_id,
        }
    }

    /// Get the source_id for this handle.
    pub fn source_id(&self) -> u64 {
        self.source_id
    }
}

/// Request cooperative cancellation of a running source.
///
/// Non-blocking: sets the CancellationToken, which the source task observes
/// in its `tokio::select!` loop. The source task will exit cooperatively.
pub fn stop_source(handle: &SourceTaskHandle) {
    info!(source_id = handle.source_id, "Stop requested for source");
    handle.cancellation_token.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn _assert_send<T: Send>() {}

    #[test]
    fn source_task_handle_is_send() {
        _assert_send::<SourceTaskHandle>();
    }

    #[test]
    fn stop_source_cancels_token() {
        let token = CancellationToken::new();
        let handle = SourceTaskHandle::new(token.clone(), 42);
        assert!(!token.is_cancelled());
        stop_source(&handle);
        assert!(token.is_cancelled());
    }

    #[test]
    fn source_task_handle_stores_source_id() {
        let token = CancellationToken::new();
        let handle = SourceTaskHandle::new(token, 123);
        assert_eq!(handle.source_id(), 123);
    }
}
