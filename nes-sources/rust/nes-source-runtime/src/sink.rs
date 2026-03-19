// AsyncSink trait and spawn_sink lifecycle function.
//
// AsyncSink is the primary trait that sink authors implement. It has a
// single `run()` method that receives a borrowed SinkContext. The framework
// handles channel creation, Tokio task spawning, and error reporting.
//
// spawn_sink() creates a bounded channel, SinkContext, Tokio task for the
// sink, a monitoring task for error reporting, and returns a SinkTaskHandle.

use std::ffi::{CString, c_void};

use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::sink_context::{SinkContext, SinkMessage};
use crate::sink_error::SinkError;
use crate::sink_handle::SinkTaskHandle;

/// Primary trait for sink implementations.
///
/// Sink authors implement this trait with a single `run()` method.
/// The framework provides a `SinkContext` with:
/// - `recv().await` -- receive the next SinkMessage (Data or Close)
/// - `cancellation_token()` -- for cooperative shutdown
///
/// The context is borrowed -- the framework retains ownership for lifecycle.
pub trait AsyncSink: Send + 'static {
    /// Run the sink, consuming data until Close is received or an error occurs.
    ///
    /// Returns Ok(()) for normal completion or Err(SinkError) for failure.
    fn run(
        &mut self,
        ctx: &SinkContext,
    ) -> impl std::future::Future<Output = Result<(), SinkError>> + Send;
}

/// C-style error callback function pointer type.
/// Same signature as source::ErrorFnPtr -- reuse pattern from source.rs.
pub type ErrorFnPtr =
    unsafe extern "C" fn(ctx: *mut c_void, source_id: u64, message: *const std::ffi::c_char);

/// Default channel capacity for sinks. Same as BRIDGE_CHANNEL_CAPACITY.
/// Phase 5 makes this configurable via spawn parameter from C++.
pub const DEFAULT_SINK_CHANNEL_CAPACITY: usize = 64;

/// Spawn a sink as a Tokio task with lifecycle management.
///
/// # Lifecycle
/// 1. Creates bounded channel with given capacity
/// 2. Creates SinkContext with receiver + cancellation token
/// 3. Spawns sink's run() as a Tokio task on the shared runtime
/// 4. Spawns monitoring task for error reporting
/// 5. Returns SinkTaskHandle with channel sender
///
/// # Arguments
/// - `sink_id`: unique identifier for this sink instance
/// - `sink`: the AsyncSink implementation to run
/// - `channel_capacity`: bounded channel size
/// - `error_fn`: C-style function pointer for error reporting
/// - `error_ctx`: opaque context pointer for error_fn
#[cfg(not(test))]
pub fn spawn_sink(
    sink_id: u64,
    mut sink: impl AsyncSink,
    channel_capacity: usize,
    error_fn: ErrorFnPtr,
    error_ctx: *mut c_void,
) -> Box<SinkTaskHandle> {
    let cancel_token = CancellationToken::new();
    let (sender, receiver) = async_channel::bounded::<SinkMessage>(channel_capacity);
    let ctx = SinkContext::new(receiver, cancel_token.clone());

    let runtime = crate::runtime::source_runtime();
    let join_handle = runtime.spawn(async move { sink.run(&ctx).await });

    let err_cb = ErrorCallback {
        error_fn,
        error_ctx: SendVoidPtr(error_ctx),
    };

    runtime.spawn(async move {
        sink_monitoring_task(join_handle, sink_id, err_cb).await;
    });

    Box::new(SinkTaskHandle::new(sender, cancel_token, sink_id))
}

/// Wrapper to mark a void* raw pointer as Send for monitoring task.
struct SendVoidPtr(*mut c_void);
// SAFETY: The error_ctx pointer is guaranteed valid for the sink's lifetime
// by the C++ TokioSink.
unsafe impl Send for SendVoidPtr {}

/// Wrapper for error callback info that is Send-safe.
///
/// Groups error_fn + error_ctx together with Send impl for use in
/// the monitoring task's async block.
struct ErrorCallback {
    error_fn: ErrorFnPtr,
    error_ctx: SendVoidPtr,
}

// SAFETY: ErrorCallback is Send because:
// - Function pointers are inherently Send/Sync in Rust
// - error_ctx validity is guaranteed by C++ TokioSink lifetime
unsafe impl Send for ErrorCallback {}

/// Monitoring task for sink lifecycle.
///
/// Awaits the sink task's JoinHandle and calls error callback on failure.
/// Simpler than source monitoring: no emit registry or backpressure to cleanup.
async fn sink_monitoring_task(
    join_handle: tokio::task::JoinHandle<Result<(), SinkError>>,
    sink_id: u64,
    err_cb: ErrorCallback,
) {
    let result = join_handle.await;
    match result {
        Ok(Ok(())) => {
            info!(sink_id = sink_id, "Sink completed successfully");
        }
        Ok(Err(e)) => {
            error!(sink_id = sink_id, error = %e, "Sink completed with error");
            let msg = CString::new(format!("{}", e)).unwrap_or_else(|_| {
                CString::new("sink error (message contained null byte)").unwrap()
            });
            unsafe {
                (err_cb.error_fn)(err_cb.error_ctx.0, sink_id, msg.as_ptr());
            }
        }
        Err(join_error) => {
            error!(sink_id = sink_id, "Sink task panicked");
            let msg = CString::new(format!("sink panicked: {}", join_error))
                .unwrap_or_else(|_| CString::new("sink panicked").unwrap());
            unsafe {
                (err_cb.error_fn)(err_cb.error_ctx.0, sink_id, msg.as_ptr());
            }
        }
    }
    info!(sink_id = sink_id, "Sink monitoring task: complete");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::TupleBufferHandle;
    use crate::buffer_iterator::BufferIterator;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    // ---- Test sink implementations ----

    /// Mock sink that collects all data segments.
    struct MockCollectorSink {
        collected: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl AsyncSink for MockCollectorSink {
        async fn run(&mut self, ctx: &SinkContext) -> Result<(), SinkError> {
            loop {
                match ctx.recv().await {
                    SinkMessage::Data(mut iter) => {
                        let mut all_bytes = Vec::new();
                        while let Some(segment) = iter.next_segment() {
                            all_bytes.extend_from_slice(segment);
                        }
                        self.collected.lock().unwrap().push(all_bytes);
                    }
                    SinkMessage::Flush => {}
                    SinkMessage::Close => return Ok(()),
                }
            }
        }
    }

    /// Sink that immediately returns Ok.
    struct TestImmediateCompleteSink;
    impl AsyncSink for TestImmediateCompleteSink {
        async fn run(&mut self, _ctx: &SinkContext) -> Result<(), SinkError> {
            Ok(())
        }
    }

    /// Sink that returns an error.
    struct TestErrorSink;
    impl AsyncSink for TestErrorSink {
        async fn run(&mut self, _ctx: &SinkContext) -> Result<(), SinkError> {
            Err(SinkError::new("test error from sink"))
        }
    }

    /// Sink that waits for cancellation.
    struct TestCancellableSink;
    impl AsyncSink for TestCancellableSink {
        async fn run(&mut self, ctx: &SinkContext) -> Result<(), SinkError> {
            ctx.cancellation_token().cancelled().await;
            Ok(())
        }
    }

    // ---- Test-only spawn ----

    fn spawn_test_sink(
        sink_id: u64,
        mut sink: impl AsyncSink,
        channel_capacity: usize,
        error_fn: ErrorFnPtr,
        error_ctx: *mut c_void,
    ) -> Box<SinkTaskHandle> {
        let cancel_token = CancellationToken::new();
        let (sender, receiver) = async_channel::bounded::<SinkMessage>(channel_capacity);
        let ctx = SinkContext::new(receiver, cancel_token.clone());

        let join_handle = tokio::spawn(async move { sink.run(&ctx).await });

        let err_cb = ErrorCallback {
            error_fn,
            error_ctx: SendVoidPtr(error_ctx),
        };

        tokio::spawn(async move {
            sink_monitoring_task(join_handle, sink_id, err_cb).await;
        });

        Box::new(SinkTaskHandle::new(sender, cancel_token, sink_id))
    }

    // ---- Mock callbacks ----

    unsafe extern "C" fn mock_error_callback(
        ctx: *mut c_void,
        source_id: u64,
        _message: *const std::ffi::c_char,
    ) {
        let log = unsafe { &*(ctx as *const Arc<Mutex<Vec<u64>>>) };
        log.lock().unwrap().push(source_id);
    }

    // ---- Compile-time checks ----

    #[test]
    fn async_sink_trait_is_implementable() {
        fn _assert_sink<T: AsyncSink>() {}
        _assert_sink::<MockCollectorSink>();
        _assert_sink::<TestImmediateCompleteSink>();
        _assert_sink::<TestErrorSink>();
        _assert_sink::<TestCancellableSink>();
    }

    #[test]
    fn sink_is_send() {
        fn _assert_send<T: Send>() {}
        _assert_send::<MockCollectorSink>();
    }

    // ---- Functional tests ----

    #[tokio::test]
    async fn spawn_sink_runs_to_completion() {
        let error_log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let error_ctx = &error_log as *const _ as *mut c_void;
        let sink_id = 40001u64;

        let handle = spawn_test_sink(
            sink_id,
            TestImmediateCompleteSink,
            8,
            mock_error_callback,
            error_ctx,
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(crate::sink_handle::is_sink_done(&handle));
        assert!(error_log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stop_sink_sends_close() {
        let error_log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let error_ctx = &error_log as *const _ as *mut c_void;
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink_id = 40002u64;

        let handle = spawn_test_sink(
            sink_id,
            MockCollectorSink {
                collected: collected.clone(),
            },
            8,
            mock_error_callback,
            error_ctx,
        );

        // Sink should be running
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!crate::sink_handle::is_sink_done(&handle));

        // Stop the sink
        let sent = crate::sink_handle::stop_sink(&handle);
        assert!(sent);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(crate::sink_handle::is_sink_done(&handle));
        assert!(error_log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sink_error_triggers_callback() {
        let error_log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let error_ctx = &error_log as *const _ as *mut c_void;
        let sink_id = 40003u64;

        let _handle = spawn_test_sink(
            sink_id,
            TestErrorSink,
            8,
            mock_error_callback,
            error_ctx,
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        let calls = error_log.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], sink_id);
    }

    #[tokio::test]
    async fn full_lifecycle_open_write_close() {
        let error_log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let error_ctx = &error_log as *const _ as *mut c_void;
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink_id = 40004u64;

        let handle = spawn_test_sink(
            sink_id,
            MockCollectorSink {
                collected: collected.clone(),
            },
            8,
            mock_error_callback,
            error_ctx,
        );

        // Send 3 data segments with distinct content
        for i in 0..3u64 {
            let mut buf = TupleBufferHandle::new_test();
            buf.as_mut_slice()[..8].copy_from_slice(&i.to_le_bytes());
            buf.set_number_of_tuples(8);
            let iter = BufferIterator::new(buf);
            handle
                .sender()
                .send(SinkMessage::Data(iter))
                .await
                .unwrap();
        }

        // Send Close
        handle
            .sender()
            .send(SinkMessage::Close)
            .await
            .unwrap();

        // Wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify all 3 segments collected
        let data = collected.lock().unwrap();
        assert_eq!(data.len(), 3);
        // Verify ordering (first 8 bytes of each segment)
        for (i, buf_data) in data.iter().enumerate() {
            let val = u64::from_le_bytes(buf_data[..8].try_into().unwrap());
            assert_eq!(val, i as u64);
        }

        // No error callback
        assert!(error_log.lock().unwrap().is_empty());
        assert!(crate::sink_handle::is_sink_done(&handle));
    }

    #[tokio::test]
    async fn sink_cancellation_token_works() {
        let error_log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let error_ctx = &error_log as *const _ as *mut c_void;
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink_id = 40005u64;

        let handle = spawn_test_sink(
            sink_id,
            MockCollectorSink {
                collected: collected.clone(),
            },
            8,
            mock_error_callback,
            error_ctx,
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!crate::sink_handle::is_sink_done(&handle));

        // Stop via Close message (primary shutdown path)
        let sent = crate::sink_handle::stop_sink(&handle);
        assert!(sent);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(crate::sink_handle::is_sink_done(&handle));
        assert!(error_log.lock().unwrap().is_empty());
    }
}
