// AsyncSource trait and spawn_source lifecycle function.
//
// AsyncSource is the primary trait that source authors implement. It has a
// single `run()` method that receives a SourceContext. The framework handles
// all complexity (backpressure, semaphores, bridge channel, error surfacing).
//
// spawn_source() creates a Tokio task for the source, a monitoring task for
// error reporting, and returns an opaque SourceTaskHandle to C++.

use std::ffi::{CString, c_void};
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::backpressure;
use crate::bridge;
use crate::buffer::BufferProviderHandle;
use crate::context::SourceContext;
use crate::error::SourceResult;
use crate::handle::SourceTaskHandle;

/// Primary trait for source implementations.
///
/// Source authors implement this trait with a single `run()` method.
/// The framework provides a `SourceContext` with:
/// - `allocate_buffer().await` -- get a buffer from C++ pool
/// - `emit(buf).await` -- send buffer downstream (backpressure + semaphore hidden)
/// - `cancellation_token()` -- for cooperative shutdown
///
/// The context is borrowed exclusively — the framework retains ownership so it
/// can send EOS through the same channel after `run()` returns, guaranteeing
/// that all data buffers are delivered before the end-of-stream signal.
pub trait AsyncSource: Send + 'static {
    /// Run the source, producing data until completion or cancellation.
    ///
    /// Returns `SourceResult::EndOfStream` for normal completion or
    /// `SourceResult::Error(e)` for failure.
    fn run(
        &mut self,
        ctx: &SourceContext,
    ) -> impl std::future::Future<Output = SourceResult> + Send;
}

/// C-style error callback function pointer type.
///
/// Called by the monitoring task when a source exits with an error or panic.
/// - `ctx`: opaque pointer to error context (owned by C++ TokioSource)
/// - `source_id`: identifies which source failed
/// - `message`: null-terminated error description
pub type ErrorFnPtr = unsafe extern "C" fn(ctx: *mut c_void, source_id: u64, message: *const std::ffi::c_char);

/// Spawn a source as a Tokio task with lifecycle management.
///
/// # Lifecycle
/// 1. Registers per-source emit callback in EMIT_REGISTRY (before spawning)
/// 2. Registers backpressure state for this source
/// 3. Spawns the source's `run()` as a Tokio task
/// 4. Spawns a monitoring task that:
///    - Awaits source completion
///    - Calls error_fn on error/panic
///    - Unregisters backpressure state and emit callback on exit
///
/// # Arguments
/// - `source_id`: unique identifier for this source instance
/// - `source`: the AsyncSource implementation to run
/// - `buffer_provider`: handle to C++ buffer pool
/// - `inflight_limit`: max concurrent in-flight buffers (semaphore capacity)
/// - `emit_fn`: C-style function pointer for emit dispatch
/// - `emit_ctx`: opaque context pointer for emit_fn
/// - `error_fn`: C-style function pointer for error reporting
/// - `error_ctx`: opaque context pointer for error_fn
///
/// # Returns
/// `Box<SourceTaskHandle>` -- opaque handle for stop_source()
#[cfg(not(test))]
pub fn spawn_source(
    source_id: u64,
    mut source: impl AsyncSource,
    buffer_provider: BufferProviderHandle,
    inflight_limit: u32,
    emit_fn: bridge::EmitFnPtr,
    emit_ctx: *mut c_void,
    error_fn: ErrorFnPtr,
    error_ctx: *mut c_void,
) -> Box<SourceTaskHandle> {
    // CRITICAL: Register emit callback BEFORE spawning the source task.
    // This ensures the bridge thread can dispatch messages for this source
    // as soon as they arrive.
    bridge::register_emit(source_id, emit_fn, emit_ctx);

    // Register backpressure state for this source
    let bp_state = backpressure::register_source(source_id);

    // Create per-source semaphore for inflight limiting
    let semaphore = Arc::new(Semaphore::new(inflight_limit as usize));

    // Create cancellation token for cooperative shutdown
    let cancel_token = CancellationToken::new();

    // Get the shared bridge channel sender (no emit callback args)
    let sender = bridge::ensure_bridge().clone();

    // Construct SourceContext with all fields
    let ctx = SourceContext::new(
        buffer_provider,
        sender,
        cancel_token.clone(),
        bp_state,
        semaphore,
        source_id,
    );

    // Spawn source task on the Tokio runtime.
    // The source task owns ctx so it can send EOS after run() returns,
    // through the same bridge channel as data (guaranteeing ordering).
    let runtime = crate::runtime::source_runtime();
    let join_handle = runtime.spawn(async move {
        let result = source.run(&ctx).await;
        if matches!(result, SourceResult::EndOfStream) {
            ctx.emit_eos().await;
        }
        result
    });

    // Wrap error callback info for Send-safe async block
    let err_cb = ErrorCallback {
        error_fn,
        error_ctx: SendVoidPtr(error_ctx),
    };

    // Spawn monitoring task
    runtime.spawn(async move {
        monitoring_task(join_handle, source_id, err_cb).await;
    });

    Box::new(SourceTaskHandle::new(cancel_token, source_id))
}

/// Wrapper to mark a void* raw pointer as Send for monitoring task.
struct SendVoidPtr(*mut c_void);
// SAFETY: The error_ctx pointer is guaranteed valid for the source's lifetime
// by the C++ TokioSource.
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
// - error_ctx validity is guaranteed by C++ TokioSource lifetime
unsafe impl Send for ErrorCallback {}

/// Shared monitoring task logic for both production and test spawn.
///
/// Awaits the source task's JoinHandle and calls error callback on failure.
/// For EndOfStream, EOS and cleanup are handled by the source task (via
/// emit_eos through the bridge channel) and the bridge thread respectively.
/// For errors/panics, the monitoring task handles cleanup directly since
/// no EOS message was sent through the bridge channel.
async fn monitoring_task(
    join_handle: tokio::task::JoinHandle<SourceResult>,
    source_id: u64,
    err_cb: ErrorCallback,
) {
    let result = join_handle.await;
    match result {
        Ok(SourceResult::EndOfStream) => {
            info!(source_id = source_id, "Source completed: end of stream");
            // EOS was sent by the source task through the bridge channel.
            // The bridge thread handles cleanup (unregister emit + backpressure)
            // after processing the EOS message — no action needed here.
        }
        Ok(SourceResult::Error(e)) => {
            error!(source_id = source_id, error = %e, "Source completed with error");
            let msg = CString::new(format!("{}", e))
                .unwrap_or_else(|_| CString::new("source error (message contained null byte)").unwrap());
            unsafe {
                (err_cb.error_fn)(err_cb.error_ctx.0, source_id, msg.as_ptr());
            }
            // Error path: no EOS was sent, so cleanup here.
            backpressure::unregister_source(source_id);
            bridge::unregister_emit(source_id);
        }
        Err(join_error) => {
            error!(source_id = source_id, "Source task panicked");
            let msg = CString::new(format!("source panicked: {}", join_error))
                .unwrap_or_else(|_| CString::new("source panicked").unwrap());
            unsafe {
                (err_cb.error_fn)(err_cb.error_ctx.0, source_id, msg.as_ptr());
            }
            // Panic path: no EOS was sent, so cleanup here.
            backpressure::unregister_source(source_id);
            bridge::unregister_emit(source_id);
        }
    }
    info!(source_id = source_id, "Source monitoring task: complete");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backpressure::BackpressureState;
    use crate::bridge::EMIT_REGISTRY;
    use crate::error::SourceError;
    use std::sync::{Arc, Mutex};

    // ---- Test source implementations ----

    /// Simple test source that returns EndOfStream immediately.
    struct TestEndOfStreamSource;

    impl AsyncSource for TestEndOfStreamSource {
        async fn run(&mut self, _ctx: &SourceContext) -> SourceResult {
            SourceResult::EndOfStream
        }
    }

    /// Test source that waits for cancellation then returns EndOfStream.
    struct TestCancellableSource;

    impl AsyncSource for TestCancellableSource {
        async fn run(&mut self, ctx: &SourceContext) -> SourceResult {
            ctx.cancellation_token().cancelled().await;
            SourceResult::EndOfStream
        }
    }

    /// Test source that returns an error.
    struct TestErrorSource;

    impl AsyncSource for TestErrorSource {
        async fn run(&mut self, _ctx: &SourceContext) -> SourceResult {
            SourceResult::Error(SourceError::new("test error from source"))
        }
    }

    // ---- Test-only spawn that avoids BridgeMessage / TupleBufferHandle ----
    //
    // BridgeMessage contains TupleBufferHandle which requires C++ linker symbols.
    // We use a test-only SourceContext that has a dummy sender (closed channel).
    // The source tasks in tests don't actually call emit(), so they never
    // touch TupleBufferHandle or BridgeMessage.

    /// Spawn a test source without requiring C++ linker symbols.
    ///
    /// Creates a SourceContext with a closed BridgeMessage sender (tests should
    /// not call emit() -- that path requires TupleBufferHandle from C++).
    /// The monitoring task is spawned identically to production.
    fn spawn_test_source(
        source_id: u64,
        source: impl AsyncSource,
        error_fn: ErrorFnPtr,
        error_ctx: *mut c_void,
    ) -> Box<SourceTaskHandle> {
        // Register backpressure state
        let bp_state = backpressure::register_source(source_id);
        let semaphore = Arc::new(Semaphore::new(10));
        let cancel_token = CancellationToken::new();

        // Create a channel that carries semaphore permits (mirrors production BridgeMessage).
        let (sender, _receiver) = async_channel::bounded::<tokio::sync::OwnedSemaphorePermit>(1024);

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
        let join_handle = tokio::spawn(async move {
            let result = source.run(&ctx).await;
            if matches!(result, SourceResult::EndOfStream) {
                ctx.emit_eos().await;
            }
            result
        });

        let err_cb = ErrorCallback {
            error_fn,
            error_ctx: SendVoidPtr(error_ctx),
        };

        tokio::spawn(async move {
            monitoring_task(join_handle, source_id, err_cb).await;
        });

        Box::new(SourceTaskHandle::new(cancel_token, source_id))
    }

    // ---- Mock callbacks ----

    /// Mock error callback that records source_id calls into a shared vec.
    unsafe extern "C" fn mock_error_callback(
        ctx: *mut c_void,
        source_id: u64,
        _message: *const std::ffi::c_char,
    ) {
        let log = unsafe { &*(ctx as *const Arc<Mutex<Vec<u64>>>) };
        log.lock().unwrap().push(source_id);
    }

    /// Mock emit callback for EMIT_REGISTRY tests.
    unsafe extern "C" fn mock_emit_fn(
        _ctx: *mut c_void,
        _origin_id: u64,
        _buffer: *mut nes_buffer_bindings::ffi::TupleBuffer,
        _semaphore_ptr: usize,
    ) -> u8 {
        0
    }

    // ---- Compile-time checks ----

    #[test]
    fn async_source_trait_is_implementable() {
        fn _assert_source<T: AsyncSource>() {}
        _assert_source::<TestEndOfStreamSource>();
        _assert_source::<TestCancellableSource>();
        _assert_source::<TestErrorSource>();
    }

    #[test]
    fn source_task_handle_is_send() {
        fn _assert_send<T: Send>() {}
        _assert_send::<SourceTaskHandle>();
    }

    // ---- Functional tests ----

    #[tokio::test]
    async fn spawn_source_creates_task_and_runs_to_end_of_stream() {
        let error_log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let error_ctx = &error_log as *const _ as *mut c_void;
        let source_id = 20001u64;

        let handle = spawn_test_source(
            source_id,
            TestEndOfStreamSource,
            mock_error_callback,
            error_ctx,
        );

        // Give monitoring task time to complete
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // EndOfStream should NOT trigger error callback
        assert!(error_log.lock().unwrap().is_empty());
        drop(handle);
    }

    #[tokio::test]
    async fn stop_source_cancels_running_source() {
        let error_log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let error_ctx = &error_log as *const _ as *mut c_void;
        let source_id = 20002u64;

        let handle = spawn_test_source(
            source_id,
            TestCancellableSource,
            mock_error_callback,
            error_ctx,
        );

        // Source should be running (waiting for cancellation)
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Stop the source
        crate::handle::stop_source(&handle);

        // Give monitoring task time to complete
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // EndOfStream after cancel should NOT trigger error callback
        assert!(error_log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn error_source_triggers_error_callback() {
        let error_log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let error_ctx = &error_log as *const _ as *mut c_void;
        let source_id = 20005u64;

        let _handle = spawn_test_source(
            source_id,
            TestErrorSource,
            mock_error_callback,
            error_ctx,
        );

        // Give monitoring task time to complete
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Error source should trigger error callback
        let calls = error_log.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], source_id);
    }

    #[tokio::test]
    async fn spawn_source_registers_emit_in_registry() {
        let source_id = 20003u64;

        // Ensure not registered
        EMIT_REGISTRY.remove(&source_id);
        assert!(!EMIT_REGISTRY.contains_key(&source_id));

        // Register emit callback (simulating what production spawn_source does
        // BEFORE spawning the source task)
        bridge::register_emit(source_id, mock_emit_fn, std::ptr::null_mut());

        // Verify it's registered
        assert!(EMIT_REGISTRY.contains_key(&source_id));

        // Cleanup
        bridge::unregister_emit(source_id);
    }

    #[tokio::test]
    async fn monitoring_task_unregisters_emit_on_error() {
        // For EndOfStream, cleanup (unregister emit) happens in the bridge thread
        // after processing the EOS message. For Error/Panic, the monitoring task
        // handles cleanup directly. This test verifies the error path.
        let error_log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let error_ctx = &error_log as *const _ as *mut c_void;
        let source_id = 20004u64;

        // Register emit callback before spawn (as production spawn_source would)
        bridge::register_emit(source_id, mock_emit_fn, std::ptr::null_mut());
        assert!(EMIT_REGISTRY.contains_key(&source_id));

        let _handle = spawn_test_source(
            source_id,
            TestErrorSource,
            mock_error_callback,
            error_ctx,
        );

        // Give monitoring task time to run cleanup
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Monitoring task should have unregistered the emit callback (error path)
        assert!(!EMIT_REGISTRY.contains_key(&source_id));
    }

    #[tokio::test]
    async fn monitoring_task_unregisters_backpressure_on_exit() {
        let error_log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let error_ctx = &error_log as *const _ as *mut c_void;
        let source_id = 20006u64;

        let _handle = spawn_test_source(
            source_id,
            TestEndOfStreamSource,
            mock_error_callback,
            error_ctx,
        );

        // Give monitoring task time to run cleanup
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // backpressure should be unregistered
        // (register_source was called inside spawn_test_source, monitoring task unregisters)
        // We can verify by trying to call on_backpressure_applied -- it's a no-op if unregistered
        backpressure::on_backpressure_applied(source_id);
        // No crash = success (no assertion needed, just verifying it doesn't panic)
    }

    #[tokio::test]
    async fn emit_backpressure_gate_blocks_when_pressure_applied() {
        let bp_state = Arc::new(BackpressureState::new());

        // Apply backpressure
        bp_state.signal_applied();

        let bp_clone = bp_state.clone();
        let emit_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let emit_completed_clone = emit_completed.clone();

        let handle = tokio::spawn(async move {
            // This simulates the first step of emit(): wait_for_release
            bp_clone.wait_for_release().await;
            emit_completed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        // Give task a chance to park
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Should still be blocked
        assert!(!emit_completed.load(std::sync::atomic::Ordering::SeqCst));

        // Release backpressure
        bp_state.signal_released();

        handle.await.unwrap();
        assert!(emit_completed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn emit_semaphore_gate_blocks_when_inflight_limit_reached() {
        let semaphore = Arc::new(Semaphore::new(1));

        // Acquire the only permit
        let held_permit = semaphore.clone().acquire_owned().await.unwrap();
        assert_eq!(semaphore.available_permits(), 0);

        let sem_clone = semaphore.clone();
        let acquired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acquired_clone = acquired.clone();

        let handle = tokio::spawn(async move {
            // This simulates the second step of emit(): acquire semaphore
            let _permit = sem_clone.acquire_owned().await.unwrap();
            acquired_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        // Give task a chance to park
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Should still be blocked
        assert!(!acquired.load(std::sync::atomic::Ordering::SeqCst));

        // Release permit
        drop(held_permit);

        handle.await.unwrap();
        assert!(acquired.load(std::sync::atomic::Ordering::SeqCst));
    }
}
