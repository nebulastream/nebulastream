/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

use super::control::*;
use crate::channel::Communication;
use crate::protocol::*;
use crate::sender::channel::{ChannelCommand, ChannelCommandQueue};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing::{Instrument, debug, info_span};

/// A handle to a registered network channel for sending tuple buffers.
///
/// `SenderChannel` represents an active data channel to a specific `ReceiverChannel`.
/// Each channel maintains its own dedicated network connection with independent
/// backpressure and flow control. It provides non-blocking send operations and
/// explicit flush control with acknowledgment-based reliability.
///
/// # Backpressure Handling
///
/// The channel has a bounded internal queue. When the queue is full, `try_send_data`
/// returns `TrySendDataResult::Full`, allowing the caller to apply backpressure or
/// buffer data locally.
///
/// # Example
///
/// ```ignore
/// let channel = network_service.register_channel(
///     connection_id,
///     channel_id,
/// )?;
///
/// match channel.try_send_data(buffer) {
///     TrySendDataResult::Ok => {},
///     TrySendDataResult::Full(buffer) => {
///         // Apply backpressure
///     },
///     TrySendDataResult::Closed(buffer) => {
///         // Channel was closed
///     },
/// }
///
/// // Ensure all data is sent and acknowledged
/// channel.flush()?;
/// ```
pub struct SenderChannel {
    queue: ChannelCommandQueue,
}

/// Result of a non-blocking send operation on a `SenderChannel`.
pub enum TrySendDataResult {
    /// The buffer was successfully queued for sending.
    Ok,
    /// The channel's internal queue is full. The buffer is returned to the caller.
    Full(TupleBuffer),
    /// The channel is closed. The buffer is returned to the caller.
    Closed(TupleBuffer),
}
impl SenderChannel {
    /// Attempts to send a tuple buffer through this channel without blocking.
    ///
    /// This method will immediately return with one of three results:
    /// - `Ok`: The buffer was successfully queued for sending
    /// - `Full(buffer)`: The internal queue is full, buffer returned to caller
    /// - `Closed(buffer)`: The channel is closed, buffer returned to caller
    ///
    /// # Backpressure
    ///
    /// When `Full` is returned, the caller should apply backpressure to avoid
    /// unbounded memory growth. The buffer is returned so it can be retried later
    /// or handled appropriately.
    pub fn try_send_data(&self, buffer: TupleBuffer) -> TrySendDataResult {
        let result = self.queue.try_send(ChannelCommand::Data(buffer));
        match result {
            Ok(()) => TrySendDataResult::Ok,
            // Cannot send more data, return the buffer back to the caller for retry
            Err(async_channel::TrySendError::Full(ChannelCommand::Data(buffer))) => {
                TrySendDataResult::Full(buffer)
            }
            // Channel was closed (e.g., receiver disconnected, network service shutdown, or explicit close).
            // The buffer is returned so the caller can handle it appropriately.
            Err(async_channel::TrySendError::Closed(ChannelCommand::Data(buffer))) => {
                TrySendDataResult::Closed(buffer)
            }
            _ => unreachable!(),
        }
    }
    /// Flushes the network writer and checks if all data has been acknowledged.
    ///
    /// This blocking operation flushes the underlying network stream to ensure
    /// buffered data is sent, then checks the state of the channel queues.
    ///
    /// # Returns
    ///
    /// - `Ok(true)`: All data has been sent and acknowledged (both pending and in-flight queues are empty)
    /// - `Ok(false)`: Flush completed but data is still pending or awaiting acknowledgment
    /// - `Err(_)`: The network service or channel was closed
    ///
    /// # Blocking
    ///
    /// This method blocks the calling thread. Do not call from async contexts without proper handling.
    pub fn flush(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.queue
            .send_blocking(ChannelCommand::Flush(tx))
            .map_err(|_| "Network Service Closed")?;
        rx.blocking_recv()
            .map_err(|_| "Network Service Closed".into())
    }

    /// Closes this channel and propagates the close signal to the `ReceiverChannel`.
    ///
    /// This method initiates graceful shutdown of the channel. The channel handler
    /// will attempt to send a `Close` message to the receiver before terminating
    /// the connection.
    ///
    /// # Returns
    ///
    /// - `true`: The channel was successfully closed
    /// - `false`: The channel was already closed
    ///
    /// Note: The return value only indicates whether the local queue was closed,
    /// not whether the close signal was successfully propagated to the receiver.
    pub fn close(self) -> bool {
        self.queue.close()
    }
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// The main network service for managing sender channels.
///
/// `NetworkService` manages the lifecycle of network connections and channels.
/// It owns a Tokio runtime that handles all async networking operations in the
/// background. Channels are registered on-demand, and each maintains its own
/// dedicated connection with independent flow control.
///
/// # Lifecycle
///
/// 1. Create with `start()`, providing a runtime and connection identifier
/// 2. Register channels using `register_channel()`
/// 3. Send data through the returned `SenderChannel` handles
/// 4. Call `shutdown()` to gracefully stop the service
///
/// # Threading Model
///
/// The service takes ownership of the provided Tokio runtime and spawns async
/// tasks for connection management and data transmission. All send operations
/// on `SenderChannel` communicate with these background tasks via bounded queues.
///
/// # Ownership and Cancellation Model
///
/// The service uses a hierarchical ownership model with layered cancellation:
///
/// ```text
/// NetworkService
///   └─► Connection Handlers (one per target connection)
///         ├─► Control Channel (connection establishment/retry)
///         └─► Data Channels (one per registered channel)
///               └─► Channel Handler (data transmission)
/// ```
///
/// ## Ownership Hierarchy
///
/// A **connection handler owns its data channels** via `ActiveTokens`, which holds
/// the cancellation tokens for all channels on that connection. When a connection
/// handler is dropped, it automatically cancels all its data channels.
///
/// ## Layered Cancellation Strategy
///
/// Different layers use different cancellation mechanisms, designed to cascade
/// gracefully from outer-to-inner layers:
///
/// ### Layer 1: Connection Handlers - Preemptive Cancellation
///
/// Connection handlers use `ScopedTask`, which enables **preemptive cancellation**
/// (via `abort()`). When a connection handler is dropped:
///
/// 1. The control channel task is aborted at its next await point
/// 2. `ActiveTokens` is dropped
/// 3. All data channel cancellation tokens are canceled
///
/// **Why preemptive?** Connection handlers manage metadata and retry logic,
/// not user data. Immediate cancellation is safe and enables fast cleanup.
///
/// ### Layer 2: Data Channel Handlers - Cooperative Cancellation
///
/// Data channel handlers use `CancellationToken` for **cooperative cancellation**.
/// They check the token at well-defined points:
///
/// - Reading from network (waiting for Ack/Nack from receiver)
/// - Reading from software queue (waiting for Data/Flush commands)
/// - Sending data to the network
/// - Flushing the network writer
///
/// **Why cooperative?** Data channels transmit user buffers and must maintain
/// protocol consistency. Cooperative cancellation ensures:
///
/// - Buffers are properly cleaned up (moved from `wait_for_ack` to `pending_writes`)
/// - A `Close` message can be sent to the receiver
/// - No partial writes or inconsistent protocol state
///
/// ## Shutdown Sequence
///
/// When `NetworkService::shutdown()` is called:
///
/// 1. Runtime shutdown begins (1-second timeout)
/// 2. Main dispatcher task stops
/// 3. Connection handlers are dropped → preemptive cancellation
/// 4. `ActiveTokens::drop()` cancels all channel tokens
/// 5. Data channels detect cancellation cooperatively
/// 6. Channels clean up and attempt to send `Close` to receivers
/// 7. After timeout, any stuck tasks are forcefully terminated
///
/// ## Channel Closure via SenderChannel
///
/// When a `SenderChannel` is closed (explicitly or via drop):
///
/// - The command queue is closed
/// - Channel handler detects closed queue at next receive
/// - Handler sends `Close` message to receiver
/// - Handler terminates with `ClosedBySoftware` status
///
/// # Example
///
/// ```ignore
/// let runtime = Runtime::new()?;
/// let service = NetworkService::start(
///     runtime,
///     "localhost:8080".parse()?,
///     TcpCommunication::default(),
/// );
///
/// let channel = service.register_channel(
///     "localhost:9090".parse()?,
///     "my-channel".to_string(),
/// )?;
///
/// channel.try_send_data(buffer)?;
/// service.shutdown()?;
/// ```
pub struct NetworkService<C: Communication> {
    runtime: Mutex<Option<Runtime>>,
    controller: NetworkServiceController,
    communication: PhantomData<C>,
}
impl<C: Communication + 'static> NetworkService<C> {
    /// Starts the network service with the provided runtime and configuration.
    ///
    /// This method takes ownership of the Tokio runtime and spawns the main
    /// network dispatcher task. The service runs in the background, handling
    /// connection establishment, channel registration, and data transmission.
    ///
    /// # Parameters
    ///
    /// - `runtime`: The Tokio runtime that will execute all async networking tasks
    /// - `this_connection`: The identifier for this service instance
    /// - `communication`: The communication layer (e.g., TCP, in-memory) to use
    ///
    /// # Returns
    ///
    /// An `Arc<NetworkService>` that can be shared across threads and used to
    /// register channels.
    pub fn start(
        runtime: Runtime,
        this_connection: ThisConnectionIdentifier,
        communication: C,
    ) -> Arc<NetworkService<C>> {
        let (controller, listener) = async_channel::bounded(20);
        runtime.spawn(
            {
                let this_connection = this_connection.clone();
                let communication = communication.clone();
                async move {
                    debug!("Starting sender network service");
                    debug!(
                        "sender network service stopped: {:?}",
                        network_sender_dispatcher(this_connection, listener, communication).await
                    );
                }
            }
            .instrument(info_span!("sender", this = %this_connection)),
        );

        Arc::new(NetworkService {
            runtime: Mutex::new(Some(runtime)),
            controller,
            communication: Default::default(),
        })
    }

    /// Registers a new channel to the specified connection.
    ///
    /// This method creates a new `SenderChannel` that will establish its own
    /// dedicated network connection to the target. The connection is established
    /// asynchronously in the background with automatic retry on failure.
    ///
    /// # Parameters
    ///
    /// - `connection`: The target connection identifier (e.g., "localhost:9090")
    /// - `channel`: A unique identifier for this channel
    ///
    /// # Returns
    ///
    /// Register a new communication channel to the remote peer with address `connection`.
    ///
    /// This function returns a `SenderChannel` immediately, regardless of whether the network
    /// connection has been established. Network messages are buffered if the connection has not
    /// yet been established. Reconnects are handled transparently by the network service.
    ///
    /// The returned `SenderChannel` can be used to write tuple buffer data to the corresponding
    /// `ReceiverChannel` on the target connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the network service has been shut down.
    pub fn register_channel(
        self: &Arc<NetworkService<C>>,
        connection: ConnectionIdentifier,
        channel: ChannelIdentifier,
    ) -> Result<SenderChannel> {
        // Use a Rust oneshot channel (internal communication primitive) to receive
        // the data channel handle from the network service
        let (tx, rx) = oneshot::channel();
        let Ok(_) = self
            .controller
            .send_blocking(NetworkServiceControlCommand::RegisterChannel(
                connection, channel, tx,
            ))
        else {
            return Err("Network Service Closed".into());
        };

        // Receive the internal queue handle for the data channel
        let internal = rx.blocking_recv().map_err(|_| "Network Service Closed")?;
        Ok(SenderChannel { queue: internal })
    }

    /// Shuts down the network service and all active channels.
    ///
    /// This method initiates graceful shutdown of the service by stopping the
    /// runtime. All spawned tasks (connection handlers, channel handlers) will
    /// be terminated. The method will wait up to 1 second for tasks to complete
    /// before forcefully shutting down.
    ///
    /// # Blocking
    ///
    /// This method blocks the calling thread for up to 1 second during shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if the service was already shut down.
    pub fn shutdown(self: Arc<NetworkService<C>>) -> Result<()> {
        let runtime = self
            .runtime
            .lock()
            .expect("BUG: Nothing should panic while holding the lock")
            .take()
            .ok_or("Networking Service was stopped")?;
        // Shutdown the runtime with a 1-second timeout. All spawned tasks (connection handlers,
        // channel handlers) are given up to 1 second to complete gracefully. After the timeout,
        // any remaining tasks are forcibly dropped/aborted.
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}
