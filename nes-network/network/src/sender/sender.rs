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
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tracing::{Instrument, debug, info_span};

/// Configuration for sender-side network channels.
#[derive(Clone, Debug)]
pub struct SenderConfig {
    /// Size of the software queue between C++ and the Rust channel handler.
    pub sender_queue_size: usize,
    /// Maximum number of buffers that can be in-flight (sent but not yet acknowledged).
    pub max_pending_acks: usize,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            sender_queue_size: 1024,
            max_pending_acks: 64,
        }
    }
}

/// A handle to a registered network channel for sending tuple buffers.
///
/// `SenderChannel` represents an active data channel to a specific `ReceiverChannel`.
/// Each channel maintains its own dedicated network connection with independent
/// backpressure and flow control. Send and flush operations are async and apply
/// backpressure by parking the caller when the internal queue is full.
pub struct SenderChannel {
    queue: ChannelCommandQueue,
}
impl SenderChannel {
    /// Sends a tuple buffer through this channel, awaiting queue capacity.
    ///
    /// Returns `Err(buffer)` if the channel has been closed.
    pub async fn send_data(&self, buffer: TupleBuffer) -> std::result::Result<(), TupleBuffer> {
        match self.queue.send(ChannelCommand::Data(buffer)).await {
            Ok(()) => Ok(()),
            Err(async_channel::SendError(ChannelCommand::Data(buffer))) => Err(buffer),
            Err(_) => unreachable!(),
        }
    }

    /// Flushes the network writer and waits for all pending data to be acknowledged.
    pub async fn flush_async(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.queue
            .send(ChannelCommand::Flush(tx))
            .await
            .map_err(|_| "Network Service Closed")?;
        rx.await.map_err(|_| "Network Service Closed".into())
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
pub struct NetworkService<C: Communication> {
    controller: NetworkServiceController,
    communication: PhantomData<C>,
}
impl<C: Communication + 'static> NetworkService<C> {
    /// Starts the network service on the provided tokio runtime.
    ///
    /// Spawns the main network dispatcher task on `runtime`. The service does
    /// not own the runtime; lifetime of spawned tasks is bounded by the runtime
    /// and by cascading cancellation when this service's controller is closed.
    pub fn start(
        runtime: &Handle,
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
    pub async fn register_channel(
        self: &Arc<NetworkService<C>>,
        connection: ConnectionIdentifier,
        channel: ChannelIdentifier,
        channel_config: SenderConfig,
    ) -> Result<SenderChannel> {
        let (tx, rx) = oneshot::channel();
        self.controller
            .send(NetworkServiceControlCommand::RegisterChannel(
                connection,
                channel,
                tx,
                channel_config,
            ))
            .await
            .map_err(|_| "Network Service Closed")?;

        let internal = rx.await.map_err(|_| "Network Service Closed")?;
        Ok(SenderChannel { queue: internal })
    }

    /// Shuts down the network service and all active channels.
    ///
    /// Closes the control queue; the dispatcher task observes the close, drops
    /// all connection handlers, and the layered cancellation described above
    /// cascades through to every channel handler. The shared tokio runtime is
    /// owned elsewhere and is unaffected.
    pub fn shutdown(self: Arc<NetworkService<C>>) -> Result<()> {
        self.controller.close();
        Ok(())
    }
}
