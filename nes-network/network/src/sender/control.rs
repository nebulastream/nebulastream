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

use super::channel::{ChannelCommandQueue, ChannelCommandQueueListener, create_channel_handler};
use crate::channel::{Channel, Communication};
use crate::protocol::*;
use crate::util::{ActiveTokens, ScopedTask};
use futures::SinkExt;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;
use tokio_retry2::RetryError::Transient;
use tokio_retry2::strategy::{ExponentialBackoff, jitter};
use tokio_retry2::{Retry, RetryError};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, error, info, info_span, warn};

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Represents a channel that is pending registration or being retried.
///
/// This is the state of a channel that is not yet bound to an actual network connection
/// but is currently (re)connecting. Once the connection is established, the channel
/// handler will read from the queue and transmit data over the network. Note that the
/// software side of the queue may already be in use, buffering messages until the connection is ready.
#[derive(Clone)]
pub(super) struct PendingChannel {
    /// The software-facing unique identifier for this channel
    pub id: ChannelIdentifier,
    /// Token used to cancel the channel registration attempt
    pub cancellation: CancellationToken,
    /// Command queue for receiving commands to send on this channel
    pub queue: ChannelCommandQueueListener,
}

/// Control commands sent to the network service dispatcher.
///
/// These commands are sent from `NetworkService::register_channel()` to request
/// new channel registrations.
pub(super) enum NetworkServiceControlCommand {
    /// Register a new channel to the specified connection and channel identifier.
    /// The oneshot sender is used to return the channel's command queue.
    RegisterChannel(
        ConnectionIdentifier,
        ChannelIdentifier,
        oneshot::Sender<ChannelCommandQueue>,
    ),
}

/// Control commands sent to a connection handler.
///
/// These commands manage the lifecycle of data channels on a specific connection.
///
/// # Relationship to `NetworkServiceControlCommand`
///
/// `NetworkServiceControlCommand::RegisterChannel` is dispatched to the appropriate
/// connection handler and then uses `NetworkingConnectionControlCommand::RegisterChannel`
/// to register the channel on that specific connection.
pub(super) enum NetworkingConnectionControlCommand {
    /// Register a new channel on this connection. The oneshot sender returns
    /// the channel's command queue once registration completes.
    RegisterChannel(ChannelIdentifier, oneshot::Sender<ChannelCommandQueue>),
    /// Retry establishing a channel after a failure. This reuses the existing
    /// cancellation token and command queue from the failed attempt.
    RetryChannel(PendingChannel),
}
pub(super) type NetworkServiceController = async_channel::Sender<NetworkServiceControlCommand>;
type NetworkServiceControlListener = async_channel::Receiver<NetworkServiceControlCommand>;
pub(super) type NetworkingConnectionController =
    async_channel::Sender<NetworkingConnectionControlCommand>;
type NetworkingConnectionControlListener =
    async_channel::Receiver<NetworkingConnectionControlCommand>;

pub(super) async fn connect<C: Communication>(
    target_identifier: ConnectionIdentifier,
    communication: C,
) -> Result<Channel<C::Reader, C::Writer>> {
    communication
        .connect(&target_identifier)
        .await
        .map_err(|e| format!("Could not connect to {target_identifier}: {e}").into())
}

/// Result of attempting to establish a channel via the control channel.
pub(super) enum EstablishChannelResult {
    /// Channel was accepted. Contains the receiver's connection identifier.
    Ok(ConnectionIdentifier),
    /// Channel was explicitly rejected by the receiver.
    ChannelReject,
    /// Connection failed during the negotiation. Contains channel ID and error.
    BadConnection(ChannelIdentifier, Error),
}

/// Negotiates channel establishment with the receiver via the control channel.
///
/// This function performs the control channel handshake to request permission
/// to create a new data channel. It sends a `ChannelRequest` and waits for
/// either acceptance or rejection from the receiver.
///
/// # Protocol Flow
///
/// 1. Send `ChannelRequest` with channel identifier to receiver
/// 2. Wait for response:
///    - `OkChannelResponse(connection_id)`: Receiver accepted, proceed to create data channel
///    - `DenyChannelResponse`: Receiver rejected the channel
/// 3. Return result to caller
///
/// # Returns
///
/// - `Ok(connection_id)`: Channel accepted, use this ID for data channel connection
/// - `ChannelReject`: Receiver denied the channel request
/// - `BadConnection`: Network/protocol error during negotiation
pub(super) async fn establish_channel<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    control_channel_sender_writer: &mut ControlChannelSenderWriter<W>,
    control_channel_sender_reader: &mut ControlChannelSenderReader<R>,
    channel_id: ChannelIdentifier,
) -> EstablishChannelResult {
    if let Err(e) = control_channel_sender_writer
        .send(ControlChannelRequest::ChannelRequest(channel_id.clone()))
        .await
    {
        return EstablishChannelResult::BadConnection(
            channel_id,
            format!("Could not send channel creation request {}", e).into(),
        );
    };

    let Some(channel_request_result) = control_channel_sender_reader.next().await else {
        return EstablishChannelResult::BadConnection(channel_id, "Connection closed".into());
    };

    let response = match channel_request_result {
        Ok(response) => response,
        Err(e) => {
            return EstablishChannelResult::BadConnection(channel_id, e.into());
        }
    };

    let channel_connection_identifier = match response {
        ControlChannelResponse::OkChannelResponse(channel_connection_identifier) => {
            channel_connection_identifier
        }
        ControlChannelResponse::DenyChannelResponse => {
            warn!("Channel '{channel_id}' was rejected");
            return EstablishChannelResult::ChannelReject;
        }
    };

    EstablishChannelResult::Ok(channel_connection_identifier)
}

async fn create_connection<C: Communication>(
    this_connection: ThisConnectionIdentifier,
    target_identifier: ConnectionIdentifier,
    communication: C,
) -> core::result::Result<
    (
        ControlChannelSenderReader<C::Reader>,
        ControlChannelSenderWriter<C::Writer>,
    ),
    RetryError<Error>,
> {
    let stream = connect(target_identifier, communication).await?;
    let (mut read, mut write) = identification_sender(stream);
    if let Err(e) = write
        .send(IdentificationRequest::IAmConnection(this_connection))
        .await
    {
        return Err(Transient {
            err: e.into(),
            retry_after: None,
        });
    }

    let Some(response) = read.next().await else {
        return Err(Transient {
            err: "Connection closed during identification".into(),
            retry_after: None,
        });
    };

    let response = match response {
        Ok(response) => response,
        Err(e) => {
            return Err(Transient {
                err: e.into(),
                retry_after: None,
            });
        }
    };

    let stream = Channel {
        writer: write.into_inner().into_inner(),
        reader: read.into_inner().into_inner(),
    };

    match response {
        IdentificationResponse::Ok => Ok(control_channel_sender(stream)),
    }
}

type EstablishChannelRequest =
    tokio::sync::mpsc::Sender<(ChannelIdentifier, oneshot::Sender<EstablishChannelResult>)>;

/// Attempts to register a channel with exponential backoff retry on failure.
///
/// This function coordinates with the connection's control channel task to
/// negotiate a new data channel with the receiver. The actual negotiation
/// happens in a dedicated task that owns the control channel socket to avoid
/// concurrent access conflicts.
///
/// # Control Channel Lifetime vs Network Connection
///
/// The control channel's lifetime is tied to the **logical connection** (identified
/// by `ConnectionIdentifier`), NOT to the underlying network connection. The logical
/// connection is created on first channel registration and currently persists until
/// the `NetworkService` shuts down, even if all data channels on this connection close.
///
/// A dedicated keepalive task manages the physical network connection and can
/// re-establish it on demand when needed for new channel registrations, providing
/// transparent reconnection without disrupting the logical connection state.
///
/// # Architecture - Single Socket Owner
///
/// Instead of directly accessing the control channel socket, this function sends
/// requests to a dedicated task via `channel_tx`. That task exclusively owns the
/// socket and serializes all channel establishment requests, preventing multiple
/// concurrent tasks from competing for socket access.
///
/// # Retry Strategy
///
/// Transient failures (connection issues, channel rejection) trigger retry with
/// exponential backoff (2ms initial, 500ms max, with jitter). Permanent failures
/// (network service shutdown) cause immediate return without spawning a handler.
///
/// On successful negotiation, the receiver provides a connection identifier for
/// the new data channel, and this function spawns the channel handler.
async fn attempt_channel_registration<C: Communication + 'static>(
    this_connection: ThisConnectionIdentifier,
    pending_channel: PendingChannel,
    channel_tx: EstablishChannelRequest,
    controller: NetworkingConnectionController,
    communication: C,
) {
    let retry = ExponentialBackoff::from_millis(2)
        .max_delay_millis(500)
        .map(jitter);

    let target_channel_identifier = Retry::spawn(
        retry,
        async || -> core::result::Result<ConnectionIdentifier, RetryError<Error>> {
            let (tx, rx) = oneshot::channel();
            if channel_tx
                .send((pending_channel.id.clone(), tx))
                .await
                .is_err()
            {
                return Err(RetryError::Permanent("NetworkService Shutdown".into()));
            }

            let Ok(result) = rx.await else {
                return Err(RetryError::Permanent("NetworkService Shutdown".into()));
            };

            match result {
                EstablishChannelResult::Ok(channel_connection_identifier) => {
                    Ok(channel_connection_identifier)
                }
                EstablishChannelResult::ChannelReject => Err(Transient {
                    err: "Channel was rejected".into(),
                    retry_after: None,
                }),
                EstablishChannelResult::BadConnection(_, _) => Err(Transient {
                    err: "Bad connection".into(),
                    retry_after: None,
                }),
            }
        },
    )
    .await;

    let Ok(target_channel_identifier) = target_channel_identifier else {
        return;
    };

    create_channel_handler(
        this_connection,
        target_channel_identifier,
        pending_channel,
        communication,
        controller,
    );
}

/// Handles all channel registrations and retries for a specific connection.
///
/// # Parameters
///
/// - `controller`: The sender half of the command channel. This is cloned and passed to
///   channel handlers so they can send `RetryChannel` commands back when a channel fails,
///   enabling the retry mechanism.
/// - `listener`: The receiver half of the command channel. This receives commands from
///   the network service dispatcher.
///
/// # Termination
///
/// This future terminates when `listener.recv()` returns an error, which happens when
/// the sender half (`controller`) is dropped. This occurs when the `connections` HashMap
/// in `network_sender_dispatcher` is dropped, which happens when the network service
/// is shut down (the control channel is closed).
async fn connection_handler<C: Communication + 'static>(
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
    controller: NetworkingConnectionController,
    listener: NetworkingConnectionControlListener,
    communication: C,
) -> Result<()> {
    let (request_connection, mut await_connection_request) = tokio::sync::mpsc::channel::<
        oneshot::Sender<(
            ControlChannelSenderReader<C::Reader>,
            ControlChannelSenderWriter<C::Writer>,
        )>,
    >(1);

    let (channel_registration_request_handler, mut await_channel_registration_request) =
        tokio::sync::mpsc::channel::<(ChannelIdentifier, oneshot::Sender<EstablishChannelResult>)>(
            10,
        );

    // Connection Keepalive Task:
    // This task is responsible for establishing and maintaining connections to the target_connection.
    // It implements exponential backoff for connection retries.
    // Once a connection has been established, it sends the Reader/Writer pair through `await_connection`
    // The task waits for new connection requests via `await_connection_request` and attempts
    // to reestablish connections when needed.
    let _connection_keep_alive = ScopedTask::new(tokio::spawn(
        {
            let target_connection = target_connection.clone();
            let this_connection = this_connection.clone();
            let communication = communication.clone();
            async move {
                while let Some(await_connection) = await_connection_request.recv().await {
                    let retry = ExponentialBackoff::from_millis(2)
                        .max_delay_millis(500)
                        .map(jitter);

                    let connection = Retry::spawn(retry, || async {
                        create_connection(
                            this_connection.clone(),
                            target_connection.clone(),
                            communication.clone(),
                        )
                        .await
                    })
                    .await;
                    let (reader, writer) = match connection {
                        Ok(connection) => connection,
                        Err(e) => {
                            error!(
                                "Could not establish connection to {}: {e:?}",
                                target_connection
                            );
                            return;
                        }
                    };

                    info!("Connection to {} was established", target_connection);
                    let _ = await_connection.send((reader, writer));
                }
            }
        }
        .in_current_span(),
    ));

    // Handling Request on the NetworkingConnectionListener is broken up into two separate tasks.
    // This task requests connections and then establishes DataChannels on those connections
    // once the underlying TCP connection is ready.
    // Spawn a task to handle channel establishment requests
    tokio::spawn({
        async move {
            'connection: loop {
                let (tx, rx) = oneshot::channel();
                // Wait until the KeepAlive task creates a connection
                request_connection
                    .send(tx)
                    .await
                    .expect("Connection Task should not have aborted");
                let (mut reader, mut writer) =
                    rx.await.expect("Connection Task should not have aborted");

                loop {
                    let Some((channel, response)) = await_channel_registration_request.recv().await
                    else {
                        return;
                    };
                    match establish_channel(&mut writer, &mut reader, channel.clone()).await {
                        // Channel could not be established, notify caller and reconnect
                        EstablishChannelResult::BadConnection(c, ct) => {
                            let _ = response.send(EstablishChannelResult::BadConnection(c, ct));
                            // If establishing a channel fails because of a networking issue, we
                            // have to reconnect first
                            continue 'connection;
                        }
                        other_result => {
                            let _ = response.send(other_result);
                        }
                    }
                }
            }
        }
        .in_current_span()
    });

    let mut active_channel = ActiveTokens::default();
    while let Ok(control_message) = listener.recv().await {
        match control_message {
            NetworkingConnectionControlCommand::RegisterChannel(channel, response) => {
                let (sender, queue) = async_channel::bounded(1024);
                let channel_cancellation = CancellationToken::new();

                let pending_channel = PendingChannel {
                    id: channel,
                    cancellation: channel_cancellation.clone(),
                    queue,
                };

                tokio::spawn(
                    attempt_channel_registration(
                        this_connection.clone(),
                        pending_channel,
                        channel_registration_request_handler.clone(),
                        controller.clone(),
                        communication.clone(),
                    )
                    .in_current_span(),
                );

                active_channel.add_token(channel_cancellation);
                let _ = response.send(sender);
            }
            NetworkingConnectionControlCommand::RetryChannel(pending_channel) => {
                tokio::spawn(
                    attempt_channel_registration(
                        this_connection.clone(),
                        pending_channel,
                        channel_registration_request_handler.clone(),
                        controller.clone(),
                        communication.clone(),
                    )
                    .in_current_span(),
                );
            }
        }
    }
    // NetworkService was closed
    Ok(())
}
/// Creates a connection handler task for managing channels on a specific connection.
///
/// Returns both the spawned task and the controller (sender) for sending commands to
/// the connection handler. The controller is cloned and passed into the handler itself
/// so that channel handlers can send `RetryChannel` commands back when they fail,
/// enabling the retry mechanism.
fn create_connection_handler(
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
    communication: impl Communication + 'static,
) -> (ScopedTask<()>, NetworkingConnectionController) {
    let (tx, rx) = async_channel::bounded::<NetworkingConnectionControlCommand>(1024);
    let control = tx.clone();
    let task = ScopedTask::new(tokio::spawn(
        {
            let target_connection = target_connection.clone();
            async move {
                info!(
                    "Connection is terminated: {:?}",
                    connection_handler(
                        this_connection,
                        target_connection,
                        control, // Pass controller clone so channel handlers can send RetryChannel commands
                        rx,
                        communication
                    )
                    .await
                );
            }
        }
        .instrument(info_span!(
                parent: Span::current(),
            "connection",
            other = %target_connection
        )),
    ));
    (task, tx)
}

/// Root future that drives the entire sender networking machinery.
///
/// This is the main event loop for the sender side of the network service. It receives
/// channel registration requests from the software side, manages all connection handlers,
/// and dispatches commands to the appropriate connection handlers. This function runs
/// until the service is shut down (when the control channel is closed).
pub(super) async fn network_sender_dispatcher(
    this_connection: ThisConnectionIdentifier,
    control: NetworkServiceControlListener,
    communication: impl Communication + 'static,
) -> Result<()> {
    // All currently active connections. Dropping the hashmap will abort all active connections.
    // TODO: Currently, connections are never removed from this map, even when all channels
    // on a connection have closed. This means connection handlers and keepalive tasks persist
    // until NetworkService shutdown. Consider implementing cleanup when ActiveTokens becomes
    // empty to avoid resource leaks for short-lived connections.
    let mut connections: HashMap<
        ConnectionIdentifier,
        (ScopedTask<()>, NetworkingConnectionController),
    > = HashMap::default();

    // Consume `RegisterChannel` requests from the worker and dispatch them to a dedicated handler
    while let Ok(NetworkServiceControlCommand::RegisterChannel(target_connection, channel, tx)) =
        control.recv().await
    {
        // if the new channel is on a connection, which has not yet been created, a new connection
        // handler is created before submitting the channel registration to the connection handler
        let (_, controller) = connections.entry(target_connection.clone()).or_insert({
            info!("Creating connection to {}", target_connection);
            let (task, controller) = create_connection_handler(
                this_connection.clone(),
                target_connection.clone(),
                communication.clone(),
            );
            (task, controller)
        });

        // Dispatch the request to the correct connection handler
        controller
            .send(NetworkingConnectionControlCommand::RegisterChannel(
                channel, tx,
            ))
            .await
            // The Connection Controller should never terminate but instead attempt
            // to reconnect. The only reason a connection controller would terminate is
            // if it has been removed from the `connections` map.
            .expect("BUG: Connection should not have been terminated");
    }

    // The software side was closed, which means that the NetworkingService was dropped.
    Err("Queue was closed".into())
}
