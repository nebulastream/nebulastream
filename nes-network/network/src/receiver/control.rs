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

use super::channel::*;
use crate::channel::{Channel, Communication, CommunicationListener};
use crate::protocol::*;
use crate::util::*;
use futures::SinkExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::RwLock;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, error, info, info_span, warn};

pub(super) type Result<T> = std::result::Result<T, Error>;
pub(super) type Error = Box<dyn std::error::Error + Send + Sync>;
pub(super) enum NetworkServiceControlCommand {
    RetryChannel(ChannelIdentifier, DataQueue, CancellationToken),
    RegisterChannel(ChannelIdentifier, DataQueue, Sender<()>),
}
pub(super) type NetworkingServiceController = async_channel::Sender<NetworkServiceControlCommand>;
type RegisteredChannels = Arc<RwLock<HashMap<ChannelIdentifier, (DataQueue, CancellationToken)>>>;
type PendingChannels<R, W> = Arc<
    RwLock<
        HashMap<
            (ConnectionIdentifier, ChannelIdentifier),
            Sender<(DataChannelReceiverReader<R>, DataChannelReceiverWriter<W>)>,
        >,
    >,
>;

type NetworkingServiceControlListener = async_channel::Receiver<NetworkServiceControlCommand>;
enum ConnectionIdentification<R: AsyncRead, W: AsyncWrite> {
    Connection(
        ControlChannelReceiverReader<R>,
        ControlChannelReceiverWriter<W>,
        ConnectionIdentifier,
    ),
    Channel(
        DataChannelReceiverReader<R>,
        DataChannelReceiverWriter<W>,
        ConnectionIdentifier,
        ChannelIdentifier,
    ),
}

async fn identify_connection<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send>(
    stream: Channel<R, W>,
) -> Result<ConnectionIdentification<R, W>> {
    let (mut read, mut write) = identification_receiver(stream);
    let response = read
        .next()
        .await
        .ok_or("Connection Closed during Identification")??;

    write.send(IdentificationResponse::Ok).await?;

    let stream = Channel {
        writer: write.into_inner().into_inner(),
        reader: read.into_inner().into_inner(),
    };

    match response {
        IdentificationRequest::IAmConnection(identifier) => {
            let (read, write) = control_channel_receiver(stream);
            Ok(ConnectionIdentification::Connection(
                read,
                write,
                identifier.into(),
            ))
        }
        IdentificationRequest::IAmChannel(connection_identifier, channel_identifier) => {
            let (read, write) = data_channel_receiver(stream);
            Ok(ConnectionIdentification::Channel(
                read,
                write,
                connection_identifier.into(),
                channel_identifier,
            ))
        }
    }
}

struct ControllerState<R: AsyncRead, W: AsyncWrite> {
    registered_channels: RegisteredChannels,
    // Channels which have been opened by the network side but not have been picked up by the
    // software side.
    pending_channels: PendingChannels<R, W>,
}

impl<R: AsyncRead, W: AsyncWrite> ControllerState<R, W> {
    async fn add_pending_channel(
        &self,
        connection_identifier: ConnectionIdentifier,
        channel_identifier: ChannelIdentifier,
        attach_to_channel: oneshot::Sender<(
            DataChannelReceiverReader<R>,
            DataChannelReceiverWriter<W>,
        )>,
    ) {
        let replaced = self.pending_channels.write().await.insert(
            (connection_identifier, channel_identifier),
            attach_to_channel,
        );
        assert!(
            replaced.is_none(),
            "There should not be a pending channel with the same identifier"
        )
    }
    async fn add_registered_channel(
        &self,
        channel_identifier: ChannelIdentifier,
        data_queue: DataQueue,
        cancellation_token: CancellationToken,
    ) {
        let mut lock = self.registered_channels.write().await;
        let replaced = lock.insert(channel_identifier, (data_queue, cancellation_token));
        lock.retain(|_, v| !v.1.is_cancelled());

        assert!(
            replaced.is_none(),
            "There should not be a registered channel with the same identifier"
        )
    }
    async fn take_registered_channel(
        &self,
        channel_identifier: &ChannelIdentifier,
    ) -> Option<(DataQueue, CancellationToken)> {
        self.registered_channels
            .write()
            .await
            .remove(channel_identifier)
    }

    async fn take_pending_channel(
        &self,
        connection_identifier: ConnectionIdentifier,
        channel_identifier: ChannelIdentifier,
    ) -> Option<Sender<(DataChannelReceiverReader<R>, DataChannelReceiverWriter<W>)>> {
        self.pending_channels
            .write()
            .await
            .remove(&(connection_identifier, channel_identifier))
    }
}

impl<R: AsyncRead, W: AsyncWrite> Default for ControllerState<R, W> {
    fn default() -> Self {
        ControllerState {
            registered_channels: Default::default(),
            pending_channels: Default::default(),
        }
    }
}

impl<R: AsyncRead, W: AsyncWrite> Clone for ControllerState<R, W> {
    fn clone(&self) -> Self {
        ControllerState {
            registered_channels: self.registered_channels.clone(),
            pending_channels: self.pending_channels.clone(),
        }
    }
}

async fn control_socket_handler<
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
>(
    this_connection_identifier: ThisConnectionIdentifier,
    other_connection_identifier: ConnectionIdentifier,
    mut reader: ControlChannelReceiverReader<R>,
    mut writer: ControlChannelReceiverWriter<W>,
    state: ControllerState<R, W>,
    control: NetworkingServiceController,
) -> Result<ControlChannelRequest> {
    loop {
        let Some(Ok(message)) = reader.next().await else {
            return Err("Connection Closed".into());
        };

        match message {
            // The other side is requesting a new data channel
            ControlChannelRequest::ChannelRequest(channel) => {
                // The data channel needs to be registered beforehand, which is done by the software
                // side via the NetworkingServiceController, which is exposed to the NetworkService.
                let Some((emit, token)) = state.take_registered_channel(&channel).await else {
                    // If the channel has not been registered yet, which is plausible because network sources and sinks
                    // are started without synchronization, the ChannelRequest is denied. Usually the other side will
                    // retry.
                    writer
                        .send(ControlChannelResponse::DenyChannelResponse)
                        .await?;
                    continue;
                };

                info!("Channel '{channel}' registered for '{other_connection_identifier}'");

                // The channel was registered, and we will instantiate a new ChannelHandler. It's
                // important to not send the OkChannelResponse yet, so the other side is not expected
                // to attempt to connect to the new ChannelHandler yet.
                // The ChannelHandler itself is waiting for a Reader/Writer Pair to talk to the other
                // side. The Reader/Writer pair is transferred to the ChannelHandler once the other
                // side attempts to connect. This is handled after Connection Identification within
                // @socket_listener.
                let attach_to_channel =
                    create_channel_handler(channel.clone(), emit, token, control.clone());

                state
                    .add_pending_channel(
                        other_connection_identifier.clone(),
                        channel,
                        attach_to_channel,
                    )
                    .await;

                // After the DataChannel has been added to the pending channels list, we accept the
                // request and tell the other side where to connect to. Currently, this is always
                // the ConnectionIdentifier of the ControlSocket, but future versions may allow
                // connecting to a different socket, i.e., to use UDP.
                writer
                    .send(ControlChannelResponse::OkChannelResponse(
                        this_connection_identifier.clone().into(),
                    ))
                    .await?;
            }
        }
    }
}

async fn socket_listener<C: Communication + 'static>(
    this_connection_identifier: ThisConnectionIdentifier,
    controller: NetworkingServiceController,
    state: ControllerState<
        <C::Listener as CommunicationListener>::Reader,
        <C::Listener as CommunicationListener>::Writer,
    >,
    mut communication: C,
    receiver_span: Span,
) {
    info!("Starting control socket: {}", this_connection_identifier);
    let mut active_connections = vec![];
    let mut communication_listener = communication
        .bind(this_connection_identifier.clone())
        .await
        .expect(&format!(
            "Failed to bind control socket to {}",
            this_connection_identifier
        ));

    loop {
        // Listen for incoming requests to the control port. Currently, all connections are received
        // via the control port. Future changes may use additional different ports for this purpose.
        // Because we are using a single port it is essential that we keep this async task very lean,
        // so we can accept further connections without waiting for slow participants.
        // Effectively, this is accepting new connections in a tight loop and spawns a dedicated task
        // to handle the connection.
        let Ok(stream) = communication_listener.listen().await else {
            error!("Control socket was closed");
            return;
        };

        let new_connection = ScopedTask::new(tokio::spawn({
            let receiver_span = receiver_span.clone();
            let this_connection_identifier = this_connection_identifier.clone();
            let state = state.clone();
            let controller = controller.clone();
            async move {
                // Once a new connection has been accepted, the first step is to identify the
                // connection. Because both Control and DataChannel connections are accepted via
                // the same port, this is required to decide what the connection is expecting.
                let identification = match identify_connection(stream).await {
                    Ok(identification) => identification,
                    Err(e) => {
                        warn!("Connection identification failed: {e:?}");
                        return;
                    }
                };

                match identification {
                    // The connection is a new control connection to a new worker.
                    ConnectionIdentification::Connection(reader, writer, connection) => {
                        async {
                            info!("Starting control socket handler for {connection}");
                            let result = control_socket_handler(
                                this_connection_identifier,
                                connection.clone(),
                                reader,
                                writer,
                                state.clone(),
                                controller.clone(),
                            )
                            .await;
                            info!("Control socket handler terminated: {:?}", result);
                        }
                        .instrument(
                            info_span!(parent: receiver_span, "connection", other = %connection),
                        )
                        .await;
                    }
                    // The connection is a new data channel for `connection`.
                    ConnectionIdentification::Channel(r, w, connection, channel) => {
                        // The control_socket_handler is responsible for negotiating data channels.
                        // So it is expected that before accepting a new DataChannel the control_socket_handler
                        // has registered a new DataChannel in the pending_channels map.
                        // The DataChannel itself is waiting for a Reader/Writer pair, and the
                        // corresponding oneshot channel is registered in the map.
                        let Some(sender) = state
                            .take_pending_channel(connection.clone(), channel.clone())
                            .await
                        else {
                            error!("Channel '{channel}' was not registered for '{connection}'");
                            return;
                        };

                        // Transfer the Reader/Writer pair to the data channel
                        if sender.send((r, w)).is_err() {
                            warn!("Channel '{channel}' to '{connection}' was already closed");
                        }
                    }
                }
            }
        }));
        active_connections.push(new_connection);
    }
}

pub(super) async fn create_control_socket_handler(
    listener: NetworkingServiceControlListener,
    controller: NetworkingServiceController,
    connection_identifier: ThisConnectionIdentifier,
    communication: impl Communication + 'static,
) -> Result<()> {
    // Communication between the network-facing side `socket_listener` and the software-facing side
    // via the NetworkingServiceController is managed with shared ControllerState.
    let state = ControllerState::default();
    let receiver_span = Span::current();

    // Instantiate the socket listener which will handle all network requests.
    let _socket_listener_task = ScopedTask::new(tokio::spawn(
        socket_listener(
            connection_identifier.clone(),
            controller,
            state.clone(),
            communication,
            receiver_span.clone(),
        )
            .instrument(info_span!(parent: receiver_span, "control-socket-listener", bind = %connection_identifier)),
    ));

    // Listen to all software-facing requests.
    loop {
        match listener.recv().await {
            // NetworkingServiceController was closed.
            Err(_) => {
                info!(
                    "Control socket listener was closed. This will terminate all active connections"
                );
                return Ok(());
            }

            // Software wants to register a new channel. The channel is registered in the controller state.
            // Future ChannelRequests with the `ChannelIdentifier` will be accepted by the @control_socket_handler,
            // and the DataChannel will be associated with the emit_fn
            Ok(NetworkServiceControlCommand::RegisterChannel(ident, emit_fn, response)) => {
                let token = CancellationToken::new();
                state
                    .add_registered_channel(ident, emit_fn, token.clone())
                    .await;

                if response.send(()).is_err() {
                    token.cancel();
                };
            }

            // A DataChannel has been ungracefully terminated, no software request nor stop request
            // from the other side. The channel requested a retry and will be placed back in the
            // registered channel list, where future ChannelRequests can reopen the channel.
            Ok(NetworkServiceControlCommand::RetryChannel(ident, emit_fn, token)) => {
                state.add_registered_channel(ident, emit_fn, token).await;
            }
        };
    }
}
