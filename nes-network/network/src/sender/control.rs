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

use super::channel::{ChannelControlQueue, ChannelControlQueueListener, create_channel_handler};
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
pub(super) enum NetworkingServiceControlMessage {
    RegisterChannel(
        ConnectionIdentifier,
        ChannelIdentifier,
        oneshot::Sender<ChannelControlQueue>,
    ),
}
pub(super) enum NetworkingConnectionControlMessage {
    RegisterChannel(ChannelIdentifier, oneshot::Sender<ChannelControlQueue>),
    RetryChannel(
        ChannelIdentifier,
        CancellationToken,
        ChannelControlQueueListener,
    ),
}
pub(super) type NetworkingServiceController =
    async_channel::Sender<NetworkingServiceControlMessage>;
type NetworkingServiceControlListener = async_channel::Receiver<NetworkingServiceControlMessage>;
pub(super) type NetworkingConnectionController =
    async_channel::Sender<NetworkingConnectionControlMessage>;
type NetworkingConnectionControlListener =
    async_channel::Receiver<NetworkingConnectionControlMessage>;

pub(super) async fn connect<L: Communication>(
    target_identifier: ConnectionIdentifier,
    communication: L,
) -> Result<Channel<L::Reader, L::Writer>> {
    communication
        .connect(&target_identifier)
        .await
        .map_err(|e| format!("Could not connect to {target_identifier}: {e}").into())
}

pub(super) enum EstablishChannelResult {
    Ok(ConnectionIdentifier),
    ChannelReject,
    BadConnection(ChannelIdentifier, Error),
}
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

async fn create_connection<L: Communication>(
    this_connection: ThisConnectionIdentifier,
    target_identifier: ConnectionIdentifier,
    communication: L,
) -> core::result::Result<
    (
        ControlChannelSenderReader<L::Reader>,
        ControlChannelSenderWriter<L::Writer>,
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

async fn attempt_channel_registration<C: Communication + 'static>(
    this_connection: ThisConnectionIdentifier,
    channel_id: ChannelIdentifier,
    channel_cancellation: CancellationToken,
    queue: ChannelControlQueueListener,
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
            if channel_tx.send((channel_id.clone(), tx)).await.is_err() {
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
        channel_id,
        channel_cancellation,
        queue,
        communication,
        controller,
    );
}

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
    // This task is only responsible for establishing a new DataChannel once they have been accepted
    // by the other side.
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
    loop {
        let Ok(control_message) = listener.recv().await else {
            // NetworkService was closed
            return Ok(());
        };

        match control_message {
            NetworkingConnectionControlMessage::RegisterChannel(channel, response) => {
                let (sender, queue) = async_channel::bounded(100);
                let channel_cancellation = CancellationToken::new();

                tokio::spawn(
                    attempt_channel_registration(
                        this_connection.clone(),
                        channel,
                        channel_cancellation.clone(),
                        queue,
                        channel_registration_request_handler.clone(),
                        controller.clone(),
                        communication.clone(),
                    )
                    .in_current_span(),
                );

                active_channel.add_token(channel_cancellation);
                let _ = response.send(sender);
            }
            NetworkingConnectionControlMessage::RetryChannel(
                channel,
                channel_cancellation,
                queue,
            ) => {
                tokio::spawn(
                    attempt_channel_registration(
                        this_connection.clone(),
                        channel,
                        channel_cancellation.clone(),
                        queue,
                        channel_registration_request_handler.clone(),
                        controller.clone(),
                        communication.clone(),
                    )
                    .in_current_span(),
                );
            }
        };
    }
}
fn create_connection_handler(
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
    communication: impl Communication + 'static,
) -> (ScopedTask<()>, NetworkingConnectionController) {
    let (tx, rx) = async_channel::bounded::<NetworkingConnectionControlMessage>(1024);
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
                        control,
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

pub(super) async fn network_sender_dispatcher(
    this_connection: ThisConnectionIdentifier,
    control: NetworkingServiceControlListener,
    communication: impl Communication + 'static,
) -> Result<()> {
    // All currently active connections. Dropping the hashmap will abort all active connections.
    let mut connections: HashMap<
        ConnectionIdentifier,
        (ScopedTask<()>, NetworkingConnectionController),
    > = HashMap::default();

    loop {
        // Listen for all Sender ControlMessage send from the software-side
        match control.recv().await {
            // The software side was closed, which means that the NetworkingService was dropped.
            Err(_) => return Err("Queue was closed".into()),

            // software-side wants to register a new channel
            Ok(NetworkingServiceControlMessage::RegisterChannel(
                target_connection,
                channel,
                tx,
            )) => {
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

                controller
                    .send(NetworkingConnectionControlMessage::RegisterChannel(
                        channel, tx,
                    ))
                    .await
                    // The Connection Controller should never terminate but instead attempt
                    // to reconnect. The only reason a connection controller would terminate is
                    // if it has been removed from the `connections` map.
                    .expect("BUG: Connection should not have been terminated");
            }
        }
    }
}
