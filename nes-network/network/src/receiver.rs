use crate::protocol::*;
use futures::SinkExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument, Span};

pub struct NetworkService {
    sender: NetworkingServiceController,
    runtime: Mutex<Option<Runtime>>,
}

enum NetworkingServiceControl {
    RetryChannel(ChannelIdentifier, DataQueue, CancellationToken),
    RegisterChannel(ChannelIdentifier, DataQueue, oneshot::Sender<()>),
}
type DataQueue = async_channel::Sender<TupleBuffer>;
type NetworkingServiceController = async_channel::Sender<NetworkingServiceControl>;
type NetworkingServiceControlListener = async_channel::Receiver<NetworkingServiceControl>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type EmitFn = Box<dyn FnMut(TupleBuffer) -> bool + Send + Sync>;

enum ChannelHandlerError {
    ClosedByOtherSide,
    Cancelled,
    Network(Error),
}

pub enum ConnectionIdentification {
    Connection(
        ControlChannelReceiverReader,
        ControlChannelReceiverWriter,
        ConnectionIdentifier,
    ),
    Channel(
        DataChannelReceiverReader,
        DataChannelReceiverWriter,
        ConnectionIdentifier,
        ChannelIdentifier,
    ),
}
pub async fn identify_connection(stream: TcpStream) -> Result<ConnectionIdentification> {
    let (mut read, mut write) = identification_receiver(stream);
    let response = read
        .next()
        .await
        .ok_or("Connection Closed during Identification")??;

    write.send(IdentificationResponse::Ok).await?;

    let stream = read
        .into_inner()
        .into_inner()
        .reunite(write.into_inner().into_inner())
        .expect("Could not reunite streams");

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

type RegisteredChannels = Arc<RwLock<HashMap<ChannelIdentifier, (DataQueue, CancellationToken)>>>;
type OpenedChannels = Arc<
    RwLock<
        HashMap<
            (ConnectionIdentifier, ChannelIdentifier),
            oneshot::Sender<(DataChannelReceiverReader, DataChannelReceiverWriter)>,
        >,
    >,
>;
async fn channel_handler(
    cancellation_token: CancellationToken,
    queue: &mut DataQueue,
    mut reader: DataChannelReceiverReader,
    mut writer: DataChannelReceiverWriter,
) -> core::result::Result<(), ChannelHandlerError> {
    let mut pending_buffer: Option<TupleBuffer> = None;
    loop {
        if let Some(pending_buffer) = pending_buffer.take() {
            let sequence = pending_buffer.sequence();
            select! {
                _ = cancellation_token.cancelled() => return Err(ChannelHandlerError::Cancelled),
                write_queue_result = queue.send(pending_buffer) => {
                    match write_queue_result {
                        Ok(_) => {
                            let Some(result) = cancellation_token.run_until_cancelled(writer.send(DataChannelResponse::AckData(sequence))).await else {
                                return Err(ChannelHandlerError::Cancelled);
                            };
                            result.map_err(|e| ChannelHandlerError::Network(e.into()))?
                        },
                        Err(_) => {
                            let Some(result) = cancellation_token.run_until_cancelled(writer.send(DataChannelResponse::Close)).await else {
                                return Err(ChannelHandlerError::Cancelled);
                            };
                            return result.map_err(|e| ChannelHandlerError::Network(e.into()));
                        }
                    }
                },
            }
        }

        select! {
            _ = cancellation_token.cancelled() => return Err(ChannelHandlerError::Cancelled),
            request = reader.next() => pending_buffer = {
                match request.ok_or(ChannelHandlerError::Network("Connection Lost".into()))?.map_err(|e| ChannelHandlerError::Network(e.into()))? {
                    DataChannelRequest::Data(buffer) => Some(buffer),
                    DataChannelRequest::Close => {
                        queue.close();
                        return Err(ChannelHandlerError::ClosedByOtherSide)
                    },
                }
            }
        }
    }
}

async fn create_channel_handler(
    connector_identifier: ConnectionIdentifier,
    channel_id: ChannelIdentifier,
    opened_channels: OpenedChannels,
    mut queue: DataQueue,
    channel_cancellation_token: CancellationToken,
    control: NetworkingServiceController,
) {
    tokio::spawn({
        let channel = channel_id.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            {
                let mut locked = opened_channels.write().await;
                locked.insert((connector_identifier, channel.clone()), tx);
            }
            let Ok((reader, writer)) = rx.await else {
                warn!("Channel was closed");
                return;
            };
            info!("Open");

            let Err(channel_handler_error) = channel_handler(
                channel_cancellation_token.clone(),
                &mut queue,
                reader,
                writer,
            )
            .await
            else {
                return;
            };

            match channel_handler_error {
                ChannelHandlerError::Cancelled => {
                    info!("Data Channel Stopped");
                    return;
                }
                ChannelHandlerError::Network(e) => {
                    warn!("Data Channel Stopped due to network error: {e}");
                }
                ChannelHandlerError::ClosedByOtherSide => {
                    info!("Data Channel Stopped");
                    queue.close();
                }
            }
            info!("reopening channel");
            // Reopen the channel
            control
                .send(NetworkingServiceControl::RetryChannel(
                    channel,
                    queue,
                    channel_cancellation_token,
                ))
                .await
                .expect("ReceiverServer should not have closed, while a channel is active");
        }
        .instrument(info_span!("channel", channel_id = %channel_id))
    });
}
async fn control_socket_handler(
    mut reader: ControlChannelReceiverReader,
    mut writer: ControlChannelReceiverWriter,
    connection_identification: ConnectionIdentifier,
    ports: &Vec<u16>,
    channels: RegisteredChannels,
    opened_channels: OpenedChannels,
    control: NetworkingServiceController,
) -> Result<ControlChannelRequest> {
    let mut active_connection_channels = vec![];
    loop {
        let Some(Ok(message)) = reader.next().await else {
            warn!(
                "Connection was closed. Cancelling {} channel(s)",
                active_connection_channels.len()
            );
            active_connection_channels
                .into_iter()
                .for_each(|t: CancellationToken| t.cancel());
            return Err("Connection Closed".into());
        };

        match message {
            ControlChannelRequest::ChannelRequest(channel) => {
                active_connection_channels.retain(|t: &CancellationToken| !t.is_cancelled());
                let Some((emit, token)) = channels.write().await.remove(&channel) else {
                    writer
                        .send(ControlChannelResponse::DenyChannelResponse)
                        .await?;
                    continue;
                };

                create_channel_handler(
                    connection_identification.clone(),
                    channel,
                    opened_channels.clone(),
                    emit,
                    token.clone(),
                    control.clone(),
                )
                .await;
                writer
                    .send(ControlChannelResponse::OkChannelResponse(ports[0]))
                    .await?;
                active_connection_channels.push(token);
            }
        }
    }
}

async fn control_socket(
    listener: NetworkingServiceControlListener,
    controller: NetworkingServiceController,
    bind_address: SocketAddr,
    connection_identifier: ThisConnectionIdentifier,
) -> Result<()> {
    info!("Starting control socket: {}", connection_identifier);
    let listener_port = TcpListener::bind(bind_address).await?;
    let registered_channels = Arc::new(RwLock::new(HashMap::default()));
    let opened_channels = Arc::new(RwLock::new(HashMap::default()));
    let receiver_span = Span::current();

    tokio::spawn(
        {
            let registered_channels = registered_channels.clone();
            let opened_channels = opened_channels.clone();
            async move {
                let receiver_span = receiver_span.clone();
                let ports = vec![listener_port.local_addr().unwrap().port()];
                loop {
                    let Ok((stream, addr)) = listener_port.accept().await else {
                        error!("Control socket was closed");
                        return;
                    };
                    info!("Received connection from {}", addr);
                    let identification = match identify_connection(stream).await {
                        Ok(identification) => identification,
                        Err(e) => {
                            warn!("Connection identification failed: {e:?}");
                            continue;
                        }
                    };

                    match identification {
                        ConnectionIdentification::Connection(reader, writer, connection) => {
                            tokio::spawn(
                                {
                                    let controller = controller.clone();
                                    let registered_channels = registered_channels.clone();
                                    let opened_channels = opened_channels.clone();
                                    let ports = ports.clone();
                                    let c = connection.clone();
                                    async move {
                                        info!("Starting control socket handler for {c}");
                                        let result = control_socket_handler(
                                            reader,
                                            writer,
                                            c,
                                            &ports,
                                            registered_channels.clone(),
                                            opened_channels.clone(),
                                            controller.clone(),
                                        )
                                        .await;
                                        info!("Control socket handler terminated: {:?}", result);
                                    }
                                }
                                .instrument(info_span!(parent: receiver_span.clone(), "connection_handler",  other = %connection)),
                            );
                        }
                        ConnectionIdentification::Channel(r, w, c, channel) => {
                            let mut lock = opened_channels.write().await;
                            let Some(sender) = lock.remove(&(c, channel)) else {
                                error!("Channel was not registered");
                                continue;
                            };
                            match sender.send((r, w)) {
                                Ok(_) => {}
                                Err(_) => {
                                    warn!("Channel was already closed");
                                }
                            }
                        }
                    }
                }
            }
        }
        .instrument(info_span!("control_socket", bind_address = %bind_address)),
    );

    loop {
        match listener.recv().await {
            Err(_) => {
                registered_channels
                    .write()
                    .await
                    .iter()
                    .for_each(|(_, (_, token))| {
                        token.cancel();
                    });
                return Ok(());
            }
            Ok(NetworkingServiceControl::RegisterChannel(ident, emit_fn, response)) => {
                let token = CancellationToken::new();
                {
                    let mut locked = registered_channels.write().await;
                    locked.retain(|_, (_, token)| !token.is_cancelled());
                    locked.insert(ident, (emit_fn, token.clone()));
                }

                match response.send(()) {
                    Ok(_) => {}
                    Err(_) => {
                        token.cancel();
                    }
                };
            }
            Ok(NetworkingServiceControl::RetryChannel(ident, emit_fn, token)) => {
                registered_channels
                    .write()
                    .await
                    .insert(ident, (emit_fn, token));
            }
        };
    }
}
impl NetworkService {
    pub fn start(
        runtime: Runtime,
        bind_address: SocketAddr,
        connection_identifier: ThisConnectionIdentifier,
    ) -> Arc<NetworkService> {
        let (tx, rx) = async_channel::bounded(10);
        let service = Arc::new(NetworkService {
            sender: tx.clone(),
            runtime: Mutex::new(Some(runtime)),
        });

        service
            .runtime
            .lock()
            .expect("BUG: No one should panic while holding this lock")
            .as_ref()
            .expect("BUG: The service was just started")
            .spawn(
                {
                    let listener = rx;
                    let controller = tx;
                    let connection_identifier = connection_identifier.clone();
                    async move {
                        let control_socket_result = control_socket(
                            listener,
                            controller,
                            bind_address,
                            connection_identifier,
                        )
                        .await;
                        match control_socket_result {
                            Ok(_) => {
                                warn!("Control stopped")
                            }
                            Err(e) => {
                                error!("Control stopped with error: {:?}", e);
                            }
                        }
                    }
                }
                .instrument(info_span!("receiver", this = %connection_identifier)),
            );

        service
    }

    pub fn register_channel(
        self: &Arc<NetworkService>,
        channel: ChannelIdentifier,
    ) -> Result<async_channel::Receiver<TupleBuffer>> {
        let (data_queue_sender, data_queue_receiver) = async_channel::bounded(10);
        let (tx, rx) = oneshot::channel();
        let Ok(_) = self
            .sender
            .send_blocking(NetworkingServiceControl::RegisterChannel(
                channel,
                data_queue_sender,
                tx,
            ))
        else {
            return Err("Networking Service was stopped".into());
        };
        rx.blocking_recv()
            .map_err(|_| "Networking Service was stopped")?;
        Ok(data_queue_receiver)
    }

    pub fn shutdown(self: Arc<NetworkService>) -> Result<()> {
        self.sender.close();
        let runtime = self
            .runtime
            .lock()
            .expect("BUG: No one should panic while holding this lock")
            .take()
            .ok_or("Networking Service was stopped")?;
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}
