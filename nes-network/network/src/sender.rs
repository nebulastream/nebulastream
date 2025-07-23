use crate::protocol::*;
use futures::SinkExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, info, info_span, warn};

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub struct NetworkService {
    runtime: Mutex<Option<Runtime>>,
    controller: NetworkingServiceController,
    cancellation_token: CancellationToken,
}

enum NetworkingServiceControlMessage {
    RegisterChannel(
        ConnectionIdentifier,
        ChannelIdentifier,
        oneshot::Sender<ChannelControlQueue>,
    ),
}
enum NetworkingConnectionControlMessage {
    RegisterChannel(ChannelIdentifier, oneshot::Sender<ChannelControlQueue>),
    RetryChannel(
        ChannelIdentifier,
        CancellationToken,
        channel_handler::ChannelControlQueueListener,
    ),
}
pub type ChannelControlMessage = channel_handler::ChannelControlMessage;
pub type ChannelControlQueue = channel_handler::ChannelControlQueue;
type NetworkingServiceController = async_channel::Sender<NetworkingServiceControlMessage>;
type NetworkingServiceControlListener = async_channel::Receiver<NetworkingServiceControlMessage>;
type NetworkingConnectionController = async_channel::Sender<NetworkingConnectionControlMessage>;
type NetworkingConnectionControlListener =
    async_channel::Receiver<NetworkingConnectionControlMessage>;

enum ChannelHandlerResult {
    Cancelled,
    ConnectionLost(Box<dyn std::error::Error + Send + Sync>),
    Closed,
}

async fn connect(target_identifier: ConnectionIdentifier) -> Result<TcpStream> {
    let address = target_identifier.to_socket_address().await?;
    let socket = TcpSocket::new_v4().map_err(|e| format!("Could not create TCP socket: {e:?}"))?;
    socket
        .connect(address)
        .await
        .map_err(|e| format!("Could not connect to {address:?}: {e:?}").into())
}

async fn create_connection(
    this_connection: ThisConnectionIdentifier,
    target_identifier: ConnectionIdentifier,
) -> Result<(ControlChannelSenderReader, ControlChannelSenderWriter)> {
    let stream = connect(target_identifier).await?;
    let (mut read, mut write) = identification_sender(stream);
    write
        .send(IdentificationRequest::IAmConnection(this_connection))
        .await?;
    let response = read
        .next()
        .await
        .ok_or("Connection closed during identification")??;

    let stream = read
        .into_inner()
        .into_inner()
        .reunite(write.into_inner().into_inner())
        .expect("Could not reunite streams");

    match response {
        IdentificationResponse::Ok => Ok(control_channel_sender(stream)),
        IdentificationResponse::NotOk => {
            Err("Could not establish connection. Identification was denied".into())
        }
    }
}

async fn create_channel(
    this_connection: ThisConnectionIdentifier,
    target_identifier: ConnectionIdentifier,
    channel_identifier: ChannelIdentifier,
) -> Result<(DataChannelSenderReader, DataChannelSenderWriter)> {
    let stream = connect(target_identifier).await?;
    let (mut read, mut write) = identification_sender(stream);
    write
        .send(IdentificationRequest::IAmChannel(
            this_connection,
            channel_identifier,
        ))
        .await?;

    let response = read
        .next()
        .await
        .ok_or("Connection closed during identification")??;

    let stream = read
        .into_inner()
        .into_inner()
        .reunite(write.into_inner().into_inner())
        .expect("Could not reunite streams");

    match response {
        IdentificationResponse::Ok => Ok(data_channel_sender(stream)),
        IdentificationResponse::NotOk => {
            Err("Could not establish connection. Identification was denied".into())
        }
    }
}

mod channel_handler {

    use crate::protocol::{DataChannelRequest, DataChannelResponse, DataChannelSenderReader, DataChannelSenderWriter, Sequence, TupleBuffer};
    use futures::SinkExt;
    use std::collections::{HashMap, VecDeque};

    use tokio::select;
    use tokio::sync::oneshot;
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;
    use tracing::{info, trace, warn};

    const MAX_PENDING_ACKS: usize = 64;

    pub enum ChannelControlMessage {
        Data(TupleBuffer),
        Flush(oneshot::Sender<()>),
        Terminate,
    }
    pub type ChannelControlQueue = async_channel::Sender<ChannelControlMessage>;
    pub(super) type ChannelControlQueueListener = async_channel::Receiver<ChannelControlMessage>;
    pub(super) enum ChannelHandlerError {
        ClosedByOtherSide,
        ConnectionLost(Box<dyn std::error::Error + Send + Sync>),
        Protocol(Box<dyn std::error::Error + Send + Sync>),
        Cancelled,
        Terminated,
    }
    pub(super) struct ChannelHandler {
        cancellation_token: CancellationToken,
        pending_writes: VecDeque<TupleBuffer>,
        wait_for_ack: HashMap<Sequence, TupleBuffer>,
        writer: DataChannelSenderWriter,
        reader: DataChannelSenderReader,
        queue: ChannelControlQueueListener,
    }

    type Result<T> = core::result::Result<T, ChannelHandlerError>;

    impl ChannelHandler {
        pub fn new(
            cancellation_token: CancellationToken,
            queue: ChannelControlQueueListener,
            reader: DataChannelSenderReader,
            writer: DataChannelSenderWriter,
        ) -> Self {
            Self {
                cancellation_token,
                pending_writes: Default::default(),
                wait_for_ack: Default::default(),
                reader,
                writer,
                queue,
            }
        }

        async fn handle_request(
            &mut self,
            channel_control_message: ChannelControlMessage,
        ) -> Result<()> {
            match channel_control_message {
                ChannelControlMessage::Data(data) => self.pending_writes.push_back(data),
                ChannelControlMessage::Flush(done) => {
                    self.flush().await?;
                    let _ = done.send(());
                }
                ChannelControlMessage::Terminate => {
                    let _ = self.writer.send(DataChannelRequest::Close).await;
                    return Err(ChannelHandlerError::Terminated);
                }
            }
            Ok(())
        }
        fn handle_response(&mut self, response: DataChannelResponse) -> Result<()> {
            match response {
                DataChannelResponse::Close => {
                    info!("Channel Closed by other receiver");
                    return Err(ChannelHandlerError::ClosedByOtherSide);
                }
                DataChannelResponse::NAckData(seq) => {
                    if let Some(write) = self.wait_for_ack.remove(&seq) {
                        warn!("NAck for {seq:?}");
                        self.pending_writes.push_back(write);
                    } else {
                        return Err(ChannelHandlerError::Protocol(
                            format!("Protocol Error. Unknown Seq {seq:?}").into(),
                        ));
                    }
                }
                DataChannelResponse::AckData(seq) => {
                    let Some(_) = self.wait_for_ack.remove(&seq) else {
                        return Err(ChannelHandlerError::Protocol(
                            format!("Protocol Error. Unknown Seq {seq:?}").into(),
                        ));
                    };
                    trace!("Ack for {seq:?}");
                }
            }

            Ok(())
        }

        async fn send_pending(
            writer: &mut DataChannelSenderWriter,
            pending_writes: &mut VecDeque<TupleBuffer>,
            wait_for_ack: &mut HashMap<Sequence, TupleBuffer>,
        ) -> Result<()> {
            if pending_writes.is_empty() {
                return Ok(());
            }

            let sequence_number = pending_writes
                .front()
                .expect("BUG: check value earlier")
                .sequence();
            wait_for_ack.insert(
                sequence_number,
                pending_writes
                    .pop_front()
                    .expect("BUG: checked value earlier"),
            );
            let next_buffer = wait_for_ack
                .get(&sequence_number)
                .expect("BUG: value was inserted earlier");

            if writer
                .feed(DataChannelRequest::Data(next_buffer.clone()))
                .await
                .is_err()
            {
                pending_writes.push_front(
                    wait_for_ack
                        .remove(&sequence_number)
                        .expect("BUG: value was inserted earlier"),
                );
            }

            Ok(())
        }

        async fn flush(&mut self) -> Result<()> {
            while !self.pending_writes.is_empty() || !self.wait_for_ack.is_empty() {
                self.writer
                    .flush()
                    .await
                    .map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?;
                if self.cancellation_token.is_cancelled() {
                    return Err(ChannelHandlerError::Cancelled);
                }

                if self.pending_writes.is_empty() || self.wait_for_ack.len() >= MAX_PENDING_ACKS {
                    select! {
                        _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                    }
                } else {
                    select! {
                        _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                        send_result = Self::send_pending(&mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack) => send_result?,
                    }
                }
            }

            Ok(())
        }

        pub(super) async fn run(&mut self) -> Result<()> {
            loop {
                if self.cancellation_token.is_cancelled() {
                    return Err(ChannelHandlerError::Cancelled);
                }

                if self.pending_writes.is_empty() || self.wait_for_ack.len() >= MAX_PENDING_ACKS {
                    select! {
                        _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                        request = self.queue.recv() => self.handle_request(request.map_err(|_| ChannelHandlerError::Cancelled)?).await?,
                    }
                } else {
                    select! {
                        _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                        request = self.queue.recv() => self.handle_request(request.map_err(|_| ChannelHandlerError::Cancelled)?).await?,
                        send_result = Self::send_pending(&mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack) => send_result?,
                    }
                }
            }
        }
    }
}

async fn channel_handler(
    cancellation_token: CancellationToken,
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
    channel_identifier: ChannelIdentifier,
    queue: channel_handler::ChannelControlQueueListener,
) -> ChannelHandlerResult {
    debug!("Channel negotiated. Connecting to {target_connection} on channel {channel_identifier}");
    let (reader, writer) = match create_channel(
        this_connection,
        target_connection.clone(),
        channel_identifier,
    )
    .await
    {
        Ok((reader, writer)) => (reader, writer),
        Err(e) => {
            return ChannelHandlerResult::ConnectionLost(
                format!("Could not create channel to {target_connection}: {e:?}").into(),
            );
        }
    };

    let mut handler =
        channel_handler::ChannelHandler::new(cancellation_token, queue, reader, writer);
    match handler.run().await {
        Ok(_) => ChannelHandlerResult::Closed,
        Err(channel_handler::ChannelHandlerError::Terminated) => ChannelHandlerResult::Closed,
        Err(channel_handler::ChannelHandlerError::ClosedByOtherSide) => {
            ChannelHandlerResult::Closed
        }
        Err(channel_handler::ChannelHandlerError::Cancelled) => ChannelHandlerResult::Cancelled,
        Err(channel_handler::ChannelHandlerError::ConnectionLost(e)) => {
            ChannelHandlerResult::ConnectionLost(e)
        }
        Err(channel_handler::ChannelHandlerError::Protocol(e)) => {
            ChannelHandlerResult::ConnectionLost(e)
        }
    }
}

enum EstablishChannelResult {
    Ok,
    ChannelReject,
    BadConnection(
        ChannelIdentifier,
        channel_handler::ChannelControlQueueListener,
        Error,
    ),
    Cancelled,
}

async fn establish_channel(
    control_channel_sender_writer: &mut ControlChannelSenderWriter,
    control_channel_sender_reader: &mut ControlChannelSenderReader,
    this_connection: &ThisConnectionIdentifier,
    target_connection: &ConnectionIdentifier,
    channel: ChannelIdentifier,
    channel_cancellation_token: CancellationToken,
    queue: channel_handler::ChannelControlQueueListener,
    controller: NetworkingConnectionController,
) -> EstablishChannelResult {
    match channel_cancellation_token
        .run_until_cancelled(
            control_channel_sender_writer
                .send(ControlChannelRequest::ChannelRequest(channel.clone())),
        )
        .await
    {
        None => return EstablishChannelResult::Cancelled,
        Some(Err(e)) => {
            return EstablishChannelResult::BadConnection(
                channel,
                queue,
                format!("Could not send channel creation request {}", e).into(),
            );
        }
        Some(Ok(())) => {}
    };

    let port = match channel_cancellation_token
        .run_until_cancelled(control_channel_sender_reader.next())
        .await
    {
        Some(Some(Ok(ControlChannelResponse::OkChannelResponse(port)))) => port,
        Some(Some(Ok(ControlChannelResponse::DenyChannelResponse))) => {
            return EstablishChannelResult::ChannelReject;
        }
        Some(Some(Err(e))) => {
            return EstablishChannelResult::BadConnection(channel, queue, e.into());
        }
        Some(None) => {
            return EstablishChannelResult::BadConnection(
                channel,
                queue,
                "Connection closed".into(),
            );
        }
        None => return EstablishChannelResult::Cancelled,
    };

    tokio::spawn(
        {
            let channel = channel.clone();
            let this_connection = this_connection.clone();
            let target_connection = target_connection.clone();
            let channel_cancellation_token = channel_cancellation_token.clone();
            async move {
                match channel_handler(
                    channel_cancellation_token.clone(),
                    this_connection,
                    target_connection,
                    channel.clone(),
                    queue.clone(),
                )
                .await
                {
                    ChannelHandlerResult::Cancelled => {}
                    ChannelHandlerResult::ConnectionLost(e) => {
                        warn!("Connection Lost: {e:?}");
                        match &channel_cancellation_token
                            .run_until_cancelled(controller.send(
                                NetworkingConnectionControlMessage::RetryChannel(
                                    channel,
                                    channel_cancellation_token.clone(),
                                    queue.clone(),
                                ),
                            ))
                            .await
                        {
                            None => {}
                            Some(Ok(())) => {
                                info!("Channel connection lost.Reopening channel");
                            }
                            Some(Err(_)) => {}
                        }
                    }
                    ChannelHandlerResult::Closed => {
                        info!("Channel closed");
                    }
                }
            }
        }
        .instrument(info_span!("channel_handler", channel = %channel, port = %port)),
    );
    EstablishChannelResult::Ok
}

async fn connection_handler(
    connection_cancellation_token: CancellationToken,
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
    controller: NetworkingConnectionController,
    listener: NetworkingConnectionControlListener,
) -> Result<()> {
    let (connection_tx, mut connection_rx) = tokio::sync::mpsc::channel::<(
        ControlChannelSenderReader,
        ControlChannelSenderWriter,
        tokio::sync::oneshot::Sender<()>,
    )>(1);
    let (channel_tx, mut channel_rx) = tokio::sync::mpsc::channel::<(
        (
            ChannelIdentifier,
            CancellationToken,
            channel_handler::ChannelControlQueueListener,
        ),
        tokio::sync::oneshot::Sender<EstablishChannelResult>,
    )>(10);

    tokio::spawn(
        {
            let target_connection = target_connection.clone();
            let this_connection = this_connection.clone();
            let connection_cancellation_token = connection_cancellation_token.clone();
            async move {
                let mut retry = 0;
                loop {
                    let Some(connection_result) = connection_cancellation_token
                        .run_until_cancelled(create_connection(
                            this_connection.clone(),
                            target_connection.clone(),
                        ))
                        .await
                    else {
                        return;
                    };

                    let (reader, writer) = match connection_result {
                        Ok((reader, writer)) => (reader, writer),
                        Err(e) => {
                            warn!(
                                "Could not create connection to {}: {e:?}",
                                target_connection
                            );
                            retry = retry + 1;
                            tokio::time::sleep(Duration::from_secs(retry)).await;
                            continue;
                        }
                    };
                    retry = 0;
                    info!("Connection to {} was established", target_connection);
                    let (tx, rx) = oneshot::channel();
                    if connection_tx.send((reader, writer, tx)).await.is_err() {
                        return;
                    }
                    if rx.await.is_err() {
                        return;
                    }
                }
            }
        }
        .instrument(Span::current()),
    );

    tokio::spawn({
        let connection_cancellation_token = connection_cancellation_token.clone();
        let this_connection = this_connection.clone();
        let target_connection = target_connection.clone();
        async move {
            'connection: loop {
                let Some((mut reader, mut writer, reconnect)) = connection_rx.recv().await else {
                    return;
                };
                loop {
                    let Some(((channel, channel_cancellation_token, queue), response)) =
                        channel_rx.recv().await
                    else {
                        return;
                    };
                    match connection_cancellation_token
                        .run_until_cancelled(establish_channel(
                            &mut writer,
                            &mut reader,
                            &this_connection,
                            &target_connection,
                            channel.clone(),
                            channel_cancellation_token.clone(),
                            queue,
                            controller.clone(),
                        ))
                        .await
                    {
                        None => {
                            return;
                        }
                        Some(EstablishChannelResult::Cancelled) => {
                            return;
                        }
                        Some(EstablishChannelResult::BadConnection(c, ct, q)) => {
                            let _ = response.send(EstablishChannelResult::BadConnection(c, ct, q));
                            let _ = reconnect.send(());
                            continue 'connection;
                        }
                        Some(r) => {
                            let _ = response.send(r);
                        }
                    }
                }
            }
        }
    });

    async fn attempt_channel_registration(
        channel: ChannelIdentifier,
        channel_cancellation: CancellationToken,
        queue: channel_handler::ChannelControlQueueListener,
        channel_tx: tokio::sync::mpsc::Sender<(
            (
                ChannelIdentifier,
                CancellationToken,
                channel_handler::ChannelControlQueueListener,
            ),
            tokio::sync::oneshot::Sender<EstablishChannelResult>,
        )>,
    ) {
        let mut retry = 0;
        loop {
            let (tx, rx) = oneshot::channel();
            if channel_tx
                .send((
                    (channel.clone(), channel_cancellation.clone(), queue.clone()),
                    tx,
                ))
                .await
                .is_err()
            {
                return;
            }
            let Ok(result) = rx.await else {
                return;
            };
            match result {
                EstablishChannelResult::Ok | EstablishChannelResult::Cancelled => {
                    return;
                }
                EstablishChannelResult::ChannelReject
                | EstablishChannelResult::BadConnection(_, _, _) => {
                    retry = retry + 1;
                    tokio::time::sleep(Duration::from_millis(100 * retry)).await;
                    continue;
                }
            }
        }
    }

    loop {
        let Ok(control_message) = listener.recv().await else {
            return Ok(());
        };
        match control_message {
            NetworkingConnectionControlMessage::RegisterChannel(channel, response) => {
                tokio::spawn({
                    let channel_tx = channel_tx.clone();
                    async move {
                        let (sender, queue) = async_channel::bounded(100);
                        let channel_cancellation = CancellationToken::new();
                        if response.send(sender).is_err() {
                            warn!("Channel registration was canceled");
                            return;
                        }
                        attempt_channel_registration(
                            channel,
                            channel_cancellation,
                            queue,
                            channel_tx,
                        )
                        .await;
                    }
                });
            }
            NetworkingConnectionControlMessage::RetryChannel(
                channel,
                channel_cancellation,
                queue,
            ) => {
                tokio::spawn(attempt_channel_registration(
                    channel,
                    channel_cancellation,
                    queue,
                    channel_tx.clone(),
                ));
            }
        };
    }
}
async fn create_connection_handler(
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
) -> Result<(CancellationToken, NetworkingConnectionController)> {
    let (tx, rx) = async_channel::bounded::<NetworkingConnectionControlMessage>(1024);
    let control = tx.clone();
    let token = CancellationToken::new();
    tokio::spawn(
        {
            let token = token.clone();
            let target_connection = target_connection.clone();
            async move {
                info!(
                    "Connection is terminated: {:?}",
                    connection_handler(token, this_connection, target_connection, control, rx)
                        .await
                );
            }
        }
        .instrument(info_span!(
            "connection_handler",
            connection = %target_connection
        )),
    );
    Ok((token, tx))
}

async fn network_sender_dispatcher(
    cancellation_token: CancellationToken,
    this_connection: ThisConnectionIdentifier,
    control: NetworkingServiceControlListener,
) -> Result<()> {
    let mut connections: HashMap<
        ConnectionIdentifier,
        (CancellationToken, NetworkingConnectionController),
    > = HashMap::default();
    let on_cancel = |active_channel: HashMap<
        ConnectionIdentifier,
        (CancellationToken, NetworkingConnectionController),
    >| {
        active_channel
            .into_iter()
            .for_each(|(_, (token, _))| token.cancel());
        Ok(())
    };

    loop {
        match cancellation_token.run_until_cancelled(control.recv()).await {
            None => return on_cancel(connections),
            Some(Err(_)) => return Err("Queue was closed".into()),
            Some(Ok(NetworkingServiceControlMessage::RegisterChannel(
                target_connection,
                channel,
                tx,
            ))) => {
                if !connections.contains_key(&target_connection) {
                    info!("Creating connection to {}", target_connection);
                    match cancellation_token
                        .run_until_cancelled(create_connection_handler(
                            this_connection.clone(),
                            target_connection.clone(),
                        ))
                        .await
                    {
                        None => return on_cancel(connections),
                        Some(Ok((token, controller))) => {
                            connections.insert(target_connection.clone(), (token, controller));
                        }
                        Some(Err(e)) => {
                            return Err(e);
                        }
                    }
                }

                match cancellation_token
                    .run_until_cancelled(
                        connections
                            .get(&target_connection)
                            .expect("BUG: Just inserted the key")
                            .1
                            .send(NetworkingConnectionControlMessage::RegisterChannel(
                                channel, tx,
                            )),
                    )
                    .await
                {
                    None => return on_cancel(connections),
                    Some(Err(e)) => {
                        return Err(e)?;
                    }
                    Some(Ok(())) => {}
                }
            }
        }
    }
}
impl NetworkService {
    pub fn start(
        runtime: Runtime,
        this_connection: ThisConnectionIdentifier,
    ) -> Arc<NetworkService> {
        let (controller, listener) = async_channel::bounded(5);
        let cancellation_token = CancellationToken::new();
        runtime.spawn(
            {
                let this_connection = this_connection.clone();
                let token = cancellation_token.clone();
                async move {
                    info!("Starting sender network service");
                    info!(
                        "sender network service stopped: {:?}",
                        network_sender_dispatcher(token, this_connection, listener).await
                    );
                }
            }
            .instrument(info_span!("sender", this = %this_connection)),
        );

        Arc::new(NetworkService {
            runtime: Mutex::new(Some(runtime)),
            cancellation_token,
            controller,
        })
    }

    pub fn register_channel(
        self: &Arc<NetworkService>,
        connection: ConnectionIdentifier,
        channel: ChannelIdentifier,
    ) -> Result<channel_handler::ChannelControlQueue> {
        let (tx, rx) = oneshot::channel();
        let Ok(_) =
            self.controller
                .send_blocking(NetworkingServiceControlMessage::RegisterChannel(
                    connection, channel, tx,
                ))
        else {
            return Err("Network Service Closed".into());
        };

        rx.blocking_recv()
            .map_err(|_| "Network Service Closed".into())
    }

    pub fn shutdown(self: Arc<NetworkService>) -> Result<()> {
        let runtime = self
            .runtime
            .lock()
            .expect("BUG: Nothing should panic while holding the lock")
            .take()
            .ok_or("Networking Service was stopped")?;
        self.cancellation_token.cancel();
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}

impl NetworkService {}
