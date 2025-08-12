use crate::memcom::{memcom_connect, memcom_listen};
use crate::protocol::{ConnectionIdentifier, ThisConnectionIdentifier};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, SimplexStream, WriteHalf};
use tokio::net::TcpSocket;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

pub struct Channel<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send> {
    pub reader: R,
    pub writer: W,
}

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub trait CommunicationListener: Send + Sync + Clone {
    type Reader: AsyncRead + Unpin + Send;
    type Writer: AsyncWrite + Unpin + Send;
    fn listen(
        &mut self,
    ) -> impl std::future::Future<Output=Result<Channel<Self::Reader, Self::Writer>>> + Send;
}
pub trait Communication: Send + Sync + Clone {
    type Listener: CommunicationListener;
    type Reader: AsyncRead + Unpin + Send;
    type Writer: AsyncWrite + Unpin + Send;
    fn bind(
        &mut self,
        this_connection_identifier: ThisConnectionIdentifier,
    ) -> impl Future<Output=Result<Self::Listener>> + Send;
    fn connect(
        &self,
        identifier: &ConnectionIdentifier,
    ) -> impl Future<Output=Result<Channel<Self::Reader, Self::Writer>>> + Send;
}

#[derive(Clone)]
pub struct TcpCommunication {}

#[derive(Clone)]
pub struct TcpCommunicationListener {
    listener_port: Arc<Mutex<tokio::net::TcpListener>>,
}

impl TcpCommunication {
    pub fn new() -> Self {
        TcpCommunication {}
    }
}

impl CommunicationListener for TcpCommunicationListener {
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;

    async fn listen(&mut self) -> Result<Channel<Self::Reader, Self::Writer>> {
        let (stream, _) = self
            .listener_port
            .lock()
            .await
            .accept()
            .await
            .map_err(|e| format!("Could not bind to port: {}", e))?;
        let (rx, tx) = stream.into_split();

        Ok(Channel {
            reader: rx,
            writer: tx,
        })
    }
}
impl Communication for TcpCommunication {
    type Listener = TcpCommunicationListener;
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;
    async fn bind(
        &mut self,
        this_connection_identifier: ThisConnectionIdentifier,
    ) -> Result<Self::Listener> {
        let identifier: ConnectionIdentifier = this_connection_identifier.into();
        let socket = identifier.to_socket_address().await?;
        let bind_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), socket.port());
        let socket = tokio::net::TcpListener::bind(bind_address)
            .await
            .map_err(|e| format!("Could not bind to port: {}", e))?;

        Ok(TcpCommunicationListener {
            listener_port: Arc::new(Mutex::new(socket)),
        })
    }
    async fn connect(
        &self,
        identifier: &ConnectionIdentifier,
    ) -> Result<Channel<OwnedReadHalf, OwnedWriteHalf>> {
        let address = identifier.to_socket_address().await?;
        let socket =
            TcpSocket::new_v4().map_err(|e| format!("Could not create TCP socket: {e:?}"))?;
        let stream = socket
            .connect(address)
            .await
            .map_err(|e| format!("Could not connect to {address:?}: {e:?}"))?;
        let (rx, tx) = stream.into_split();
        Ok(Channel {
            reader: rx,
            writer: tx,
        })
    }
}

#[derive(Clone)]
pub struct MemCom {}
#[derive(Clone)]
pub struct MemComListener {
    connection_identifier: Arc<ConnectionIdentifier>,
}

impl MemCom {
    pub fn new() -> Self {
        Self {}
    }
}

impl CommunicationListener for MemComListener {
    type Reader = ReadHalf<SimplexStream>;
    type Writer = WriteHalf<SimplexStream>;
    async fn listen(&mut self) -> Result<Channel<Self::Reader, Self::Writer>> {
        let duplex = memcom_listen(self.connection_identifier.as_ref().clone()).await?;
        Ok(Channel {
            reader: duplex.read,
            writer: duplex.write,
        })
    }
}
impl Communication for MemCom {
    type Listener = MemComListener;
    type Reader = ReadHalf<SimplexStream>;
    type Writer = WriteHalf<SimplexStream>;
    async fn bind(
        &mut self,
        this_connection_identifier: ThisConnectionIdentifier,
    ) -> Result<MemComListener> {
        Ok(MemComListener {
            connection_identifier: Arc::new(this_connection_identifier.into()),
        })
    }
    async fn connect(
        &self,
        identifier: &ConnectionIdentifier,
    ) -> Result<Channel<ReadHalf<SimplexStream>, WriteHalf<SimplexStream>>> {
        let channel = memcom_connect(identifier).await?;
        Ok(Channel {
            reader: channel.read,
            writer: channel.write,
        })
    }
}
