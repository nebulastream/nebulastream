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

//! Communication channel abstractions for network and in-memory connections.
//!
//! This module provides a unified interface for different types of bidirectional communication:
//! - TCP-based network communication for distributed systems
//! - In-memory channels for local process communication
//!
//! The core abstraction is the `Communication` trait, which allows the networking layer
//! to be agnostic to the underlying transport mechanism.

use crate::memcom;
use crate::memcom::{SimplexStreamWriter, memcom_bind, memcom_connect};
use crate::protocol::{ConnectionIdentifier, ThisConnectionIdentifier};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, SimplexStream};
use tokio::net::TcpSocket;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

/// A bidirectional communication channel with separate reader and writer halves.
///
/// This generic structure allows for different underlying transport mechanisms
/// (TCP sockets, in-memory channels, etc.) while providing a consistent interface.
pub struct Channel<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send> {
    pub reader: R,
    pub writer: W,
}

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// A listener that accepts incoming connections and produces reader/writer pairs.
///
/// Each implementation defines the specific reader and writer types for its connections.
pub trait CommunicationListener: Send + Sync {
    type Reader: AsyncRead + Unpin + Send;
    type Writer: AsyncWrite + Unpin + Send;
    fn listen(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Channel<Self::Reader, Self::Writer>>> + Send;
}

/// Main abstraction for communication backends.
///
/// Implementations provide both client (connect) and server (bind/listen) functionality.
/// The trait is cloneable to allow sharing across multiple async tasks.
pub trait Communication: Send + Sync + Clone {
    type Listener: CommunicationListener;
    type Reader: AsyncRead + Unpin + Send;
    type Writer: AsyncWrite + Unpin + Send;
    fn bind(
        &mut self,
        this_connection_identifier: ThisConnectionIdentifier,
    ) -> impl Future<Output = Result<Self::Listener>> + Send;
    fn connect(
        &self,
        identifier: &ConnectionIdentifier,
    ) -> impl Future<Output = Result<Channel<Self::Reader, Self::Writer>>> + Send;
}

/// TCP-based network communication implementation.
#[derive(Clone)]
pub struct TcpCommunication {
    // Stateless: creates sockets on demand using the underlying C API
}

/// Listener for accepting incoming TCP connections.
#[derive(Clone)]
pub struct TcpCommunicationListener {
    // Wrapped in Arc<Mutex<>> to allow cloning while maintaining exclusive access to the listener
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

/// MemCom (Memory Communication) provides in-memory channel communication
/// for local connections, as opposed to network-based TCP communication.
#[derive(Clone)]
pub struct MemCom {
    // Stateless: creates in-memory channels on demand via the memcom registry
}

/// Listener for accepting incoming in-memory connections.
pub struct MemComListener {
    // Receives channel pairs from the memcom registry when remote peers connect
    incoming_connections: tokio::sync::mpsc::Receiver<memcom::Channel>,
}

impl MemCom {
    pub fn new() -> Self {
        Self {}
    }
}

impl CommunicationListener for MemComListener {
    type Reader = ReadHalf<SimplexStream>;
    type Writer = SimplexStreamWriter;
    async fn listen(&mut self) -> Result<Channel<Self::Reader, Self::Writer>> {
        let duplex = self
            .incoming_connections
            .recv()
            .await
            .ok_or("Could not receive connection")?;
        Ok(Channel {
            reader: duplex.read,
            writer: duplex.write,
        })
    }
}
impl Communication for MemCom {
    type Listener = MemComListener;
    type Reader = ReadHalf<SimplexStream>;
    type Writer = SimplexStreamWriter;
    async fn bind(
        &mut self,
        this_connection_identifier: ThisConnectionIdentifier,
    ) -> Result<MemComListener> {
        Ok(MemComListener {
            incoming_connections: memcom_bind(this_connection_identifier.into()).await?,
        })
    }
    async fn connect(
        &self,
        identifier: &ConnectionIdentifier,
    ) -> Result<Channel<ReadHalf<SimplexStream>, SimplexStreamWriter>> {
        let channel = memcom_connect(identifier).await?;
        Ok(Channel {
            reader: channel.read,
            writer: channel.write,
        })
    }
}
