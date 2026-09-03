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

use crate::protocol::ConnectionIdentifier;
use crate::{check_io, fault_testing};
use futures::task::noop_waker_ref;
use pin_project::{pin_project, pinned_drop};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{io, sync};
use tokio::io::{
    AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf, ReadHalf, SimplexStream, WriteHalf,
};
use tokio::sync::mpsc::error::SendError;
use tokio_retry2::strategy::{ExponentialBackoff, jitter};
use tokio_retry2::{Retry, RetryError};
use tracing::warn;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

// Buffer size for simplex streams (1MB)
const SIMPLEX_BUFFER_SIZE: usize = 1024 * 1024;

// Retry configuration for connection attempts
const INITIAL_RETRY_DELAY_MS: u64 = 2;
const MAX_RETRY_DELAY_MS: u64 = 32;
const MAX_RETRIES: usize = 10;

#[derive(Debug)]
pub struct FaultInjectingReader<R> {
    inner: R,
}

#[derive(Debug)]
pub struct FaultInjectingWriter<W> {
    inner: W,
}

impl<R> FaultInjectingReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<W> FaultInjectingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl<R> AsyncRead for FaultInjectingReader<R>
where
    R: AsyncRead + Unpin + Send,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if check_io!() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection killed",
            )));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<W> AsyncWrite for FaultInjectingWriter<W>
where
    W: AsyncWrite + Unpin + Send,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if check_io!() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection killed",
            )));
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if check_io!() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection killed",
            )));
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if check_io!() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection killed",
            )));
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub struct Channel {
    pub read: FaultInjectingReader<ReadHalf<SimplexStream>>,
    pub write: FaultInjectingWriter<SimplexStreamWriter>,
}
/// By default, a SimplexStream does not shut down the ReadHalf.
/// SimplexStreamWriter is a wrapper around WriteHalf<SimplexStream> that calls shutdown on drop
#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct SimplexStreamWriter {
    #[pin]
    inner: WriteHalf<SimplexStream>,
}
impl SimplexStreamWriter {
    pub fn new(inner: WriteHalf<SimplexStream>) -> Self {
        Self { inner }
    }
}
// AsyncWrite implementation that delegates to the inner WriteHalf.
// The Pin/Context parameters are required by the AsyncWrite trait - Pin ensures
// the writer stays in the same memory location (needed for self-referential async state),
// and Context provides the Waker that allows the async runtime to wake this task.
impl AsyncWrite for SimplexStreamWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}

#[pinned_drop]
impl PinnedDrop for SimplexStreamWriter {
    fn drop(self: Pin<&mut Self>) {
        let mut cx = Context::from_waker(noop_waker_ref());
        let _ = self.project().inner.poll_shutdown(&mut cx);
    }
}

struct MemCom {
    listening:
        tokio::sync::RwLock<HashMap<ConnectionIdentifier, tokio::sync::mpsc::Sender<Channel>>>,
}

static INSTANCE: sync::LazyLock<MemCom> = sync::LazyLock::new(|| MemCom {
    listening: tokio::sync::RwLock::new(HashMap::new()),
});

pub async fn memcom_bind(
    connection_identifier: ConnectionIdentifier,
) -> Result<tokio::sync::mpsc::Receiver<Channel>> {
    INSTANCE.bind(&connection_identifier).await
}

pub async fn memcom_connect(connection_identifier: &ConnectionIdentifier) -> Result<Channel> {
    INSTANCE.connect(connection_identifier).await
}

impl MemCom {
    async fn bind(
        &self,
        connection: &ConnectionIdentifier,
    ) -> Result<tokio::sync::mpsc::Receiver<Channel>> {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        let mut locked = self.listening.write().await;
        if let Some(_) = locked.insert(connection.clone(), tx) {
            warn!("Rebinding {connection}");
        }
        Ok(rx)
    }

    async fn connect(&self, connection: &ConnectionIdentifier) -> Result<Channel> {
        let (client_read, server_write) = tokio::io::simplex(SIMPLEX_BUFFER_SIZE);
        let (server_read, client_write) = tokio::io::simplex(SIMPLEX_BUFFER_SIZE);

        let server_channel = Channel {
            read: FaultInjectingReader::new(server_read),
            write: FaultInjectingWriter::new(SimplexStreamWriter::new(server_write)),
        };

        let client_channel = Channel {
            read: FaultInjectingReader::new(client_read),
            write: FaultInjectingWriter::new(SimplexStreamWriter::new(client_write)),
        };

        async fn try_connect(
            this: &MemCom,
            connection: &ConnectionIdentifier,
        ) -> core::result::Result<tokio::sync::mpsc::Sender<Channel>, RetryError<Error>> {
            let channel = if check_io!() {
                None
            } else {
                this.listening.read().await.get(connection).cloned()
            };
            let Some(channel) = channel else {
                warn!("Could not connect to {}. Retrying...", connection);
                return RetryError::to_transient("Worker not found in registry".into());
            };
            Ok(channel)
        }

        let retry = ExponentialBackoff::from_millis(INITIAL_RETRY_DELAY_MS)
            .max_delay_millis(MAX_RETRY_DELAY_MS)
            .map(jitter)
            .take(MAX_RETRIES);
        let handshake_channel =
            Retry::spawn(retry, || async { try_connect(self, connection).await }).await?;
        match handshake_channel.send(server_channel).await {
            Ok(_) => Ok(client_channel),
            Err(SendError(_)) => Err("could not connect".into()),
        }
    }
}
