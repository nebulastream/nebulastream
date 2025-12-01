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
use futures::task::noop_waker_ref;
use pin_project::{pin_project, pinned_drop};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync;
use std::task::{Context, Poll};
use tokio::io::{AsyncWrite, ReadHalf, SimplexStream, WriteHalf};
use tokio::sync::mpsc::error::SendError;
use tokio_retry2::strategy::{ExponentialBackoff, jitter};
use tokio_retry2::{Retry, RetryError};
use tracing::warn;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
pub struct Channel {
    pub read: ReadHalf<SimplexStream>,
    pub write: SimplexStreamWriter,
}
/// By default, a SimplexStream does not shut down the ReadHalf.
/// SimplexStreamWriter is a wrapper the WriteHalf<SimplexStream> that does call shutdown on drop
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
    INSTANCE.bind(connection_identifier).await
}

pub async fn memcom_connect(connection_identifier: &ConnectionIdentifier) -> Result<Channel> {
    INSTANCE.connect(connection_identifier).await
}

impl MemCom {
    async fn bind(
        &self,
        connection: ConnectionIdentifier,
    ) -> Result<tokio::sync::mpsc::Receiver<Channel>> {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        let mut locked = self.listening.write().await;
        if let Some(_) = locked.insert(connection.clone(), tx) {
            warn!("Rebinding {connection}");
        }
        Ok(rx)
    }

    async fn connect(&self, connection: &ConnectionIdentifier) -> Result<Channel> {
        let (client_read, server_write) = tokio::io::simplex(1024 * 1024);
        let (server_read, client_write) = tokio::io::simplex(1024 * 1024);

        let server_channel = Channel {
            read: server_read,
            write: SimplexStreamWriter::new(server_write),
        };

        let client_channel = Channel {
            read: client_read,
            write: SimplexStreamWriter::new(client_write),
        };

        async fn action(
            this: &MemCom,
            connection: &ConnectionIdentifier,
        ) -> core::result::Result<tokio::sync::mpsc::Sender<Channel>, RetryError<Error>> {
            let channel = this.listening.read().await.get(connection).cloned();
            let Some(channel) = channel else {
                warn!("could not connect to {}. retrying...", connection);
                return RetryError::to_transient("unreachable".into());
            };
            Ok(channel)
        }

        let retry = ExponentialBackoff::from_millis(2)
            .max_delay_millis(32)
            .map(jitter)
            .take(10);
        let channel = Retry::spawn(retry, || async { action(self, connection).await }).await?;
        match channel.send(server_channel).await {
            Ok(_) => Ok(client_channel),
            Err(SendError(_)) => {
                self.listening.write().await.remove(connection);
                Err("could not connect".into())
            }
        }
    }
}
