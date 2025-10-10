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
use once_cell::sync;
use std::collections::HashMap;
use tokio::io::{ReadHalf, SimplexStream, WriteHalf};
use tokio::sync::mpsc::error::SendError;
use tokio_retry2::strategy::{ExponentialBackoff, jitter};
use tokio_retry2::{Retry, RetryError};
use tracing::{warn};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
pub struct Channel {
    pub read: ReadHalf<SimplexStream>,
    pub write: WriteHalf<SimplexStream>,
}
struct MemCom {
    listening:
        tokio::sync::RwLock<HashMap<ConnectionIdentifier, tokio::sync::mpsc::Sender<Channel>>>,
}

static INSTANCE: sync::Lazy<MemCom> = sync::Lazy::new(|| MemCom {
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
        match locked.entry(connection.clone()) {
            std::collections::hash_map::Entry::Occupied(_) => return Err("cannot bind".into()),
            std::collections::hash_map::Entry::Vacant(v) => v.insert(tx),
        };
        Ok(rx)
    }

    async fn connect(&self, connection: &ConnectionIdentifier) -> Result<Channel> {
        let (client_read, server_write) = tokio::io::simplex(1024 * 1024);
        let (server_read, client_write) = tokio::io::simplex(1024 * 1024);

        let server_channel = Channel {
            read: server_read,
            write: server_write,
        };

        let client_channel = Channel {
            read: client_read,
            write: client_write,
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
