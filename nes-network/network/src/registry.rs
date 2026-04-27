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

//! Per-`IORuntime` lookup of the network sender/receiver services.
//!
//! C++ attaches a JSON config blob to the [`IORuntime::configs`] under
//! [`SENDER_SERVICE_CONFIG`] / [`RECEIVER_SERVICE_CONFIG`] at worker startup.
//! [`sender_service`] / [`receiver_service`] then construct the service on
//! first use via [`ServiceRegistry::try_get_or_init`], reading the config back
//! and binding the underlying TCP/MemCom listener.

use crate::channel::{MemCom, TcpCommunication};
use crate::protocol::{ConnectionIdentifier, ThisConnectionIdentifier};
use crate::receiver::{self, ReceiverChannel};
use crate::sender::{self, SenderChannel, SenderConfig};
use nes_io_runtime::IORuntime;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tracing::warn;

/// Config-registry name under which the sender service config is attached.
pub const SENDER_SERVICE_CONFIG: &str = "nes-network-sender";
/// Config-registry name under which the receiver service config is attached.
pub const RECEIVER_SERVICE_CONFIG: &str = "nes-network-receiver";

/// JSON-encoded shape attached to the IORuntime under [`SENDER_SERVICE_CONFIG`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SenderServiceConfig {
    /// Bind address used to identify this connection.
    pub this_connection: String,
    /// Default size of the per-channel sender software queue.
    #[serde(default)]
    pub sender_queue_size: usize,
    /// Default cap on in-flight buffers awaiting acknowledgement.
    #[serde(default)]
    pub max_pending_acks: usize,
}

/// JSON-encoded shape attached to the IORuntime under [`RECEIVER_SERVICE_CONFIG`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiverServiceConfig {
    /// Bind address used to identify this connection.
    pub this_connection: String,
    /// Default size of the per-channel receiver data queue.
    #[serde(default)]
    pub receiver_queue_size: usize,
}

#[derive(Clone)]
pub enum SenderService {
    MemCom(Arc<sender::NetworkService<MemCom>>),
    Tcp(Arc<sender::NetworkService<TcpCommunication>>),
}

impl SenderService {
    pub async fn register_channel(
        &self,
        connection: ConnectionIdentifier,
        channel: String,
        config: SenderConfig,
    ) -> Result<SenderChannel, String> {
        match self {
            SenderService::MemCom(s) => s.register_channel(connection, channel, config).await,
            SenderService::Tcp(s) => s.register_channel(connection, channel, config).await,
        }
        .map_err(|_| "The NetworkingService was closed unexpectedly".to_string())
    }
}

#[derive(Clone)]
pub enum ReceiverService {
    MemCom(Arc<receiver::NetworkService<MemCom>>),
    Tcp(Arc<receiver::NetworkService<TcpCommunication>>),
}

impl ReceiverService {
    pub async fn register_channel(
        &self,
        channel: String,
        data_queue_size: usize,
    ) -> Result<ReceiverChannel, String> {
        match self {
            ReceiverService::MemCom(r) => r.register_channel(channel, data_queue_size).await,
            ReceiverService::Tcp(r) => r.register_channel(channel, data_queue_size).await,
        }
        .map_err(|_| "The receiver channel was shutdown unexpectedly.".to_string())
    }
}

/// Sender service plus the defaults that travel with it.
///
/// Stored in the per-IORuntime [`ServiceRegistry`] as `Arc<SharedSenderService>`.
/// When the last `Arc` drops, the underlying network service is shut down.
pub struct SharedSenderService {
    pub service: SenderService,
    pub default_config: SenderConfig,
}

impl Drop for SharedSenderService {
    fn drop(&mut self) {
        match &self.service {
            SenderService::MemCom(s) => {
                let _ = s.clone().shutdown();
            }
            SenderService::Tcp(s) => {
                let _ = s.clone().shutdown();
            }
        }
    }
}

/// Receiver service plus the defaults that travel with it.
pub struct SharedReceiverService {
    pub service: ReceiverService,
    pub default_queue_size: usize,
}

impl Drop for SharedReceiverService {
    fn drop(&mut self) {
        match &self.service {
            ReceiverService::MemCom(s) => {
                let _ = s.clone().shutdown();
            }
            ReceiverService::Tcp(s) => {
                let _ = s.clone().shutdown();
            }
        }
    }
}

static USE_MEMCOM: Mutex<bool> = Mutex::new(false);

/// Process-wide switch that routes the network service through the in-memory
/// channel implementation instead of TCP. Exported as a C symbol so C++ test
/// drivers (REPL, systest) can flip it without going through cxx.
#[unsafe(no_mangle)]
pub extern "C" fn enable_memcom() {
    let mut use_memcom = USE_MEMCOM.lock().expect("USE_MEMCOM mutex poisoned");
    if *use_memcom {
        warn!("Memcom is already enabled");
    }
    *use_memcom = true;
}

fn use_memcom() -> bool {
    *USE_MEMCOM.lock().expect("USE_MEMCOM mutex poisoned")
}

/// Returns the shared sender service for `io`, building it on first use.
///
/// Reads the JSON blob attached under [`SENDER_SERVICE_CONFIG`] in
/// `io.configs()`. Subsequent calls return the same `Arc` until the last
/// consumer drops it; the underlying network service is then torn down and
/// the next call re-binds.
pub fn sender_service(io: &IORuntime) -> Result<Arc<SharedSenderService>, String> {
    io.services()
        .try_get_or_init::<SharedSenderService, _, String>(|| {
            let raw = io
                .configs()
                .get(SENDER_SERVICE_CONFIG)
                .ok_or_else(|| {
                    format!("{SENDER_SERVICE_CONFIG} config not attached to IORuntime")
                })?;
            let cfg: SenderServiceConfig = serde_json::from_str(&raw)
                .map_err(|e| format!("invalid {SENDER_SERVICE_CONFIG} config: {e}"))?;
            let this_connection = ThisConnectionIdentifier::from_str(&cfg.this_connection)
                .map_err(|e| format!("invalid this_connection `{}`: {e}", cfg.this_connection))?;
            let handle = io.handle();
            let service = if use_memcom() {
                SenderService::MemCom(sender::NetworkService::start(
                    &handle,
                    this_connection,
                    MemCom::new(),
                ))
            } else {
                SenderService::Tcp(sender::NetworkService::start(
                    &handle,
                    this_connection,
                    TcpCommunication::new(),
                ))
            };
            Ok(Arc::new(SharedSenderService {
                service,
                default_config: SenderConfig {
                    sender_queue_size: cfg.sender_queue_size,
                    max_pending_acks: cfg.max_pending_acks,
                },
            }))
        })
}

/// Returns the shared receiver service for `io`, building it on first use.
pub fn receiver_service(io: &IORuntime) -> Result<Arc<SharedReceiverService>, String> {
    io.services()
        .try_get_or_init::<SharedReceiverService, _, String>(|| {
            let raw = io
                .configs()
                .get(RECEIVER_SERVICE_CONFIG)
                .ok_or_else(|| {
                    format!("{RECEIVER_SERVICE_CONFIG} config not attached to IORuntime")
                })?;
            let cfg: ReceiverServiceConfig = serde_json::from_str(&raw)
                .map_err(|e| format!("invalid {RECEIVER_SERVICE_CONFIG} config: {e}"))?;
            let this_connection = ThisConnectionIdentifier::from_str(&cfg.this_connection)
                .map_err(|e| format!("invalid this_connection `{}`: {e}", cfg.this_connection))?;
            let handle = io.handle();
            let service = if use_memcom() {
                ReceiverService::MemCom(receiver::NetworkService::start(
                    &handle,
                    this_connection,
                    MemCom::new(),
                ))
            } else {
                ReceiverService::Tcp(receiver::NetworkService::start(
                    &handle,
                    this_connection,
                    TcpCommunication::new(),
                ))
            };
            Ok(Arc::new(SharedReceiverService {
                service,
                default_queue_size: cfg.receiver_queue_size,
            }))
        })
}
