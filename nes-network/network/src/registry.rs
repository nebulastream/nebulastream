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

//! Per-process registry of active network services.
//!
//! C++ brings services up via the FFI at worker startup; Rust code (async
//! source/sink tasks running on the shared `IORuntime`) looks them up by
//! address to register channels.

use crate::channel::{MemCom, TcpCommunication};
use crate::protocol::{ConnectionIdentifier, ThisConnectionIdentifier};
use crate::receiver::{self, ReceiverChannel};
use crate::sender::{self, SenderChannel, SenderConfig};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex};
use tokio::runtime::Handle;
use tracing::warn;

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

    fn shutdown(self) {
        let _ = match self {
            SenderService::MemCom(s) => s.shutdown(),
            SenderService::Tcp(s) => s.shutdown(),
        };
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

    fn shutdown(self) {
        let _ = match self {
            ReceiverService::MemCom(s) => s.shutdown(),
            ReceiverService::Tcp(s) => s.shutdown(),
        };
    }
}

#[derive(Default)]
struct Services {
    receivers: HashMap<ThisConnectionIdentifier, (ReceiverService, usize)>,
    senders: HashMap<ThisConnectionIdentifier, (SenderService, SenderConfig)>,
}

static USE_MEMCOM: Mutex<bool> = Mutex::new(false);
static SERVICES: LazyLock<Mutex<Services>> = LazyLock::new(Mutex::default);

pub fn enable_memcom() {
    let mut use_memcom = USE_MEMCOM.lock().expect("registry mutex poisoned");
    if *use_memcom {
        warn!("Memcom is already enabled");
    }
    *use_memcom = true;
}

fn use_memcom() -> bool {
    *USE_MEMCOM.lock().expect("registry mutex poisoned")
}

pub fn init_sender_service(
    this_connection_addr: &str,
    runtime: &Handle,
    default_config: SenderConfig,
) -> Result<(), String> {
    let this_connection =
        ThisConnectionIdentifier::from_str(this_connection_addr).map_err(|e| e.to_string())?;
    let mut services = SERVICES.lock().expect("registry mutex poisoned");

    if !use_memcom() && !services.senders.is_empty() {
        return Err("TCP mode allows only one sender service per process".to_string());
    }

    let service = if use_memcom() {
        SenderService::MemCom(sender::NetworkService::start(
            runtime,
            this_connection.clone(),
            MemCom::new(),
        ))
    } else {
        SenderService::Tcp(sender::NetworkService::start(
            runtime,
            this_connection.clone(),
            TcpCommunication::new(),
        ))
    };

    if let Some((old, _)) = services
        .senders
        .insert(this_connection.clone(), (service, default_config))
    {
        warn!("Recreating sender service for {this_connection}, shutting down old service");
        old.shutdown();
    }

    Ok(())
}

pub fn init_receiver_service(
    this_connection_addr: &str,
    runtime: &Handle,
    default_data_queue_size: usize,
) -> Result<(), String> {
    let this_connection =
        ThisConnectionIdentifier::from_str(this_connection_addr).map_err(|e| e.to_string())?;
    let mut services = SERVICES.lock().expect("registry mutex poisoned");

    if !use_memcom() && !services.receivers.is_empty() {
        return Err("TCP mode allows only one receiver service per process".to_string());
    }

    let service = if use_memcom() {
        ReceiverService::MemCom(receiver::NetworkService::start(
            runtime,
            this_connection.clone(),
            MemCom::new(),
        ))
    } else {
        ReceiverService::Tcp(receiver::NetworkService::start(
            runtime,
            this_connection.clone(),
            TcpCommunication::new(),
        ))
    };

    if let Some((old, _)) = services
        .receivers
        .insert(this_connection.clone(), (service, default_data_queue_size))
    {
        warn!("Recreating receiver service for {this_connection}, shutting down old service");
        old.shutdown();
    }

    Ok(())
}

pub fn sender_service(addr: &str) -> Result<(SenderService, SenderConfig), String> {
    let this_connection = ThisConnectionIdentifier::from_str(addr).map_err(|e| e.to_string())?;
    SERVICES
        .lock()
        .expect("SERVICES mutex poisoned")
        .senders
        .get(&this_connection)
        .cloned()
        .ok_or_else(|| "Sender server has not been initialized yet.".to_string())
}

pub fn receiver_service(addr: &str) -> Result<(ReceiverService, usize), String> {
    let this_connection = ThisConnectionIdentifier::from_str(addr).map_err(|e| e.to_string())?;
    SERVICES
        .lock()
        .expect("SERVICES mutex poisoned")
        .receivers
        .get(&this_connection)
        .cloned()
        .ok_or_else(|| "Receiver server has not been initialized yet.".to_string())
}
