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

use nes_network::protocol::{ConnectionIdentifier, ThisConnectionIdentifier, TupleBuffer};
use nes_network::receiver::{ReceiverChannel, ReceiverChannelResult};
use nes_network::sender::{SenderChannel, TrySendDataResult};
use nes_network::*;
use std::collections::HashMap;
use std::error::Error;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tracing::warn;

#[cxx::bridge]
pub mod ffi {
    enum SendResult {
        Ok,
        Closed,
        Full,
    }

    struct SerializedTupleBufferHeader {
        sequence_number: u64,
        origin_id: u64,
        chunk_number: u64,
        number_of_tuples: u64,
        watermark: u64,
        last_chunk: bool,
    }

    /// Configuration for network services (sender and receiver).
    /// Passed once during initialization and applied to all channels on this worker.
    struct NetworkServiceOptions {
        /// Size of the sender software queue per channel (default: 1024).
        sender_queue_size: u32,
        /// Maximum number of in-flight buffers awaiting acknowledgment per channel (default: 64).
        max_pending_acks: u32,
        /// Size of the receiver data queue per channel (default: 10).
        receiver_queue_size: u32,
        /// Number of IO threads for the sender tokio runtime. 0 means use the number of available cores.
        sender_io_threads: u32,
        /// Number of IO threads for the receiver tokio runtime. 0 means use the number of available cores.
        receiver_io_threads: u32,
    }

    unsafe extern "C++" {
        include!("NetworkBindings.hpp");
        type TupleBufferBuilder;
        #[allow(non_snake_case)]
        fn setMetadata(self: Pin<&mut TupleBufferBuilder>, meta: &SerializedTupleBufferHeader);
        #[allow(non_snake_case)]
        fn setData(self: Pin<&mut TupleBufferBuilder>, data: &[u8]);
        #[allow(non_snake_case)]
        fn addChildBuffer(self: Pin<&mut TupleBufferBuilder>, data: &[u8]);
        #[allow(non_snake_case)]
        fn identifyThread(thread_name: &str, worker_id: &str);
    }

    extern "Rust" {
        type ReceiverNetworkService;
        type SenderNetworkService;
        type SenderDataChannel;
        type ReceiverDataChannel;

        fn enable_memcom();
        fn init_receiver_service(
            connection_addr: String,
            worker_id: String,
            options: &NetworkServiceOptions,
        ) -> Result<()>;
        fn receiver_instance(connection_addr: String) -> Result<Box<ReceiverNetworkService>>;
        fn init_sender_service(
            connection_addr: String,
            worker_id: String,
            options: &NetworkServiceOptions,
        ) -> Result<()>;
        fn sender_instance(connection_addr: String) -> Result<Box<SenderNetworkService>>;

        fn register_receiver_channel(
            server: &mut ReceiverNetworkService,
            channel_identifier: String,
            options: &NetworkServiceOptions,
        ) -> Result<Box<ReceiverDataChannel>>;
        fn receive_buffer(
            receiver_channel: &ReceiverDataChannel,
            builder: Pin<&mut TupleBufferBuilder>,
        ) -> Result<bool>;
        fn interrupt_receive(receiver_channel: &ReceiverDataChannel);

        fn close_receiver_channel(channel: Box<ReceiverDataChannel>);

        fn register_sender_channel(
            server: &SenderNetworkService,
            connection_identifier: String,
            channel_identifier: String,
            options: &NetworkServiceOptions,
        ) -> Result<Box<SenderDataChannel>>;

        fn close_sender_channel(channel: Box<SenderDataChannel>);
        fn flush_sender_channel(channel: &SenderDataChannel) -> bool;
        fn send_buffer(
            channel: &SenderDataChannel,
            metadata: SerializedTupleBufferHeader,
            data: &[u8],
            children: &[&[u8]],
        ) -> SendResult;
    }
}

#[derive(Clone)]
enum ReceiverService {
    MemCom(Arc<receiver::NetworkService<channel::MemCom>>),
    Tcp(Arc<receiver::NetworkService<channel::TcpCommunication>>),
}
#[derive(Clone)]
enum SenderService {
    MemCom(Arc<sender::NetworkService<channel::MemCom>>),
    Tcp(Arc<sender::NetworkService<channel::TcpCommunication>>),
}
#[derive(Default)]
struct Services {
    receivers: HashMap<ThisConnectionIdentifier, (ReceiverService, usize)>,
    senders: HashMap<ThisConnectionIdentifier, (SenderService, sender::SenderConfig)>,
}

static USE_MEMCOM: Mutex<bool> = Mutex::new(false);
fn enable_memcom() {
    let mut use_memcom = USE_MEMCOM.lock().unwrap();
    if *use_memcom {
        warn!("Memcom is already enabled");
    }
    *use_memcom = true;
}
/// Lazy singleton types, initialized on first use in a thread-safe way
static SERVICES: std::sync::LazyLock<Mutex<Services>> =
    std::sync::LazyLock::new(|| Mutex::default());

pub struct ReceiverNetworkService {
    handle: ReceiverService,
    /// Worker-level default for the receiver data queue size.
    default_data_queue_size: usize,
}

struct SenderNetworkService {
    handle: SenderService,
    /// Worker-level defaults for sender channel configuration.
    default_config: sender::SenderConfig,
}

/// Wrapper around `SenderChannel` for C++ FFI.
///
/// This wrapper is required by the `cxx` crate to expose Rust types to C++.
/// The `chan` field contains the actual channel implementation. The indirection
/// is necessary because `cxx` requires opaque types to be wrapped in structs.
struct SenderDataChannel {
    chan: SenderChannel,
}

/// Wrapper around `ReceiverChannel` for C++ FFI.
///
/// This wrapper is required by the `cxx` crate to expose Rust types to C++.
/// The `chan` field contains the actual channel implementation, wrapped in
/// `Pin<Box<>>` because `async_channel::Receiver` is not `Unpin`. The indirection
/// is necessary because `cxx` requires opaque types to be wrapped in structs.
struct ReceiverDataChannel {
    chan: Pin<Box<ReceiverChannel>>,
}

fn init_sender_service(
    this_connection_addr: String,
    worker_id: String,
    options: &ffi::NetworkServiceOptions,
) -> Result<(), String> {
    let this_connection = ThisConnectionIdentifier::from_str(this_connection_addr.as_str())
        .map_err(|e| e.to_string())?;
    let mut services = SERVICES.lock().unwrap();

    let config = sender::SenderConfig {
        sender_queue_size: options.sender_queue_size as usize,
        max_pending_acks: options.max_pending_acks as usize,
    };

    // Validate: TCP mode allows only one service per process
    let use_memcom = *USE_MEMCOM.lock().unwrap();
    if !use_memcom && !services.senders.is_empty() {
        return Err("TCP mode allows only one sender service per process".to_string());
    }

    let old = services.senders.insert(this_connection.clone(), {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .thread_name("net-sender")
            .on_thread_start(move || ffi::identifyThread("net-sender", &worker_id))
            .enable_io()
            .enable_time();
        if options.sender_io_threads > 0 {
            builder.worker_threads(options.sender_io_threads as usize);
        }
        let runtime = builder.build().expect("Failed to create tokio runtime");
        let service = if *USE_MEMCOM.lock().unwrap() {
            SenderService::MemCom(sender::NetworkService::start(
                runtime,
                this_connection.clone(),
                channel::MemCom::new(),
            ))
        } else {
            SenderService::Tcp(sender::NetworkService::start(
                runtime,
                this_connection.clone(),
                channel::TcpCommunication::new(),
            ))
        };
        (service, config)
    });

    if let Some((old_service, _)) = old {
        warn!("Recreating sender service for {this_connection}, shutting down old service");
        // Properly shutdown the old service to avoid resource leaks
        let shutdown_result = match old_service {
            SenderService::MemCom(service) => service.shutdown(),
            SenderService::Tcp(service) => service.shutdown(),
        };
        if let Err(e) = shutdown_result {
            warn!("Failed to shutdown old sender service: {e}");
        }
    }

    Ok(())
}

fn init_receiver_service(
    connection_addr: String,
    worker_id: String,
    options: &ffi::NetworkServiceOptions,
) -> Result<(), String> {
    let this_connection =
        ThisConnectionIdentifier::from_str(connection_addr.as_str()).map_err(|e| e.to_string())?;
    let mut services = SERVICES.lock().unwrap();

    let data_queue_size = options.receiver_queue_size as usize;

    // Validate: TCP mode allows only one service per process
    let use_memcom = *USE_MEMCOM.lock().unwrap();
    if !use_memcom && !services.receivers.is_empty() {
        return Err("TCP mode allows only one receiver service per process".to_string());
    }

    let old = services.receivers.insert(this_connection.clone(), {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .thread_name("net-receiver")
            .on_thread_start(move || ffi::identifyThread("net-receiver", &worker_id))
            .enable_io()
            .enable_time();
        if options.receiver_io_threads > 0 {
            builder.worker_threads(options.receiver_io_threads as usize);
        }
        let runtime = builder.build().expect("Failed to create tokio runtime");
        let service = if *USE_MEMCOM.lock().unwrap() {
            ReceiverService::MemCom(receiver::NetworkService::start(
                runtime,
                this_connection.clone(),
                channel::MemCom::new(),
            ))
        } else {
            ReceiverService::Tcp(receiver::NetworkService::start(
                runtime,
                this_connection.clone(),
                channel::TcpCommunication::new(),
            ))
        };
        (service, data_queue_size)
    });

    if let Some((old_service, _)) = old {
        warn!("Recreating receiver service for {this_connection}, shutting down old service");
        // Properly shutdown the old service to avoid resource leaks
        let shutdown_result = match old_service {
            ReceiverService::MemCom(service) => service.shutdown(),
            ReceiverService::Tcp(service) => service.shutdown(),
        };
        if let Err(e) = shutdown_result {
            warn!("Failed to shutdown old receiver service: {e}");
        }
    }

    Ok(())
}

fn receiver_instance(
    connection_identifier: String,
) -> Result<Box<ReceiverNetworkService>, Box<dyn Error>> {
    let this_connection = ThisConnectionIdentifier::from_str(connection_identifier.as_str())
        .map_err(|e| e.to_string())?;
    let services = SERVICES.lock().unwrap();
    let (service, default_data_queue_size) = services
        .receivers
        .get(&this_connection)
        .ok_or("Receiver server has not been initialized yet.")?;
    Ok(Box::new(ReceiverNetworkService {
        handle: service.clone(),
        default_data_queue_size: *default_data_queue_size,
    }))
}

fn sender_instance(
    connection_identifier: String,
) -> Result<Box<SenderNetworkService>, Box<dyn Error>> {
    let this_connection = ThisConnectionIdentifier::from_str(connection_identifier.as_str())
        .map_err(|e| e.to_string())?;
    let services = SERVICES.lock().unwrap();
    let (service, default_config) = services
        .senders
        .get(&this_connection)
        .ok_or("Sender server has not been initialized yet.")?;
    Ok(Box::new(SenderNetworkService {
        handle: service.clone(),
        default_config: default_config.clone(),
    }))
}

fn register_receiver_channel(
    receiver_service: &mut ReceiverNetworkService,
    channel_identifier: String,
    options: &ffi::NetworkServiceOptions,
) -> Result<Box<ReceiverDataChannel>, String> {
    // Channel-level override: 0 means use worker default
    let data_queue_size = if options.receiver_queue_size > 0 {
        options.receiver_queue_size as usize
    } else {
        receiver_service.default_data_queue_size
    };

    // register_channel can fail if the receiver service has been shut down.
    // This should not happen in normal operation as the service is kept alive
    // for the lifetime of the worker.
    let queue = match &receiver_service.handle {
        ReceiverService::MemCom(r) => {
            r.register_channel(channel_identifier.clone(), data_queue_size)
        }
        ReceiverService::Tcp(r) => r.register_channel(channel_identifier.clone(), data_queue_size),
    }
    .map_err(|_| "The receiver channel was shutdown unexpectedly.")?;

    Ok(Box::new(ReceiverDataChannel {
        chan: Box::pin(queue),
    }))
}

/// Receives a tuple buffer from the channel and populates the builder.
///
/// # Returns
///
/// - `Ok(true)`: A buffer was successfully received and the builder was populated
/// - `Ok(false)`: The channel was closed (no more data available)
/// - `Err(_)`: An error occurred while receiving the buffer
fn receive_buffer(
    receiver_channel: &ReceiverDataChannel,
    mut buffer_builder: Pin<&mut ffi::TupleBufferBuilder>,
) -> Result<bool, Box<dyn Error>> {
    let buffer = match receiver_channel.chan.receive() {
        ReceiverChannelResult::Ok(buffer) => buffer,
        ReceiverChannelResult::Closed => {
            return Ok(false);
        }
        ReceiverChannelResult::Error(e) => {
            return Err(format!("Error while receiving buffer: {}", e).into());
        }
    };

    buffer_builder
        .as_mut()
        .setMetadata(&ffi::SerializedTupleBufferHeader {
            sequence_number: buffer.sequence_number as u64,
            origin_id: buffer.origin_id as u64,
            watermark: buffer.watermark as u64,
            chunk_number: buffer.chunk_number as u64,
            number_of_tuples: buffer.number_of_tuples as u64,
            last_chunk: buffer.last_chunk,
        });

    buffer_builder.as_mut().setData(&buffer.data);

    for child_buffer in buffer.child_buffers.iter() {
        assert!(!child_buffer.is_empty());
        buffer_builder.as_mut().addChildBuffer(child_buffer);
    }

    Ok(true)
}

// The `interrupt_receive` and `close_receiver_channel` are identical because,
// currently, there is no requirement to keep the channel alive after a `receive_buffer` has been
// interrupted as the network source will shut down and never try to receive a buffer again.
// And closing the channel is the easiest option to interrupt a pending `receive_buffer`.
fn interrupt_receive(channel: &ReceiverDataChannel) {
    channel.chan.close();
}

// CXX requires the usage of Boxed types
#[allow(clippy::boxed_local)]
fn close_receiver_channel(channel: Box<ReceiverDataChannel>) {
    channel.chan.close();
}

/// Registers a sender channel to send data to a downstream worker.
///
/// # Arguments
/// * `connection_addr` - The URL of the downstream worker (target) that will receive the data
/// * `channel_id` - The identifier for this specific data channel
/// * `options` - Per-channel overrides; fields set to 0 use the worker-level defaults
fn register_sender_channel(
    sender_service: &SenderNetworkService,
    connection_addr: String,
    channel_id: String,
    options: &ffi::NetworkServiceOptions,
) -> Result<Box<SenderDataChannel>, String> {
    // Channel-level overrides: 0 means use worker default
    let config = sender::SenderConfig {
        sender_queue_size: if options.sender_queue_size > 0 {
            options.sender_queue_size as usize
        } else {
            sender_service.default_config.sender_queue_size
        },
        max_pending_acks: if options.max_pending_acks > 0 {
            options.max_pending_acks as usize
        } else {
            sender_service.default_config.max_pending_acks
        },
    };

    let connection_addr =
        ConnectionIdentifier::from_str(connection_addr.as_str()).map_err(|e| e.to_string())?;
    let data_queue = match &sender_service.handle {
        SenderService::MemCom(s) => {
            s.register_channel(connection_addr.clone(), channel_id.clone(), config)
        }
        SenderService::Tcp(s) => {
            s.register_channel(connection_addr.clone(), channel_id.clone(), config)
        }
    }
    .map_err(|_| "The NetworkingService was closed unexpectedly")?;

    Ok(Box::new(SenderDataChannel { chan: data_queue }))
}

fn send_buffer(
    channel: &SenderDataChannel,
    metadata: ffi::SerializedTupleBufferHeader,
    data: &[u8],
    children: &[&[u8]],
) -> ffi::SendResult {
    let buffer = TupleBuffer {
        sequence_number: metadata.sequence_number,
        origin_id: metadata.origin_id,
        chunk_number: metadata.chunk_number,
        number_of_tuples: metadata.number_of_tuples,
        watermark: metadata.watermark,
        last_chunk: metadata.last_chunk,
        data: Vec::from(data),
        child_buffers: children.iter().map(|bytes| Vec::from(*bytes)).collect(),
    };

    // Because we copy the data anyway, we don't have to reuse the buffer if sending failed.
    match channel.chan.try_send_data(buffer) {
        TrySendDataResult::Ok => ffi::SendResult::Ok,
        TrySendDataResult::Full(_) => ffi::SendResult::Full,
        TrySendDataResult::Closed(_) => ffi::SendResult::Closed,
    }
}
fn flush_sender_channel(channel: &SenderDataChannel) -> bool {
    // If the channel has been closed, we pretend it has been flushed
    channel.chan.flush().unwrap_or(true)
}

// CXX requires the usage of Boxed types
#[allow(clippy::boxed_local)]
fn close_sender_channel(channel: Box<SenderDataChannel>) {
    channel.chan.close();
}
