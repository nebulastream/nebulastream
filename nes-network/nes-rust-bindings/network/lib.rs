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

    unsafe extern "C++" {
        include!("NetworkBindings.hpp");
        type TupleBufferBuilder;
        #[allow(non_snake_case)]
        fn setMetadata(self: Pin<&mut TupleBufferBuilder>, meta: &SerializedTupleBufferHeader);
        #[allow(non_snake_case)]
        fn setData(self: Pin<&mut TupleBufferBuilder>, data: &[u8], is_encoded: bool);
        #[allow(non_snake_case)]
        fn addChildBuffer(self: Pin<&mut TupleBufferBuilder>, data: &[u8], is_encoded: bool);
        #[allow(non_snake_case)]
        fn identifyThread(thread_name: &str, worker_id: &str);
    }

    extern "Rust" {
        type ReceiverNetworkService;
        type SenderNetworkService;
        type SenderDataChannel;
        type ReceiverDataChannel;

        fn enable_memcom();
        fn init_receiver_service(connection_addr: String, worker_id: String) -> Result<()>;
        fn receiver_instance(connection_addr: String) -> Result<Box<ReceiverNetworkService>>;
        fn init_sender_service(connection_addr: String, worker_id: String) -> Result<()>;
        fn sender_instance(connection_addr: String) -> Result<Box<SenderNetworkService>>;

        fn register_receiver_channel(
            server: &mut ReceiverNetworkService,
            channel_identifier: String,
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
        ) -> Result<Box<SenderDataChannel>>;

        fn close_sender_channel(channel: Box<SenderDataChannel>);
        fn flush_sender_channel(channel: &SenderDataChannel) -> bool;
        fn send_buffer(
            channel: &SenderDataChannel,
            metadata: SerializedTupleBufferHeader,
            encoded_data: bool,
            encoded_children: &[u8],
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
    receiver: HashMap<ThisConnectionIdentifier, ReceiverService>,
    sender: HashMap<ThisConnectionIdentifier, SenderService>,
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
}

struct SenderNetworkService {
    handle: SenderService,
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

fn init_sender_service(this_connection_addr: String, worker_id: String) -> Result<(), String> {
    let this_connection = ThisConnectionIdentifier::from_str(this_connection_addr.as_str())
        .map_err(|e| e.to_string())?;
    let mut services = SERVICES.lock().unwrap();
    let old = services.sender.insert(this_connection.clone(), {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("net-sender")
            .worker_threads(1)
            .on_thread_start(move || ffi::identifyThread("net-sender", &worker_id))
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        if *USE_MEMCOM.lock().unwrap() {
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
        }
    });

    if let Some(old_service) = old {
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

fn init_receiver_service(connection_addr: String, worker_id: String) -> Result<(), String> {
    let this_connection =
        ThisConnectionIdentifier::from_str(connection_addr.as_str()).map_err(|e| e.to_string())?;
    let mut services = SERVICES.lock().unwrap();
    let old = services.receiver.insert(this_connection.clone(), {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("net-receiver")
            .worker_threads(1)
            .enable_io()
            .enable_time()
            .on_thread_start(move || ffi::identifyThread("net-receiver", &worker_id))
            .build()
            .unwrap();
        if *USE_MEMCOM.lock().unwrap() {
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
        }
    });

    if let Some(old_service) = old {
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
    let receiver = services
        .receiver
        .get(&this_connection)
        .ok_or("Receiver server has not been initialized yet.")?;
    Ok(Box::new(ReceiverNetworkService {
        handle: receiver.clone(),
    }))
}

fn sender_instance(
    connection_identifier: String,
) -> Result<Box<SenderNetworkService>, Box<dyn Error>> {
    let this_connection = ThisConnectionIdentifier::from_str(connection_identifier.as_str())
        .map_err(|e| e.to_string())?;
    let services = SERVICES.lock().unwrap();
    let sender = services
        .sender
        .get(&this_connection)
        .ok_or("Sender server has not been initialized yet.")?;
    Ok(Box::new(SenderNetworkService {
        handle: sender.clone(),
    }))
}

fn register_receiver_channel(
    receiver_service: &mut ReceiverNetworkService,
    channel_identifier: String,
) -> Result<Box<ReceiverDataChannel>, String> {
    // register_channel can fail if the receiver service has been shut down.
    // This should not happen in normal operation as the service is kept alive
    // for the lifetime of the worker.
    let queue = match &receiver_service.handle {
        ReceiverService::MemCom(r) => r.register_channel(channel_identifier.clone()),
        ReceiverService::Tcp(r) => r.register_channel(channel_identifier.clone()),
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

    buffer_builder.as_mut().setData(&buffer.data, buffer.encoded_data as bool);

    let mut i : usize = 0;
    for child_buffer in buffer.child_buffers.iter() {
        let is_encoded : bool = buffer.encoded_children[i];
        assert!(!child_buffer.is_empty());
        buffer_builder.as_mut().addChildBuffer(child_buffer, is_encoded);
        i += 1;
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
fn register_sender_channel(
    sender_service: &SenderNetworkService,
    connection_addr: String,
    channel_id: String,
) -> Result<Box<SenderDataChannel>, String> {
    let connection_addr =
        ConnectionIdentifier::from_str(connection_addr.as_str()).map_err(|e| e.to_string())?;
    let data_queue = match &sender_service.handle {
        SenderService::MemCom(s) => s.register_channel(connection_addr.clone(), channel_id.clone()),
        SenderService::Tcp(s) => s.register_channel(connection_addr.clone(), channel_id.clone()),
    }
    .map_err(|_| "The NetworkingService was closed unexpectedly")?;

    Ok(Box::new(SenderDataChannel { chan: data_queue }))
}

fn send_buffer(
    channel: &SenderDataChannel,
    metadata: ffi::SerializedTupleBufferHeader,
    encoded_data: bool,
    encoded_children: &[u8],
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
        encoded_data: encoded_data,
        encoded_children: encoded_children.iter().map(|&value| value == 1).collect(),
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
