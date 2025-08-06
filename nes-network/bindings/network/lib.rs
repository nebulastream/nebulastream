use async_channel::TrySendError;
use nes_network::protocol::{ConnectionIdentifier, ThisConnectionIdentifier, TupleBuffer};
use nes_network::sender::{ChannelControlMessage, ChannelControlQueue};
use nes_network::*;
use once_cell::sync;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::error::Error;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

#[cxx::bridge]
pub mod ffi {
    enum SendResult {
        Ok,
        Closed,
        Full,
    }

    struct SerializedTupleBuffer {
        sequence_number: usize,
        origin_id: usize,
        chunk_number: usize,
        number_of_tuples: usize,
        watermark: usize,
        last_chunk: bool,
    }

    unsafe extern "C++" {
        include!("NetworkBindings.hpp");
        type TupleBufferBuilder;
        fn set_metadata(self: Pin<&mut TupleBufferBuilder>, meta: &SerializedTupleBuffer);
        fn set_data(self: Pin<&mut TupleBufferBuilder>, data: &[u8]);
        fn add_child_buffer(self: Pin<&mut TupleBufferBuilder>, data: &[u8]);
    }

    extern "Rust" {
        type ReceiverNetworkService;
        type SenderNetworkService;
        type SenderDataChannel;
        type ReceiverDataChannel;

        fn init_receiver_service(connection_addr: String) -> Result<()>;
        fn receiver_instance(connection_addr: String) -> Result<Box<ReceiverNetworkService>>;
        fn init_sender_service(connection_addr: String) -> Result<()>;
        fn sender_instance(connection_addr: String) -> Result<Box<SenderNetworkService>>;

        fn register_receiver_channel(
            server: &mut ReceiverNetworkService,
            channel_identifier: String,
        ) -> Box<ReceiverDataChannel>;
        fn interrupt_receiver(receiver_channel: &ReceiverDataChannel) -> bool;
        fn receive_buffer(
            receiver_channel: &ReceiverDataChannel,
            builder: Pin<&mut TupleBufferBuilder>,
        ) -> bool;

        fn close_receiver_channel(channel: Box<ReceiverDataChannel>);

        fn register_sender_channel(
            server: &SenderNetworkService,
            connection_identifier: String,
            channel_identifier: String,
        ) -> Result<Box<SenderDataChannel>>;

        fn close_sender_channel(channel: Box<SenderDataChannel>);
        fn sender_writes_pending(channel: &SenderDataChannel) -> bool;

        fn flush_channel(channel: &SenderDataChannel);
        fn try_send_on_channel(
            channel: &SenderDataChannel,
            metadata: SerializedTupleBuffer,
            data: &[u8],
            children: &[&[u8]],
        ) -> SendResult;
    }
}

type ReceiverService = Arc<receiver::NetworkService<channel::MemCom>>;
type SenderService = Arc<sender::NetworkService<channel::MemCom>>;
#[derive(Default)]
struct Services {
    receiver: HashMap<ThisConnectionIdentifier, ReceiverService>,
    sender: HashMap<ThisConnectionIdentifier, SenderService>,
}

/// Lazy singleton types, initialized on first use in a thread-safe way
static SERVICES: sync::Lazy<Mutex<Services>> = sync::Lazy::new(|| Mutex::default());

pub struct ReceiverNetworkService {
    handle: Arc<receiver::NetworkService<channel::MemCom>>,
}

struct SenderNetworkService {
    handle: Arc<sender::NetworkService<channel::MemCom>>,
}

struct SenderDataChannel {
    chan: ChannelControlQueue,
}

struct ReceiverDataChannel {
    chan: Box<async_channel::Receiver<TupleBuffer>>,
}

fn init_sender_service(connection_addr: String) -> Result<(), String> {
    let this_connection =
        ThisConnectionIdentifier::from_str(connection_addr.as_str()).map_err(|e| e.to_string())?;
    let mut services = SERVICES.lock().unwrap();
    match services.sender.entry(this_connection.clone()) {
        Entry::Occupied(_) => Err("Sender service was already registered".into()),
        Entry::Vacant(e) => {
            e.insert({
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .thread_name("net-sender")
                    .worker_threads(2)
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap();
                sender::NetworkService::start(
                    runtime,
                    this_connection,
                    channel::MemCom::new(),
                )
            });
            Ok(())
        }
    }
}

fn init_receiver_service(connection_addr: String) -> Result<(), String> {
    let this_connection =
        ThisConnectionIdentifier::from_str(connection_addr.as_str()).map_err(|e| e.to_string())?;
    let mut services = SERVICES.lock().unwrap();
    match services.receiver.entry(this_connection.clone()) {
        Entry::Occupied(_) => Err("Receiver service was already registered".into()),
        Entry::Vacant(e) => {
            e.insert({
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .thread_name("net-receiver")
                    .worker_threads(2)
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap();
                receiver::NetworkService::start(
                    runtime,
                    this_connection,
                    channel::MemCom::new(),
                )
            });
            Ok(())
        }
    }
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
) -> Box<ReceiverDataChannel> {
    let queue = receiver_service
        .handle
        .register_channel(channel_identifier.clone())
        .unwrap();

    Box::new(ReceiverDataChannel {
        chan: Box::new(queue),
    })
}

fn interrupt_receiver(receiver_channel: &ReceiverDataChannel) -> bool {
    receiver_channel.chan.close()
}

fn receive_buffer(
    receiver_channel: &ReceiverDataChannel,
    mut builder: Pin<&mut ffi::TupleBufferBuilder>,
) -> bool {
    let Ok(buffer) = receiver_channel.chan.recv_blocking() else {
        return false;
    };

    builder.as_mut().set_metadata(&ffi::SerializedTupleBuffer {
        sequence_number: buffer.sequence_number as usize,
        origin_id: buffer.origin_id as usize,
        watermark: buffer.watermark as usize,
        chunk_number: buffer.chunk_number as usize,
        number_of_tuples: buffer.number_of_tuples as usize,
        last_chunk: buffer.last_chunk,
    });

    builder.as_mut().set_data(&buffer.data);

    for child_buffer in buffer.child_buffers.iter() {
        assert!(!child_buffer.is_empty());
        builder.as_mut().add_child_buffer(child_buffer);
    }

    true
}

// CXX requires the usage of Boxed types
#[allow(clippy::boxed_local)]
fn close_receiver_channel(channel: Box<ReceiverDataChannel>) {
    channel.chan.close();
}

fn register_sender_channel(
    sender_service: &SenderNetworkService,
    connection_addr: String,
    channel_id: String,
) -> Result<Box<SenderDataChannel>, String> {
    let connection_addr =
        ConnectionIdentifier::from_str(connection_addr.as_str()).map_err(|e| e.to_string())?;
    let data_queue = sender_service
        .handle
        .register_channel(ConnectionIdentifier::from(connection_addr), channel_id)
        .unwrap();
    Ok(Box::new(SenderDataChannel { chan: data_queue }))
}

fn try_send_on_channel(
    channel: &SenderDataChannel,
    metadata: ffi::SerializedTupleBuffer,
    data: &[u8],
    children: &[&[u8]],
) -> ffi::SendResult {
    let buffer = TupleBuffer {
        sequence_number: metadata.sequence_number as u64,
        origin_id: metadata.origin_id as u64,
        chunk_number: metadata.chunk_number as u64,
        number_of_tuples: data.len() as u64,
        watermark: metadata.watermark as u64,
        last_chunk: metadata.last_chunk,
        data: Vec::from(data),
        child_buffers: children.iter().map(|bytes| Vec::from(*bytes)).collect(),
    };
    match channel.chan.try_send(ChannelControlMessage::Data(buffer)) {
        Ok(()) => ffi::SendResult::Ok,
        Err(TrySendError::Full(_)) => ffi::SendResult::Full,
        Err(TrySendError::Closed(_)) => ffi::SendResult::Closed,
    }
}

fn sender_writes_pending(channel: &SenderDataChannel) -> bool {
    if channel.chan.is_closed() {
        return false;
    }
    !channel.chan.is_empty()
}

fn flush_channel(channel: &SenderDataChannel) {
    let (tx, _) = tokio::sync::oneshot::channel();
    let _ = channel.chan.send_blocking(ChannelControlMessage::Flush(tx));
}

// CXX requires the usage of Boxed types
#[allow(clippy::boxed_local)]
fn close_sender_channel(channel: Box<SenderDataChannel>) {
    let (tx, rx) = tokio::sync::oneshot::channel();

    if channel
        .chan
        .send_blocking(ChannelControlMessage::Flush(tx))
        .is_err()
    {
        // already terminated
        return;
    }

    let _ = rx.blocking_recv();
    let _ = channel.chan.send_blocking(ChannelControlMessage::Terminate);
}
