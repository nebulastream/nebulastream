use async_channel::TrySendError;
use nes_network::protocol::{
    ConnectionIdentifier, ThisConnectionIdentifier, TupleBuffer,
};
use nes_network::sender::{ChannelControlMessage, ChannelControlQueue};
use nes_network::*;
use once_cell::sync;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;

#[cxx::bridge]
pub mod ffi {
    enum SendResult {
        Ok,
        Error,
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

        fn init_receiver_service(bind_addr: String, connection_addr: String) -> Result<()>;
        fn receiver_instance() -> Result<Box<ReceiverNetworkService>>;
        fn init_sender_service(connection_addr: String) -> Result<()>;
        fn sender_instance() -> Result<Box<SenderNetworkService>>;

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
        ) -> Box<SenderDataChannel>;

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

/// Lazy singleton types, initialized on first use in a thread-safe way
static RECEIVER: sync::OnceCell<Arc<receiver::NetworkService>> = sync::OnceCell::new();
static SENDER: sync::OnceCell<Arc<sender::NetworkService>> = sync::OnceCell::new();

pub struct ReceiverNetworkService {
    handle: Arc<receiver::NetworkService>,
}

struct SenderNetworkService {
    handle: Arc<sender::NetworkService>,
}

struct SenderDataChannel {
    chan: ChannelControlQueue,
}

struct ReceiverDataChannel {
    chan: Box<async_channel::Receiver<TupleBuffer>>,
}

fn init_sender_service(connection_addr: String) -> Result<(), String> {
    SENDER.get_or_init(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("net-receiver")
            .worker_threads(2)
            .enable_io()
            .enable_time()
            .worker_threads(2)
            .build()
            .unwrap();
        sender::NetworkService::start(runtime, ThisConnectionIdentifier::from(connection_addr))
    });
    Ok(())
}

fn init_receiver_service(bind_addr: String, connection_addr: String) -> Result<(), String> {
    let bind = bind_addr
        .parse()
        .map_err(|e| format!("Bind address `{bind_addr}` is invalid: {e:?}"))?;

    RECEIVER.get_or_init(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("net-sender")
            .worker_threads(2)
            .enable_io()
            .worker_threads(2)
            .enable_time()
            .build()
            .unwrap();
        receiver::NetworkService::start(
            runtime,
            bind,
            ThisConnectionIdentifier::from(connection_addr),
        )
    });
    Ok(())
}

fn receiver_instance() -> Result<Box<ReceiverNetworkService>, Box<dyn Error>> {
    Ok(Box::new(ReceiverNetworkService {
        handle: RECEIVER
            .get()
            .ok_or("Receiver server has not been initialized yet.")?
            .clone(),
    }))
}

fn sender_instance() -> Result<Box<SenderNetworkService>, Box<dyn Error>> {
    Ok(Box::new(SenderNetworkService {
        handle: SENDER
            .get()
            .ok_or("Sender server has not been initialized yet.")?
            .clone(),
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
) -> Box<SenderDataChannel> {
    let data_queue = sender_service
        .handle
        .register_channel(
            ConnectionIdentifier::from(connection_addr),
            channel_id,
        )
        .unwrap();
    Box::new(SenderDataChannel { chan: data_queue })
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
    match channel
        .chan
        .try_send(ChannelControlMessage::Data(buffer))
    {
        Ok(()) => ffi::SendResult::Ok,
        Err(TrySendError::Full(_)) => ffi::SendResult::Full,
        Err(TrySendError::Closed(_)) => ffi::SendResult::Error,
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
    let _ = channel
        .chan
        .send_blocking(ChannelControlMessage::Flush(tx));
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
    let _ = channel
        .chan
        .send_blocking(ChannelControlMessage::Terminate);
}
