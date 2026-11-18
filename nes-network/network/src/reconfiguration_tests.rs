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

//! End-to-end tests of the reconfiguration primitives of ISQD (markers, detach and redirect) over the in-memory transport.
#![allow(clippy::unwrap_used)]

use crate::channel::MemCom;
use crate::protocol::{ConnectionIdentifier, ThisConnectionIdentifier, TupleBuffer};
use crate::receiver::{self, ReceiverChannel, ReceiverChannelResult};
use crate::sender::{self, SenderChannel, SenderConfig, TrySendDataResult};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// Every test uses its own ports, because the in-memory transport has a process-wide registry.
fn receiver_service(port: u16) -> Arc<receiver::NetworkService<MemCom>> {
    receiver::NetworkService::start(
        runtime(),
        ThisConnectionIdentifier::from_str(&format!("receiver:{port}")).unwrap(),
        MemCom::new(),
    )
}

fn sender_service(port: u16) -> Arc<sender::NetworkService<MemCom>> {
    sender::NetworkService::start(
        runtime(),
        ThisConnectionIdentifier::from_str(&format!("sender:{port}")).unwrap(),
        MemCom::new(),
    )
}

fn receiver_address(port: u16) -> ConnectionIdentifier {
    ConnectionIdentifier::from_str(&format!("receiver:{port}")).unwrap()
}

fn buffer(sequence_number: u64) -> TupleBuffer {
    TupleBuffer {
        sequence_number,
        origin_id: 1,
        watermark: sequence_number,
        chunk_number: 1,
        number_of_tuples: 1,
        last_chunk: true,
        data: sequence_number.to_le_bytes().to_vec(),
        child_buffers: vec![],
    }
}

#[derive(Debug, PartialEq)]
enum Received {
    Buffer(u64),
    Marker(Vec<u8>),
    Closed,
}

/// Receives on a separate thread, so a test fails instead of hanging if nothing arrives.
fn receive(channel: &Arc<ReceiverChannel>) -> Received {
    let (tx, rx) = mpsc::channel();
    let channel = channel.clone();
    std::thread::spawn(move || {
        let result = match channel.receive() {
            ReceiverChannelResult::Ok(buffer) => Received::Buffer(buffer.sequence_number),
            ReceiverChannelResult::Marker(marker) => Received::Marker(marker),
            ReceiverChannelResult::Closed => Received::Closed,
            ReceiverChannelResult::Error(e) => panic!("receive failed: {e}"),
        };
        let _ = tx.send(result);
    });
    rx.recv_timeout(TIMEOUT).expect("receive timed out")
}

fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(start.elapsed() < TIMEOUT, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn send(channel: &SenderChannel, sequence_number: u64) {
    assert!(matches!(
        channel.try_send_data(buffer(sequence_number)),
        TrySendDataResult::Ok
    ));
}

fn flush(channel: &SenderChannel) {
    wait_until("the channel is flushed", || channel.flush().unwrap());
}

fn register_sender(
    service: &Arc<sender::NetworkService<MemCom>>,
    receiver_port: u16,
    channel: &str,
) -> SenderChannel {
    service
        .register_channel(
            receiver_address(receiver_port),
            channel.to_string(),
            SenderConfig::default(),
        )
        .unwrap()
}

#[test]
fn marker_is_delivered_after_the_data_and_ends_the_stream() {
    let receiver = receiver_service(1001);
    let sender = sender_service(1001);
    let receiver_channel = Arc::new(receiver.register_channel("c".to_string(), 10).unwrap());
    let sender_channel = register_sender(&sender, 1001, "c");

    for i in 1..=3 {
        send(&sender_channel, i);
    }
    flush(&sender_channel);
    sender_channel.close_with_marker(vec![4, 2]);

    for i in 1..=3 {
        assert_eq!(receive(&receiver_channel), Received::Buffer(i));
    }
    assert_eq!(receive(&receiver_channel), Received::Marker(vec![4, 2]));
    assert_eq!(receive(&receiver_channel), Received::Closed);
}

#[test]
fn sender_detach_keeps_the_receiver_channel_open_for_a_new_sender() {
    let receiver = receiver_service(1002);
    let first_sender = sender_service(1002);
    let second_sender = sender_service(1003);
    let receiver_channel = Arc::new(receiver.register_channel("c".to_string(), 10).unwrap());

    let first = register_sender(&first_sender, 1002, "c");
    send(&first, 1);
    send(&first, 2);
    flush(&first);
    first.detach();

    assert_eq!(receive(&receiver_channel), Received::Buffer(1));
    assert_eq!(receive(&receiver_channel), Received::Buffer(2));

    let second = register_sender(&second_sender, 1002, "c");
    send(&second, 3);
    send(&second, 4);
    flush(&second);
    second.close();

    assert_eq!(receive(&receiver_channel), Received::Buffer(3));
    assert_eq!(receive(&receiver_channel), Received::Buffer(4));
    assert_eq!(receive(&receiver_channel), Received::Closed);
}

#[test]
fn receiver_detach_parks_the_sender_until_it_is_redirected() {
    const NUMBER_OF_BUFFERS: u64 = 200;
    let old_receiver = receiver_service(1004);
    let new_receiver = receiver_service(1005);
    let sender = sender_service(1004);

    // A small data queue makes the old receiver apply backpressure, so many buffers are still in flight or
    // queued at the sender when it detaches.
    let old_channel = Arc::new(old_receiver.register_channel("old".to_string(), 2).unwrap());
    let sender_channel = register_sender(&sender, 1004, "old");
    for i in 1..=NUMBER_OF_BUFFERS {
        send(&sender_channel, i);
    }

    let mut received = vec![];
    for _ in 0..10 {
        let Received::Buffer(i) = receive(&old_channel) else {
            panic!("expected data");
        };
        received.push(i);
    }
    old_channel.detach();
    // Buffers which already reached the data queue are still handed out, then the stream ends.
    loop {
        match receive(&old_channel) {
            Received::Buffer(i) => received.push(i),
            Received::Closed => break,
            Received::Marker(_) => panic!("unexpected marker"),
        }
    }
    assert!((received.len() as u64) < NUMBER_OF_BUFFERS);

    wait_until("the sender notices the detach", || {
        sender_channel.is_detached()
    });
    let kept = buffer(NUMBER_OF_BUFFERS + 1);
    assert!(matches!(
        sender_channel.try_send_data(kept),
        TrySendDataResult::Detached(_)
    ));
    assert!(sender_channel.flush().is_ok_and(|flushed| !flushed));

    let new_channel = Arc::new(
        new_receiver
            .register_channel("new".to_string(), 10)
            .unwrap(),
    );
    sender_channel
        .redirect(receiver_address(1005), "new".to_string())
        .unwrap();
    assert!(!sender_channel.is_detached());
    // The caller sends the buffer it kept after the redirect.
    send(&sender_channel, NUMBER_OF_BUFFERS + 1);

    while (received.len() as u64) < NUMBER_OF_BUFFERS + 1 {
        let Received::Buffer(i) = receive(&new_channel) else {
            panic!("expected data");
        };
        received.push(i);
    }
    // Nothing is lost or duplicated, and the order is kept across both receivers.
    assert_eq!(received, (1..=NUMBER_OF_BUFFERS + 1).collect::<Vec<_>>());

    flush(&sender_channel);
    sender_channel.close();
    assert_eq!(receive(&new_channel), Received::Closed);
}

#[test]
fn redirect_fails_if_the_channel_is_not_detached() {
    let receiver = receiver_service(1006);
    let sender = sender_service(1006);
    let _receiver_channel = receiver.register_channel("c".to_string(), 10).unwrap();
    let sender_channel = register_sender(&sender, 1006, "c");
    assert!(
        sender_channel
            .redirect(receiver_address(1006), "other".to_string())
            .is_err()
    );
}

#[test]
fn receiver_detach_before_a_sender_attached_ends_the_stream() {
    let receiver = receiver_service(1007);
    let channel = Arc::new(receiver.register_channel("c".to_string(), 10).unwrap());
    channel.detach();
    assert_eq!(receive(&channel), Received::Closed);
}
