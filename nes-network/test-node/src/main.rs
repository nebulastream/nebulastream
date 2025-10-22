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

mod config;
mod engine;

use crate::config::{Command, SinkExpectation, SourceExpectation};
use crate::engine::{
    EmitFn, ExecutablePipeline, Node, PipelineContext, Query, QueryEngine, SourceImpl, SourceNode,
};
use bytes::{Buf, BufMut, BytesMut};
use clap::{Parser, Subcommand};
use log::error;
use nes_network::channel::{Communication, MemCom, TcpCommunication};
use nes_network::protocol::{ChannelIdentifier, ConnectionIdentifier, TupleBuffer};
use nes_network::receiver::ReceiverChannelResult;
use nes_network::sender::TrySendDataResult;
use nes_network::{receiver, sender};
use std::collections::{HashSet, VecDeque};
use std::io::Cursor;
use std::io::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{sync, thread};
use tracing::{info, trace, warn, Span};
use tracing_subscriber::EnvFilter;

// mod inter_node {
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error>;
#[derive(Parser)]
struct CLIArgs {
    file: String,
    /// Index of the node to run. If not provided, all nodes will run in the same process using MemCom.
    index: Option<usize>,
}

#[derive(Subcommand)]
enum Commands {}

struct PrintSink {
    sequence_tracker: Mutex<MissingSequenceTracker>,
    counter: AtomicUsize,
    expected_messages: Option<usize>,
    expected_messages_uncertainty: Option<usize>,
}

fn verify_tuple_buffer(received: &TupleBuffer) -> bool {
    let counter = (received.sequence_number - 1) as usize;

    // Reconstruct main buffer
    let mut expected_buffer = BytesMut::with_capacity(16);
    let value_bytes = counter.to_le_bytes();

    while expected_buffer.len() + size_of::<usize>() <= expected_buffer.capacity() {
        expected_buffer.put(&value_bytes[..]);
    }

    // Verify main buffer
    if received.data != expected_buffer.freeze() {
        return false;
    }

    // Reconstruct child buffers
    let mut expected_child_buffers = vec![];
    for idx in 0..(counter % 3) {
        let buffer = vec![0u8; ((idx + 1) * (10 * counter)) % 16];
        let mut cursor = Cursor::new(buffer);
        while cursor.has_remaining() {
            cursor.write_all(&counter.to_le_bytes()).unwrap();
        }
        expected_child_buffers.push(cursor.into_inner());
    }

    // Verify child buffers
    if received.child_buffers.len() != expected_child_buffers.len() {
        return false;
    }

    for (received_child, expected_child) in received
        .child_buffers
        .iter()
        .zip(expected_child_buffers.iter())
    {
        if received_child != expected_child {
            return false;
        }
    }

    true
}

/// A data structure that tracks seen positive integers and finds the lowest unseen number.
#[derive(Default)]
pub struct MissingSequenceTracker {
    seen: HashSet<u64>,
    highest_seen: u64,
    lowest_removed: u64,
}

impl MissingSequenceTracker {
    /// Creates a new empty tracker.
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
            highest_seen: 0,
            lowest_removed: 0,
        }
    }

    /// Adds a positive number to the tracker.
    /// Panics if the number is zero or already seen.
    pub fn add(&mut self, num: u64) {
        // Assert that the number is valid (positive)
        assert!(num > 0, "Only positive numbers are allowed");
        assert!(
            !self.seen.contains(&num),
            "Number {} has already been seen",
            num
        );

        if num > self.highest_seen {
            self.highest_seen = num;
        }

        self.seen.insert(num);

        if self.seen.len() > 1000 {
            let lowest = self.query();
            self.seen.retain(|&x| x > lowest);
            self.lowest_removed = lowest - 1;
        }
    }

    /// Returns the lowest positive integer that hasn't been seen yet.
    pub fn query(&self) -> u64 {
        if self.seen.len() as u64 == self.highest_seen - self.lowest_removed {
            return self.highest_seen + 1;
        }

        for i in 1..=self.highest_seen {
            if !self.seen.contains(&(i + self.lowest_removed)) {
                return i + self.lowest_removed;
            }
        }

        unreachable!()
    }
}

#[test]
fn testSequenceTrackerMissingOne() {
    let mut tracker = MissingSequenceTracker::default();
    for i in 1..1004 {
        if i == 100 || i == 193 || i == 210 {
            continue;
        }
        tracker.add(i);
    }

    assert_eq!(tracker.query(), 100);
    tracker.add(100);
    tracker.add(1004);
    assert_eq!(tracker.query(), 193);
    tracker.add(193);
    assert_eq!(tracker.query(), 210);
    tracker.add(1005);
}
#[test]
fn testSequenceTrackerMissing() {
    let tracker = MissingSequenceTracker::default();

    assert_eq!(tracker.query(), 1);
}
#[test]
fn testSequenceTrackerOOMissing() {
    let mut tracker = MissingSequenceTracker::default();
    tracker.add(1);
    tracker.add(4);
    tracker.add(2);

    assert_eq!(tracker.query(), 3);
}
#[test]
fn testSequenceTrackerOO() {
    let mut tracker = MissingSequenceTracker::default();
    tracker.add(1);
    tracker.add(3);
    tracker.add(2);

    assert_eq!(tracker.query(), 4);
}
#[test]
fn testSequenceTracker() {
    let mut tracker = MissingSequenceTracker::default();
    tracker.add(1);
    tracker.add(2);
    tracker.add(3);

    assert_eq!(tracker.query(), 4);
}

impl ExecutablePipeline for PrintSink {
    fn execute(&self, data: &TupleBuffer, _context: &mut dyn PipelineContext) {
        self.counter.fetch_add(1, Ordering::Relaxed);
        if !verify_tuple_buffer(data) {
            error!("Invalid data received");
        }
        self.sequence_tracker
            .lock()
            .unwrap()
            .add(data.sequence_number);
    }

    fn stop(&self) {
        info!(
            "Sink stopped. Last Sequence: {}",
            self.sequence_tracker.lock().unwrap().query() - 1
        );
        let counter = self.counter.load(Ordering::Relaxed);
        let receive_check = if let Some(expected_messages) = self.expected_messages {
            let uncertainty = self.expected_messages_uncertainty.unwrap_or(0);
            counter >= expected_messages - uncertainty && counter <= expected_messages + uncertainty
        } else {
            true
        };
        if !receive_check {
            error!(
                "Received {} messages, but expected {} messages (uncertainty: {})",
                counter,
                self.expected_messages.unwrap(),
                self.expected_messages_uncertainty.unwrap_or(0)
            );
        }
    }
}

struct NetworkSink<L: Communication> {
    service: Arc<sender::NetworkService<L>>,
    connection: ConnectionIdentifier,
    channel: ChannelIdentifier,
    queue: sync::RwLock<Option<sender::SenderChannel>>,
    buffer: std::sync::RwLock<VecDeque<TupleBuffer>>,
    should_close: bool,
    closed: AtomicBool,
    counter: AtomicUsize,
    expectation: SinkExpectation,
}

impl<L: Communication> NetworkSink<L> {
    pub fn new(
        service: Arc<sender::NetworkService<L>>,
        connection: ConnectionIdentifier,
        channel: ChannelIdentifier,
        should_close: bool,
        expectation: SinkExpectation,
    ) -> Self {
        Self {
            service,
            connection,
            channel,
            should_close,
            closed: AtomicBool::new(false),
            queue: std::sync::RwLock::new(None),
            buffer: std::sync::RwLock::new(VecDeque::new()),
            counter: AtomicUsize::new(0),
            expectation,
        }
    }
}

impl<L: Communication + 'static> ExecutablePipeline for NetworkSink<L> {
    fn execute(&self, data: &TupleBuffer, _context: &mut dyn PipelineContext) {
        if self.closed.load(Ordering::Acquire) {
            trace!("Dropping message because sink is closed");
            return;
        }

        self.counter.fetch_add(1, Ordering::Relaxed);

        if self.queue.read().unwrap().is_none() {
            let mut write_locked = self.queue.write().unwrap();
            if write_locked.is_none() {
                info!("Network Sink Setup");
                write_locked.replace(
                    self.service
                        .register_channel(self.connection.clone(), self.channel.clone())
                        .unwrap(),
                );
                info!("Network Sink Setup Done");
            }
        }

        if !self.buffer.read().unwrap().is_empty() {
            let mut locked = self.buffer.write().unwrap();
            if !locked.is_empty() {
                locked.push_back(data.clone());
                while !locked.is_empty() {
                    let front = locked.pop_front().unwrap();
                    match self
                        .queue
                        .read()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .try_send_data(front)
                    {
                        TrySendDataResult::Ok => {}
                        TrySendDataResult::Full(buffer) => {
                            locked.push_front(buffer);
                            break;
                        }
                        TrySendDataResult::Closed(_) => {
                            panic!("Sink was closed unexpectedly");
                        }
                    }
                }
            }
        }

        match self
            .queue
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .try_send_data(data.clone())
        {
            TrySendDataResult::Ok => {
                let _ = self.queue.read().unwrap().as_ref().unwrap().flush();
            }
            TrySendDataResult::Full(buffer) => {
                self.buffer.write().unwrap().push_front(buffer);
            }
            TrySendDataResult::Closed(_) => {
                self.closed.store(true, Ordering::Release);
            }
        }
    }

    fn stop(&self) {
        info!("Network Sink stopped. Sent {} messages", self.counter.load(Ordering::Relaxed));

        // Check message count expectations
        let counter = self.counter.load(Ordering::Relaxed);
        let receive_check = if let Some(expected_messages) = self.expectation.expected_messages {
            let uncertainty = self.expectation.expected_messages_uncertainty.unwrap_or(0);
            counter >= expected_messages - uncertainty && counter <= expected_messages + uncertainty
        } else {
            true
        };
        if !receive_check {
            error!(
                "Sent {} messages, but expected {} messages (uncertainty: {})",
                counter,
                self.expectation.expected_messages.unwrap(),
                self.expectation.expected_messages_uncertainty.unwrap_or(0)
            );
        }

        // Check if channel was closed from other side
        let was_closed = self.closed.load(Ordering::Acquire);
        if let Some(expect_close) = self.expectation.expect_close_from_other_side {
            if expect_close && !was_closed {
                error!("Expected channel to be closed from other side, but it wasn't");
            } else if !expect_close && was_closed {
                error!("Channel was closed from other side, but expected it to stay open");
            } else if expect_close && was_closed {
                info!("Channel was closed from other side as expected");
            }
        }

        info!("Closing Network Sink");
        let queue = self.queue.write().unwrap().take().unwrap();
        loop {
            let flush_result = queue.flush();
            let Ok(flush_result) = flush_result else {
                warn!("Network sink was already closed");
                return;
            };
            if flush_result {
                break;
            }
        }

        if self.should_close {
            if !queue.close() {
                warn!("Network sink was already closed");
                return;
            }
        }
    }
}

struct NetworkSource<L: Communication> {
    channel: ChannelIdentifier,
    service: Arc<receiver::NetworkService<L>>,
    ingestion_rate: Option<Duration>,
    thread: std::sync::Mutex<Option<Thread<()>>>,
    #[allow(dead_code)]
    close_source: bool,
    counter: Arc<AtomicUsize>,
    expectation: SourceExpectation,
    closed_externally: Arc<AtomicBool>,
}

impl<L: Communication> NetworkSource<L> {
    pub fn new(
        channel: ChannelIdentifier,
        ingestion_rate: Option<Duration>,
        service: Arc<receiver::NetworkService<L>>,
        close_source: bool,
        expectation: SourceExpectation,
    ) -> Self {
        Self {
            channel,
            service,
            ingestion_rate,
            thread: sync::Mutex::default(),
            close_source,
            counter: Arc::new(AtomicUsize::new(0)),
            expectation,
            closed_externally: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<L: Communication + 'static> engine::SourceImpl for NetworkSource<L> {
    fn start(&self, emit: engine::EmitFn) {
        let ingestion_rate = self.ingestion_rate.unwrap_or(Duration::from_millis(0));
        let queue = self.service.register_channel(self.channel.clone()).unwrap();
        let counter = self.counter.clone();
        let closed_externally = self.closed_externally.clone();

        let span = Span::current();
        self.thread
            .lock()
            .unwrap()
            .replace(Thread::spawn(move |stopped: &AtomicBool| {
                let _enter = span.enter();
                while !stopped.load(Ordering::Relaxed) {
                    match queue.receive() {
                        ReceiverChannelResult::Ok(d) => {
                            counter.fetch_add(1, Ordering::Relaxed);
                            emit(d);
                        }
                        ReceiverChannelResult::Closed => {
                            closed_externally.store(true, Ordering::Release);
                            info!("External source stop - channel closed from other side");
                            return;
                        }
                        ReceiverChannelResult::Error(e) => {
                            error!("Error while receiving: {:?}", e);
                        }
                    };
                    thread::sleep(ingestion_rate);
                }
                info!("Internal source stop");
                queue.close();
            }));
    }

    fn stop(&self) {
        info!("Network Source stopped. Received {} messages", self.counter.load(Ordering::Relaxed));

        // Check message count expectations
        let counter = self.counter.load(Ordering::Relaxed);
        let receive_check = if let Some(expected_messages) = self.expectation.expected_messages {
            let uncertainty = self.expectation.expected_messages_uncertainty.unwrap_or(0);
            counter >= expected_messages - uncertainty && counter <= expected_messages + uncertainty
        } else {
            true
        };
        if !receive_check {
            error!(
                "Received {} messages, but expected {} messages (uncertainty: {})",
                counter,
                self.expectation.expected_messages.unwrap(),
                self.expectation.expected_messages_uncertainty.unwrap_or(0)
            );
        }

        // Check if channel was closed from other side
        let was_closed_externally = self.closed_externally.load(Ordering::Acquire);
        if let Some(expect_close) = self.expectation.expect_close_from_other_side {
            if expect_close && !was_closed_externally {
                error!("Expected channel to be closed from other side, but it wasn't");
            } else if !expect_close && was_closed_externally {
                error!("Channel was closed from other side, but expected it to stay open");
            } else if expect_close && was_closed_externally {
                info!("Channel was closed from other side as expected");
            }
        }

        // Stop the thread
        let _ = self.thread.lock().unwrap().take();
    }
}

struct Thread<T> {
    stopped: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<T>>,
}

impl<T> Drop for Thread<T> {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        if !self
            .handle
            .as_ref()
            .expect("BUG: Dropped multiple times")
            .is_finished()
        {
            self.handle
                .take()
                .expect("BUG: Dropped multiple times")
                .join()
                .unwrap();
        }
    }
}

impl<T> Thread<T> {
    pub fn spawn<F>(function: F) -> Self
    where
        F: 'static + Send + FnOnce(&AtomicBool) -> T,
        T: 'static + Send,
    {
        let stopped: Arc<AtomicBool> = Arc::default();
        let handle = thread::spawn({
            let token = stopped.clone();
            move || function(token.as_ref())
        });

        Thread {
            stopped,
            handle: Some(handle),
        }
    }
}

struct GeneratorSource {
    thread: sync::RwLock<Option<Thread<()>>>,
    interval: Duration,
}

impl SourceImpl for GeneratorSource {
    fn start(&self, emit: EmitFn) {
        self.thread.write().unwrap().replace(Thread::spawn({
            let interval = self.interval;
            move |stopped| {
                let mut counter = 0_usize;
                while !stopped.load(Ordering::Relaxed) {
                    let buffer = vec![0u8; 16];
                    let mut cursor = Cursor::new(buffer);
                    while cursor.has_remaining() {
                        cursor.write_all(&counter.to_le_bytes()).unwrap();
                    }

                    let mut child_buffers = vec![];
                    for idx in 0..(counter % 3) {
                        let buffer = vec![0u8; ((idx + 1) * (10 * counter)) % 16];
                        let mut cursor = Cursor::new(buffer);
                        while cursor.has_remaining() {
                            cursor.write_all(&counter.to_le_bytes()).unwrap();
                        }
                        child_buffers.push(cursor.into_inner());
                    }

                    counter += 1;
                    emit(TupleBuffer {
                        sequence_number: counter as u64,
                        origin_id: 1,
                        watermark: 2,
                        chunk_number: 1,
                        number_of_tuples: 1,
                        last_chunk: true,
                        data: cursor.into_inner(),
                        child_buffers,
                    });
                    thread::sleep(interval);
                }
                info!("Source stopped. Last Sequence: {counter}");
            }
        }));
    }

    fn stop(&self) {
        let _ = self.thread.write().unwrap().take();
    }
}

impl GeneratorSource {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            thread: sync::RwLock::new(None),
        }
    }
}

fn generator<L: Communication + 'static>(
    downstream_connection: ConnectionIdentifier,
    downstream_channel: ChannelIdentifier,
    ingestion_rate_in_milliseconds: Option<u64>,
    sender: Arc<sender::NetworkService<L>>,
    engine: Arc<QueryEngine>,
    close_sink: bool,
    sink_expectation: SinkExpectation,
) -> usize {
    let query = engine.start_query(Query::new(vec![SourceNode::new(
        Node::new(
            None,
            Arc::new(NetworkSink::new(
                sender.clone(),
                downstream_connection,
                downstream_channel,
                close_sink,
                sink_expectation,
            )),
        ),
        Box::new(GeneratorSource::new(Duration::from_millis(
            ingestion_rate_in_milliseconds.unwrap_or(250),
        ))),
    )]));

    query
}

fn sink<L: Communication + 'static>(
    channel: ChannelIdentifier,
    ingestion_rate_in_milliseconds: Option<u64>,
    engine: Arc<QueryEngine>,
    receiver: Arc<receiver::NetworkService<L>>,
    close_source: bool,
    source_expectation: SourceExpectation,
) -> usize {
    engine.start_query(Query::new(vec![SourceNode::new(
        Node::new(
            None,
            Arc::new(PrintSink {
                sequence_tracker: Default::default(),
                counter: Default::default(),
                expected_messages: source_expectation.expected_messages,
                expected_messages_uncertainty: source_expectation.expected_messages_uncertainty,
            }),
        ),
        Box::new(NetworkSource::new(
            channel,
            ingestion_rate_in_milliseconds.map(|millis| Duration::from_millis(millis)),
            receiver.clone(),
            close_source,
            source_expectation,
        )),
    )]))
}

fn bridge<L: Communication + 'static>(
    input_channel: ChannelIdentifier,
    downstream_channel: ChannelIdentifier,
    downstream_connection: ConnectionIdentifier,
    ingestion_rate_in_milliseconds: Option<u64>,
    engine: Arc<QueryEngine>,
    receiver: Arc<receiver::NetworkService<L>>,
    sender: Arc<sender::NetworkService<L>>,
    close_source: bool,
    close_sink: bool,
    sink_expectation: SinkExpectation,
    source_expectation: SourceExpectation,
) -> usize {
    let query = engine.start_query(Query::new(vec![SourceNode::new(
        Node::new(
            None,
            Arc::new(NetworkSink::new(
                sender.clone(),
                downstream_connection,
                downstream_channel,
                close_sink,
                sink_expectation,
            )),
        ),
        Box::new(NetworkSource::new(
            input_channel,
            ingestion_rate_in_milliseconds.map(|millis| Duration::from_millis(millis)),
            receiver.clone(),
            close_source,
            source_expectation,
        )),
    )]));
    query
}

fn run_node<L: Communication + 'static>(
    config: config::Node,
    mut engine: Arc<QueryEngine>,
    mut sender: Arc<sender::NetworkService<L>>,
    mut receiver: Arc<receiver::NetworkService<L>>,
    communication: L,
) {
    let connection = config.connection.clone();

    for command in config.commands.into_iter() {
        match command {
            Command::StartQuery { q } => {
                match q {
                    config::Query::Source {
                        downstream_channel,
                        downstream_connection,
                        ingestion_rate_in_milliseconds,
                        sink_expectation,
                        close_sink,
                    } => generator(
                        downstream_connection,
                        downstream_channel,
                        ingestion_rate_in_milliseconds,
                        sender.clone(),
                        engine.clone(),
                        close_sink,
                        sink_expectation,
                    ),
                    config::Query::Bridge {
                        input_channel,
                        downstream_channel,
                        downstream_connection,
                        ingestion_rate_in_milliseconds,
                        close_source,
                        close_sink,
                        sink_expectation,
                        source_expectation,
                    } => bridge(
                        input_channel,
                        downstream_channel,
                        downstream_connection,
                        ingestion_rate_in_milliseconds,
                        engine.clone(),
                        receiver.clone(),
                        sender.clone(),
                        close_source,
                        close_sink,
                        sink_expectation,
                        source_expectation,
                    ),
                    config::Query::Sink {
                        input_channel,
                        ingestion_rate_in_milliseconds,
                        close_source,
                        source_expectation,
                    } => sink(
                        input_channel,
                        ingestion_rate_in_milliseconds,
                        engine.clone(),
                        receiver.clone(),
                        close_source,
                        source_expectation,
                    ),
                };
            }
            Command::StopQuery { id } => {
                engine.stop_query(id);
            }
            Command::Wait { millis } => {
                thread::sleep(Duration::from_millis(millis as u64));
            }
            Command::Reset => {
                info!("Resetting worker: stopping all queries and shutting down network services");

                // Stop all queries
                // engine.stop_all_queries();

                // Shutdown network services - we need to take ownership to call shutdown
                // Since shutdown consumes self, we clone the Arc, then let the old ones drop
                let sender_clone = sender.clone();
                let receiver_clone = receiver.clone();

                // Drop our references first so we can call shutdown (which requires unique ownership via Arc::try_unwrap or consuming self)
                drop(sender);
                drop(receiver);

                if let Err(e) = sender_clone.shutdown() {
                    error!("Failed to shutdown sender: {:?}", e);
                }
                if let Err(e) = receiver_clone.shutdown() {
                    error!("Failed to shutdown receiver: {:?}", e);
                }

                info!("Restarting worker: creating new network services and engine");

                // Create new engine with the same worker_id
                let worker_id = connection.to_string();
                engine = engine::QueryEngine::start(worker_id);

                // Create new network services
                let rt_sender = tokio::runtime::Builder::new_multi_thread()
                    .thread_name("sender")
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap();
                sender = sender::NetworkService::start(rt_sender, connection.clone(), communication.clone());

                let rt_receiver = tokio::runtime::Builder::new_multi_thread()
                    .thread_name("receiver")
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap();
                receiver = receiver::NetworkService::start(rt_receiver, connection.clone(), communication.clone());

                info!("Worker reset complete");
            }
        };
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = CLIArgs::parse();

    match args.index {
        // TCP mode: run single node at specified index
        Some(index) => {
            info!("Running in TCP mode for node {}", index);
            let config = config::load_config(std::path::Path::new(&args.file), index);
            let tcp_com = TcpCommunication::new();

            let worker_id = config.connection.to_string();
            let engine = engine::QueryEngine::start(worker_id);
            let connection = config.connection.clone();
            let rt = tokio::runtime::Builder::new_multi_thread()
                .thread_name("sender")
                .enable_io()
                .enable_time()
                .build()
                .unwrap();
            let sender = sender::NetworkService::start(rt, connection.clone(), tcp_com.clone());

            let rt = tokio::runtime::Builder::new_multi_thread()
                .thread_name("receiver")
                .enable_io()
                .enable_time()
                .build()
                .unwrap();
            let receiver = receiver::NetworkService::start(rt, connection, tcp_com.clone());

            run_node(config, engine, sender, receiver, tcp_com);
        }
        // MemCom mode: run all nodes in the same process
        None => {
            info!("Running in MemCom mode (all nodes in same process)");
            let configs = config::load_all_configs(std::path::Path::new(&args.file));
            let memcom = MemCom::new();

            // Create network services and engines for each node
            let mut worker_data = Vec::new();
            for (idx, config) in configs.iter().enumerate() {
                // Each worker gets its own QueryEngine with local query ID counter
                let worker_id = config.connection.to_string();
                let engine = engine::QueryEngine::start(worker_id);

                let rt_sender = tokio::runtime::Builder::new_multi_thread()
                    .thread_name(format!("sender-{}", idx))
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap();
                let sender = sender::NetworkService::start(
                    rt_sender,
                    config.connection.clone(),
                    memcom.clone(),
                );

                let rt_receiver = tokio::runtime::Builder::new_multi_thread()
                    .thread_name(format!("receiver-{}", idx))
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap();
                let receiver = receiver::NetworkService::start(
                    rt_receiver,
                    config.connection.clone(),
                    memcom.clone(),
                );

                worker_data.push((engine, sender, receiver));
            }

            // Run all nodes in parallel using threads
            let handles: Vec<_> = configs
                .into_iter()
                .zip(worker_data.into_iter())
                .map(|(config, (engine, sender, receiver))| {
                    let memcom_clone = memcom.clone();
                    thread::spawn(move || {
                        run_node(config, engine, sender, receiver, memcom_clone);
                    })
                })
                .collect();

            // Wait for all threads to complete
            for handle in handles {
                handle.join().unwrap();
            }
        }
    }
}
