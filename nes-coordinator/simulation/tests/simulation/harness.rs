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

//! Simulation harness: starts the coordinator and worker nodes inside madsim and gives workloads one API to operate them.
//!
//! The coordinator and each worker is a separate madsim node with its own IP.
//! For example, the coordinator is 192.168.1.1 and the first two workers are 192.168.1.2 and 192.168.1.3.
//! Statements and fault primitives are harness methods.
//! The simulator injects the faults.

use crate::worker::{HealthServer, HealthService, WorkerRpcServiceServer, WorkerService};
use anyhow::Result;
use controller::remote::WorkerRpcServiceClient;
use controller::remote::worker_rpc_service::{WorkerStatusRequest, WorkerStatusResponse};
use futures::future::join_all;
use madsim::net::NetSim;
use madsim::runtime::{Handle, NodeHandle};
use madsim::task::NodeId;
use model::database::{Database, StateBackend};
use model::identifier::QueryFragmentId;
use model::request::{Request, Statement};
use model::statement::{ParsedStatement, StatementResult};
use model::worker::endpoint::{DEFAULT_HOST_PORT, NetworkAddr};
use model::worker::{CreateWorker, DropWorker};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::Range;
use std::time::Duration;
use tonic::transport::Server;
use tracing::debug;

const COORDINATOR_NAME: &str = "coordinator";
const SUBNET: [u8; 3] = [192, 168, 1];
const COORDINATOR_OCTET: u8 = 1;
pub const FIRST_WORKER_OCTET: u8 = 2;

pub const fn build_ip(octet: u8) -> Ipv4Addr {
    let [a, b, c] = SUBNET;
    Ipv4Addr::new(a, b, c, octet)
}

/// The simulation only sends statements that the coordinator replies to immediately,
/// so a reply that takes this long in simulated time means that the request handler is stuck.
const REPLY_TIMEOUT: Duration = Duration::from_secs(30);

/// The latency range of the simulated network outside of a degradation window.
/// The lower bound is above zero, so no trial relies on instant delivery.
const DEFAULT_SEND_LATENCY: Range<Duration> = Duration::from_millis(1)..Duration::from_millis(100);

fn configure_network(send_latency: Range<Duration>, packet_loss_rate: f64) {
    NetSim::current().update_config(|cfg| {
        cfg.send_latency = send_latency;
        cfg.packet_loss_rate = packet_loss_rate;
    });
}

struct Coordinator {
    sender: async_channel::Sender<Request>,
    handle: NodeHandle,
}

/// Starts the coordinator of one trial and the nodes of the workers that it creates, and injects faults into them.
/// It must be created and used inside a running madsim runtime.
pub struct Harness {
    coordinator: Coordinator,
    workers: RefCell<HashMap<NetworkAddr, NodeId>>,
}

impl Harness {
    pub fn start() -> Self {
        configure_network(DEFAULT_SEND_LATENCY, 0.0);

        Self {
            coordinator: Self::start_coordinator(),
            workers: RefCell::default(),
        }
    }

    /// A worker create starts the worker's node before the statement is sent.
    /// A worker drop stops the node after the reply, also when the drop failed.
    pub async fn send(&self, statement: ParsedStatement) -> Result<StatementResult> {
        match statement {
            ParsedStatement::CreateWorker(create) => self.create_worker(create).await,
            ParsedStatement::DropWorker(drop) => self.drop_worker(drop).await,
            other => self.send_to_coordinator(other).await,
        }
    }

    async fn send_to_coordinator(&self, statement: ParsedStatement) -> Result<StatementResult> {
        let (rx, req) = Request::new(Statement::Parsed(statement));
        self.coordinator.sender.send(req).await?;
        tokio::time::timeout(REPLY_TIMEOUT, rx)
            .await
            .unwrap_or_else(|_| panic!("the coordinator did not reply within {REPLY_TIMEOUT:?}"))
            .expect("the coordinator dropped the request without a reply")
    }

    fn start_coordinator() -> Coordinator {
        let (sender, receiver) = async_channel::bounded(16);

        let handle = Handle::current()
            .create_node()
            .name(COORDINATOR_NAME)
            .ip(build_ip(COORDINATOR_OCTET).into())
            .init(move || {
                let receiver = receiver.clone();
                async move {
                    // A file catalog opens several connections, and the simulation runs them all on one thread,
                    // so a connection that waits for the write lock would block the one that holds it.
                    let db = Database::open(StateBackend::Memory)
                        .await
                        .expect("failed to open the database");
                    coordinator::run(db, None, None, receiver).await;
                }
            })
            .build();

        Coordinator { sender, handle }
    }

    async fn create_worker(&self, create: CreateWorker) -> Result<StatementResult> {
        debug!("{:?} starting", create.host_addr);

        let node = Handle::current()
            .create_node()
            .name(create.host_addr.to_string())
            .ip(create.host_addr.host().parse()?)
            .init(move || async move {
                Server::builder()
                    .add_service(WorkerRpcServiceServer::new(WorkerService::new()))
                    .add_service(HealthServer::new(HealthService))
                    .serve(SocketAddr::from((Ipv4Addr::UNSPECIFIED, DEFAULT_HOST_PORT)))
                    .await
                    .expect("worker could not be started");
            })
            .build()
            .id();

        self.workers
            .borrow_mut()
            .insert(create.host_addr.clone(), node);

        self.send_to_coordinator(ParsedStatement::CreateWorker(create))
            .await
    }

    async fn drop_worker(&self, drop: DropWorker) -> Result<StatementResult> {
        debug!("{:?} stopping", drop.host_addr);

        let result = self
            .send_to_coordinator(ParsedStatement::DropWorker(drop.clone()))
            .await;

        let node = self
            .workers
            .borrow_mut()
            .remove(&drop.host_addr)
            .expect("dropped a worker that the harness never created");
        Handle::current().send_ctrl_c(node);

        result
    }

    pub fn kill(&self, id: NodeId) {
        Handle::current().kill(id);
    }

    /// Does nothing for a worker that was dropped while it was down.
    /// A restarted worker has no fragments.
    pub fn restart(&self, id: NodeId) {
        if self.workers.borrow().values().any(|&worker| worker == id) {
            Handle::current().restart(id);
        }
    }

    pub fn pause(&self, id: NodeId) {
        Handle::current().pause(id);
    }

    pub fn resume(&self, id: NodeId) {
        Handle::current().resume(id);
    }

    pub fn clog(&self, id: NodeId) {
        NetSim::current().clog_node(id);
    }

    pub fn unclog(&self, id: NodeId) {
        NetSim::current().unclog_node(id);
    }

    pub fn degrade_network(&self, send_latency: Range<Duration>, packet_loss_rate: f64) {
        configure_network(send_latency, packet_loss_rate);
    }

    pub fn restore_network(&self) {
        configure_network(DEFAULT_SEND_LATENCY, 0.0);
    }

    /// The nodes of the created workers that were not dropped, including killed, paused, and clogged ones.
    pub fn get_worker_nodes(&self) -> Vec<NodeId> {
        self.workers.borrow().values().copied().collect()
    }

    /// The fragments that the given workers report as active.
    pub async fn get_worker_fragments(
        &self,
        workers: &HashSet<NetworkAddr>,
    ) -> HashSet<QueryFragmentId> {
        // The requests run on the coordinator's node, because the main node of the simulation has no IP and cannot connect.
        let statuses = workers
            .iter()
            .cloned()
            .map(|addr| self.coordinator.handle.spawn(request_worker_status(addr)));
        join_all(statuses)
            .await
            .into_iter()
            .flat_map(|status| {
                status
                    .expect("the worker status request did not complete")
                    .active_queries
            })
            .map(|q| {
                q.query_id
                    .and_then(|id| id.fragment_id())
                    .expect("a worker reported an active query without a fragment id")
            })
            .map(QueryFragmentId::new)
            .collect()
    }
}

async fn request_worker_status(addr: NetworkAddr) -> WorkerStatusResponse {
    WorkerRpcServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap_or_else(|e| panic!("failed to connect to {addr}: {e}"))
        .request_status(WorkerStatusRequest {
            after_unix_timestamp_in_milli_seconds: 0,
        })
        .await
        .unwrap_or_else(|e| panic!("{addr} did not report its status: {e}"))
        .into_inner()
}
