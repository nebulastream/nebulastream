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

//! Simulation harness: starts the coordinator and the worker nodes inside madsim, and sends the statements of the workloads.
//!
//! The coordinator and each worker is a separate madsim node with its own IP.
//! For example, the coordinator is 192.168.1.1 and the first two workers are 192.168.1.2 and 192.168.1.3.
//! madsim assigns an IP to one node only, so an address that is registered again restarts its earlier node.
//! A restart and the network settings go through the harness, which knows the registered workers and the default latency.
//! The other faults go to madsim directly.

use crate::worker::{WorkerRpcServiceServer, WorkerService};
use anyhow::Result;
use controller::worker_client::remote::WorkerRpcServiceClient;
use controller::worker_client::remote::worker_rpc_service::{
    WorkerStatusRequest, WorkerStatusResponse,
};
use madsim::net::NetSim;
use madsim::runtime::{Handle, NodeHandle};
use madsim::task::NodeId;
use model::database::{Database, StateBackend};
use model::identifier::QueryFragmentId;
use model::query::GetQuery;
use model::request::{Statement, Wait, Watched};
use model::statement::{ParsedStatement, StatementResult};
use model::worker::endpoint::{DEFAULT_HOST_PORT, NetworkAddr};
use model::worker::{CreateWorker, DropWorker};
use request_handler::Client;
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

pub const fn build_ip(octet: u8) -> Ipv4Addr {
    let [a, b, c] = SUBNET;
    Ipv4Addr::new(a, b, c, octet)
}

/// The coordinator replies to a statement immediately, and to a watch by the end of its timeout,
/// so a reply that takes this much longer in simulated time means that the request handler is stuck.
const REPLY_TIMEOUT: Duration = Duration::from_secs(30);

/// The latency range of the simulated network outside of a degradation window.
/// The lower bound is above zero, so no trial relies on instant delivery.
const DEFAULT_SEND_LATENCY: Range<Duration> = Duration::from_millis(1)..Duration::from_millis(100);

/// Starts the coordinator of one trial and the nodes of the workers that it creates, and injects faults into them.
/// It must be created and used inside a running madsim runtime.
pub struct Harness {
    client: Client,
    coordinator: NodeHandle,
    /// Every worker node that the harness started, also those of dropped workers.
    nodes: RefCell<HashMap<NetworkAddr, NodeId>>,
    /// The nodes of the registered workers.
    workers: RefCell<HashMap<NetworkAddr, NodeId>>,
}

impl Harness {
    pub fn start() -> Self {
        let (sender, receiver) = async_channel::bounded(16);
        let coordinator = Handle::current()
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
        let harness = Self {
            client: Client::new(sender),
            coordinator,
            nodes: RefCell::default(),
            workers: RefCell::default(),
        };
        harness.restore_network();
        harness
    }

    /// A worker create starts the worker's node before the statement is sent, and stops it if the create fails.
    /// A worker drop stops the node after the reply, also when the drop failed.
    pub async fn send(&self, statement: ParsedStatement) -> Result<StatementResult> {
        match statement {
            ParsedStatement::CreateWorker(create) => self.create_worker(create).await,
            ParsedStatement::DropWorker(drop) => self.drop_worker(drop).await,
            other => self.send_to_coordinator(other).await,
        }
    }

    async fn send_to_coordinator(&self, statement: ParsedStatement) -> Result<StatementResult> {
        let reply = self.client.send(Statement::Parsed(statement));
        tokio::time::timeout(REPLY_TIMEOUT, reply)
            .await
            .unwrap_or_else(|_| panic!("the coordinator did not reply within {REPLY_TIMEOUT:?}"))
    }

    /// Every watch in the simulation has a timeout, so a lost reply fails the trial instead of hanging it.
    pub async fn watch(&self, read: GetQuery, wait: Wait) -> Result<Watched> {
        let bound = wait.timeout.expect("a watch without a timeout") + REPLY_TIMEOUT;
        tokio::time::timeout(bound, self.client.watch(read, wait))
            .await
            .unwrap_or_else(|_| panic!("the coordinator did not reply to a watch within {bound:?}"))
    }

    async fn create_worker(&self, create: CreateWorker) -> Result<StatementResult> {
        debug!("{:?} starting", create.host_addr);

        let addr = create.host_addr.clone();
        let earlier = self.nodes.borrow().get(&addr).copied();
        let node = if let Some(node) = earlier {
            Handle::current().restart(node);
            node
        } else {
            let node = Handle::current()
                .create_node()
                .name(addr.to_string())
                .ip(addr.host().parse()?)
                .init(move || async move {
                    Server::builder()
                        .add_service(WorkerRpcServiceServer::new(WorkerService::default()))
                        .serve(SocketAddr::from((Ipv4Addr::UNSPECIFIED, DEFAULT_HOST_PORT)))
                        .await
                        .expect("worker could not be started");
                })
                .build()
                .id();
            self.nodes.borrow_mut().insert(addr.clone(), node);
            node
        };
        self.workers.borrow_mut().insert(addr.clone(), node);

        let result = self
            .send_to_coordinator(ParsedStatement::CreateWorker(create))
            .await;
        if result.is_err() {
            self.workers.borrow_mut().remove(&addr);
            Handle::current().kill(node);
        }
        result
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

    /// Does nothing for a worker that was dropped while it was down,
    /// or that runs again because its address was registered again.
    /// A restart of a running node would keep its old socket bound, and the new server could not start.
    /// A restarted worker has no fragments.
    pub fn restart(&self, id: NodeId) {
        let handle = Handle::current();
        if handle.is_exit(id) && self.workers.borrow().values().any(|&worker| worker == id) {
            handle.restart(id);
        }
    }

    pub fn degrade_network(&self, send_latency: Range<Duration>, packet_loss_rate: f64) {
        NetSim::current().update_config(|cfg| {
            cfg.send_latency = send_latency;
            cfg.packet_loss_rate = packet_loss_rate;
        });
    }

    pub fn restore_network(&self) {
        self.degrade_network(DEFAULT_SEND_LATENCY, 0.0);
    }

    /// The nodes of the created workers that were not dropped, including killed, paused, and clogged ones.
    pub fn get_worker_nodes(&self) -> Vec<NodeId> {
        self.workers.borrow().values().copied().collect()
    }

    /// The fragments that the worker reports as active.
    pub async fn get_worker_fragments(&self, addr: NetworkAddr) -> HashSet<QueryFragmentId> {
        // The request runs on the coordinator's node, because the main node of the simulation has no IP and cannot connect.
        self.coordinator
            .spawn(request_worker_status(addr))
            .await
            .expect("the worker status request did not complete")
            .active_queries
            .into_iter()
            .map(|q| {
                q.query_id
                    .and_then(|id| id.get_fragment_id())
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
