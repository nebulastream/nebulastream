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

//! Simulation harness: starts the coordinator and worker nodes inside madsim
//! and gives workloads one API to operate them.
//!
//! The coordinator and each worker is a separate madsim node with its own IP.
//! Statements and fault primitives (kill, pause, clog, restart) are method calls on the harness;
//! the faults themselves happen inside the simulator.

use crate::config::{SEND_LATENCY_HI, SEND_LATENCY_LO};
use crate::worker::{HealthServer, HealthServiceImpl, SingleNodeWorker, WorkerRpcServiceServer};
use anyhow::Result;
use controller::remote::WorkerRpcServiceClient;
use controller::remote::worker_rpc_service::{WorkerStatusRequest, WorkerStatusResponse};
use futures::future::join_all;
use madsim::net::NetSim;
use madsim::runtime::{Handle, NodeHandle};
use madsim::task::{NodeId, ToNodeId};
use model::database::{Database, StateBackend};
use model::identifier::QueryFragmentId;
use model::request::{Request, Statement};
use model::statement::{ParsedStatement, StatementResult};
use model::worker::endpoint::{DEFAULT_HOST_PORT, NetworkAddr};
use model::worker::{CreateWorker, DropWorker};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::transport::Endpoint;
use tonic::transport::Server;
use tracing::debug;

pub(crate) const COORDINATOR_NAME: &str = "coordinator";
const COORDINATOR_IP: &str = "192.168.1.1";

/// The simulation only sends statements that the coordinator replies to at once,
/// so a reply that takes this long in simulated time means that the request handler is stuck.
const SEND_TIMEOUT: Duration = Duration::from_secs(30);

thread_local! {
    static NEXT_WORKER_OCTET: Cell<u8> = const { Cell::new(2) }; // .1 is the coordinator
}

pub fn next_worker_ip() -> String {
    NEXT_WORKER_OCTET.with(|cell| {
        let octet = cell.get();
        cell.set(octet.checked_add(1).expect("ran out of worker IPs"));
        format!("192.168.1.{octet}")
    })
}
struct Coordinator {
    sender: async_channel::Sender<Request>,
    handle: NodeHandle,
}

/// Per-trial control point over the simulated cluster.
pub struct TestHarness {
    coordinator: Coordinator,
    workers: RefCell<HashMap<NetworkAddr, NodeId>>,
}

impl TestHarness {
    pub async fn start() -> Self {
        NetSim::current().update_config(|cfg| {
            cfg.send_latency = SEND_LATENCY_LO..SEND_LATENCY_HI;
            cfg.packet_loss_rate = 0.0;
        });

        Self {
            coordinator: Self::start_coordinator().await,
            workers: Default::default(),
        }
    }

    pub async fn send(&self, statement: ParsedStatement) -> Result<StatementResult> {
        let (rx, req) = Request::new(Statement::Parsed(statement));
        self.coordinator.sender.send(req).await?;
        tokio::time::timeout(SEND_TIMEOUT, rx)
            .await
            .unwrap_or_else(|_| panic!("the coordinator did not reply within {SEND_TIMEOUT:?}"))
            .expect("the coordinator dropped the request without a reply")
    }

    async fn start_coordinator() -> Coordinator {
        let (sender, receiver) = async_channel::bounded(16);

        let handle = Handle::current()
            .create_node()
            .name(COORDINATOR_NAME)
            .ip(COORDINATOR_IP.parse().unwrap())
            .init({
                let receiver = receiver.clone();
                move || {
                    let receiver = receiver.clone();
                    async move {
                        // The in-memory catalog has a single connection.
                        // The simulation runs all connections on one thread,
                        // so a connection that waits for the write lock would block the one that holds it.
                        let db = Database::open(StateBackend::Memory)
                            .await
                            .expect("failed to open the database");
                        coordinator::run(db, None, None, receiver).await;
                    }
                }
            })
            .build();

        Coordinator { sender, handle }
    }

    pub async fn create_worker(&self, req: CreateWorker) -> Result<StatementResult> {
        debug!("{:?} starting", req.host_addr);

        let id = Handle::current()
            .create_node()
            .name(req.host_addr.to_string())
            .ip(req.host_addr.host().parse()?)
            .init(move || async move {
                Server::builder()
                    .add_service(WorkerRpcServiceServer::new(SingleNodeWorker::new()))
                    .add_service(HealthServer::new(HealthServiceImpl))
                    .serve(format!("0.0.0.0:{DEFAULT_HOST_PORT}").parse().unwrap())
                    .await
                    .expect("worker could not be started");
            })
            .build()
            .id();

        self.workers.borrow_mut().insert(req.host_addr.clone(), id);

        self.send(ParsedStatement::CreateWorker(req)).await
    }

    pub async fn drop_worker(&self, req: DropWorker) -> Result<StatementResult> {
        debug!("{:?} stopping", req.host_addr);

        let result = self.send(ParsedStatement::DropWorker(req.clone())).await;

        let node = self
            .workers
            .borrow_mut()
            .remove(&req.host_addr)
            .expect("dropped a worker that the harness never created");
        Handle::current().send_ctrl_c(node);

        result
    }

    pub fn kill(&self, id: impl ToNodeId) {
        Handle::current().kill(id);
    }

    pub fn restart(&self, id: impl ToNodeId) {
        Handle::current().restart(id);
    }

    pub fn pause(&self, id: impl ToNodeId) {
        Handle::current().pause(id);
    }

    pub fn resume(&self, id: impl ToNodeId) {
        Handle::current().resume(id);
    }

    pub fn clog_node(&self, id: NodeId) {
        NetSim::current().clog_node(id);
    }

    pub fn unclog_node(&self, id: NodeId) {
        NetSim::current().unclog_node(id);
    }

    pub fn get_worker_ids(&self) -> Vec<NodeId> {
        self.workers.borrow().values().copied().collect()
    }

    /// The fragments that the given workers report as active.
    pub async fn get_active_fragments(&self, workers: &[NetworkAddr]) -> Vec<QueryFragmentId> {
        let (tx, rx): (
            Vec<oneshot::Sender<WorkerStatusResponse>>,
            Vec<oneshot::Receiver<WorkerStatusResponse>>,
        ) = (0..workers.len()).map(|_| oneshot::channel()).unzip();

        for (addr, tx) in workers.iter().cloned().zip(tx) {
            self.coordinator.handle.spawn(async move {
                let endpoint = Endpoint::from_shared(format!("http://{}", addr)).unwrap();
                let channel = endpoint.connect().await.unwrap();
                let mut client = WorkerRpcServiceClient::new(channel);
                let req = tonic::Request::new(WorkerStatusRequest {
                    after_unix_timestamp_in_milli_seconds: 0,
                });
                let rsp = client.request_status(req).await.unwrap().into_inner();
                tx.send(rsp).unwrap();
            });
        }

        let responses = join_all(rx.into_iter().map(|rx| async { rx.await.unwrap() })).await;

        let mut active = Vec::new();
        for rsp in responses {
            active.extend(rsp.active_queries.into_iter().filter_map(|q| {
                q.query_id
                    .and_then(|id| id.fragment_id())
                    .map(QueryFragmentId::new)
            }));
        }
        active
    }
}
