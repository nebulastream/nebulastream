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

#![cfg(madsim)]

use crate::config::NetworkConfig;
use crate::worker::{HealthServer, HealthServiceImpl, SingleNodeWorker, WorkerRpcServiceServer};
use anyhow::Result;
use controller::remote::WorkerRpcServiceClient;
use controller::remote::worker_rpc_service::{WorkerStatusRequest, WorkerStatusResponse};
use futures_util::future::join_all;
use madsim::net::NetSim;
use madsim::runtime::{Handle, NodeHandle};
use madsim::task::{NodeId, ToNodeId};
use model::database::{Database, StateBackend};
use model::identifier::FragmentId;
use model::request::Request;
use model::statement::{Statement, StatementResult};
use model::worker::endpoint::NetworkAddr;
use model::worker::{CreateWorker, DropWorker};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{RngAlgorithm, TestRng, TestRunner};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::transport::Endpoint;
use tonic::transport::Server;
use tracing::debug;

pub(crate) const COORDINATOR_NAME: &str = "coordinator";
const COORDINATOR_IP: &str = "192.168.1.1";

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
const DEFAULT_SEND_LATENCY_LO: Duration = Duration::from_millis(1);
const DEFAULT_SEND_LATENCY_HI: Duration = Duration::from_millis(100);
const SEND_TIMEOUT: Duration = Duration::from_secs(30);

struct Coordinator {
    sender: flume::Sender<Request>,
    handle: NodeHandle,
}

pub struct TestHarness {
    coordinator: Coordinator,
    workers: RefCell<HashMap<NetworkAddr, NodeId>>,
    test_dir: std::path::PathBuf,
}

impl TestHarness {
    pub async fn start(network: Option<NetworkConfig>) -> Self {
        // Set network conditions for simulation run
        NetSim::current().update_config(|cfg| {
            if let Some(ref net) = network {
                let lo = net
                    .send_latency_lo_ms
                    .map(Duration::from_millis)
                    .unwrap_or(DEFAULT_SEND_LATENCY_LO);
                let hi = net
                    .send_latency_hi_ms
                    .map(Duration::from_millis)
                    .unwrap_or(DEFAULT_SEND_LATENCY_HI);
                cfg.send_latency = lo..hi;
                cfg.packet_loss_rate = net.packet_loss_rate.unwrap_or(0.0);
            } else {
                cfg.send_latency = DEFAULT_SEND_LATENCY_LO..DEFAULT_SEND_LATENCY_HI;
                cfg.packet_loss_rate = 0.0;
            }
        });

        // prepare state directory for the DB
        let seed = Handle::current().seed();
        let test_dir = std::env::temp_dir().join(format!("{COORDINATOR_NAME}-{seed}"));
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).expect("failed to create test directory");
        Self {
            coordinator: Self::start_coordinator(
                test_dir
                    .join("state.db")
                    .to_str()
                    .expect("non-UTF-8 temp path"),
            )
            .await,
            workers: Default::default(),
            test_dir,
        }
    }

    pub async fn send(&self, statement: Statement) -> Result<StatementResult> {
        let (rx, req) = Request::new(statement);
        self.coordinator.sender.send_async(req).await?;
        tokio::time::timeout(SEND_TIMEOUT, rx)
            .await
            .expect("did not respond within timeout")
            .expect("dropped the request")
    }

    async fn start_coordinator(db_path: &str) -> Coordinator {
        let (sender, receiver) = flume::bounded(16);

        let handle = Handle::current()
            .create_node()
            .name(COORDINATOR_NAME)
            .ip(COORDINATOR_IP.parse().unwrap())
            .init({
                let receiver = receiver.clone();
                let db_path = db_path.to_string();
                move || {
                    let receiver = receiver.clone();
                    let db_path = db_path.clone();
                    async move {
                        let db = Database::with(StateBackend::sqlite(&db_path))
                            .await
                            .expect("failed to create database");
                        db.migrate()
                            .await
                            .expect("failed to run database migrations");
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
            .ip(req.host_addr.host.parse()?)
            .init(move || async move {
                Server::builder()
                    .add_service(WorkerRpcServiceServer::new(SingleNodeWorker::new()))
                    .add_service(HealthServer::new(HealthServiceImpl))
                    .serve("0.0.0.0:8080".parse().unwrap())
                    .await
                    .expect("worker could not be started");
            })
            .build()
            .id();

        self.workers.borrow_mut().insert(req.host_addr.clone(), id);

        self.send(Statement::CreateWorker(req)).await
    }

    pub async fn drop_worker(&self, req: DropWorker) -> Result<StatementResult> {
        debug!("{:?} stopping", req.host_addr);

        let result = self.send(Statement::DropWorker(req.clone())).await;

        Handle::current().send_ctrl_c(self.workers.borrow().get(&req.host_addr).unwrap());
        self.workers.borrow_mut().remove(&req.host_addr);

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

    pub fn clog_link(&self, src: NodeId, dst: NodeId) {
        NetSim::current().clog_link(src, dst);
    }

    pub fn unclog_link(&self, src: NodeId, dst: NodeId) {
        NetSim::current().unclog_link(src, dst);
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

    pub fn get_all_nodes(&self) -> Vec<NodeId> {
        let mut nodes = vec![self.coordinator.handle.id()];
        nodes.extend(self.get_worker_ids());
        nodes
    }

    pub async fn worker_status(
        &self,
        workers: &[NetworkAddr],
    ) -> (Vec<FragmentId>, Vec<FragmentId>) {
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
        let mut terminated = Vec::new();
        for rsp in responses {
            active.extend(
                rsp.active_queries
                    .into_iter()
                    .filter_map(|q| q.query_id.map(|id| FragmentId::new(id.id))),
            );
            terminated.extend(
                rsp.terminated_queries
                    .into_iter()
                    .filter_map(|q| q.query_id.map(|id| FragmentId::new(id.id))),
            );
        }
        (active, terminated)
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.test_dir);
    }
}
