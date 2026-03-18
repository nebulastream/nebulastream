#![cfg(madsim)]
use crate::spec::NetworkConfig;
use crate::worker::{HealthServer, HealthServiceImpl, SingleNodeWorker, WorkerRpcServiceServer};
use anyhow::Result;
use model::request::Request;
use coordinator::coordinator::{CoordinatorRequest, start_for_sim};
use madsim::net::NetSim;
use madsim::runtime::Handle;
use model::worker;
use model::worker::endpoint::{GrpcAddr, NetworkAddr};
use model::worker::{CreateWorker, GetWorker, WorkerState};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{RngAlgorithm, TestRng, TestRunner};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tonic::transport::Server;
use std::collections::{HashMap, HashSet};
use tonic::transport::Endpoint;
use tracing::{debug, info};

use controller::worker::worker_task::worker_rpc_service;
use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;

const COORDINATOR_NAME: &str = "coordinator";
const COORDINATOR_IP: &str = "192.168.1.1";
const DEFAULT_SEND_LATENCY_LO: Duration = Duration::from_millis(1);
const DEFAULT_SEND_LATENCY_HI: Duration = Duration::from_millis(100);

pub const POLL_INTERVAL: Duration = Duration::from_secs(1);
const SEND_TIMEOUT: Duration = Duration::from_secs(30);
const WORKER_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(30);
const COORDINATOR_READY_TIMEOUT: Duration = Duration::from_secs(30);
const COORDINATOR_READY_POLL: Duration = Duration::from_millis(100);
const CHACHA_SEED_BYTES: usize = 32;
const SEED_CHUNK_SIZE: usize = 8;

thread_local! {
    static RUNNER: std::cell::RefCell<TestRunner> = std::cell::RefCell::new({
        let seed = Handle::current().seed();
        let seed_bytes = seed.to_le_bytes();
        let mut full_seed = [0u8; CHACHA_SEED_BYTES];
        for (i, chunk) in full_seed.chunks_exact_mut(SEED_CHUNK_SIZE).enumerate() {
            chunk.copy_from_slice(&seed_bytes);
            chunk[0] = chunk[0].wrapping_add(i as u8);
        }
        let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &full_seed);
        TestRunner::new_with_rng(Default::default(), rng)
    });
}

pub fn arb<S: Strategy>(strategy: S) -> S::Value {
    RUNNER.with(|r| strategy.new_tree(&mut r.borrow_mut()).unwrap().current())
}

pub struct TestHarness {
    workers: Vec<CreateWorker>,
    coordinator_sender: Arc<std::sync::Mutex<Option<flume::Sender<CoordinatorRequest>>>>,
    test_dir: std::path::PathBuf,
}

impl TestHarness {
    pub async fn start(
        workers: &[CreateWorker],
        network: Option<NetworkConfig>,
    ) -> Result<Self> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_timer(tracing_subscriber::fmt::time::uptime())
            .with_target(false)
            .try_init();

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

        let seed = Handle::current().seed();
        let test_dir = std::env::temp_dir().join(format!("{COORDINATOR_NAME}-{seed}"));
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).expect("failed to create test directory");
        let db_path = test_dir.join("state.db");
        let db_path = db_path.to_str().expect("non-UTF-8 temp path").to_string();

        let handle = Handle::current();
        Self::start_workers(&handle, workers);
        let sender = Self::start_coordinator(&handle, &db_path).await;
        info!("started {} workers + coordinator", workers.len());

        let harness = Self {
            workers: workers.to_vec(),
            coordinator_sender: sender,
            test_dir,
        };
        harness.register_workers().await;
        Ok(harness)
    }

    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }

    pub fn worker_name(&self, index: usize) -> String {
        format!("worker-{}", index + 1)
    }

    pub fn worker_host(&self, index: usize) -> NetworkAddr {
        self.workers[index].host_addr.clone()
    }

    pub fn worker_config(&self, index: usize) -> CreateWorker {
        self.workers[index].clone()
    }

    pub fn coordinator_name(&self) -> &str {
        COORDINATOR_NAME
    }

    async fn wait_for_coordinator(&self) -> flume::Sender<CoordinatorRequest> {
        let deadline = tokio::time::Instant::now() + COORDINATOR_READY_TIMEOUT;
        loop {
            {
                let mut guard = self.coordinator_sender.lock().unwrap();
                if let Some(ref sender) = *guard {
                    if !sender.is_disconnected() {
                        return sender.clone();
                    }
                    *guard = None;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "coordinator not available within {COORDINATOR_READY_TIMEOUT:?}"
            );
            tokio::time::sleep(COORDINATOR_READY_POLL).await;
        }
    }

    pub async fn send<P, R>(&self, payload: P) -> R
    where
        P: Debug,
        Request<P, R>: Into<CoordinatorRequest>,
    {
        let sender = self.wait_for_coordinator().await;
        let (rx, req) = Request::new(payload);
        sender.send_async(req.into()).await.unwrap();
        tokio::time::timeout(SEND_TIMEOUT, rx)
            .await
            .expect("coordinator did not respond within timeout")
            .expect("coordinator dropped the request")
    }

    async fn start_coordinator(
        net: &Handle,
        db_path: &str,
    ) -> Arc<std::sync::Mutex<Option<flume::Sender<CoordinatorRequest>>>> {
        let shared_sender: Arc<std::sync::Mutex<Option<flume::Sender<CoordinatorRequest>>>> =
            Arc::new(std::sync::Mutex::new(None));

        let (ready_tx, ready_rx) = flume::bounded(1);

        net.create_node()
            .name(COORDINATOR_NAME)
            .ip(COORDINATOR_IP.parse().unwrap())
            .init({
                let shared = shared_sender.clone();
                let ready_tx = ready_tx.clone();
                let db_path = db_path.to_string();
                move || {
                    let shared = shared.clone();
                    let ready_tx = ready_tx.clone();
                    let db_path = db_path.clone();
                    async move {
                        let sender = start_for_sim(&db_path).await;
                        *shared.lock().unwrap() = Some(sender);
                        let _ = ready_tx.send_async(()).await;
                        std::future::pending::<()>().await;
                    }
                }
            })
            .build();

        ready_rx
            .recv_async()
            .await
            .expect("failed to receive coordinator ready signal");
        shared_sender
    }

    fn start_workers(net: &Handle, workers: &[CreateWorker]) {
        workers.iter().enumerate().for_each(|(i, worker)| {
            let idx = i + 1;
            let ip = worker.host_addr.host.clone();
            net.create_node()
                .name(format!("worker-{idx}"))
                .ip(ip.parse().unwrap())
                .init(move || async move {
                    debug!("worker-{idx} starting");
                    let worker = SingleNodeWorker::new();

                    Server::builder()
                        .add_service(WorkerRpcServiceServer::new(worker))
                        .add_service(HealthServer::new(HealthServiceImpl))
                        .serve("0.0.0.0:8080".parse().unwrap())
                        .await
                        .expect("worker could not be started");
                })
                .build();
        });
    }

    async fn register_workers(&self) {
        let strategy = ExponentialBackoff::from_millis(50).factor(10).map(jitter).take(10);
        for worker in &self.workers {
            Retry::spawn(strategy.clone(), || async {
                let result: anyhow::Result<worker::Model> =
                    self.send(worker.clone()).await;
                result
            })
            .await
            .expect("worker registration failed after retries");
        }

        let num_workers = self.workers.len();
        let deadline = tokio::time::Instant::now() + WORKER_REGISTRATION_TIMEOUT;
        loop {
            let workers: Vec<worker::Model> = self.send(GetWorker::all()).await.unwrap();
            let active_count = workers
                .iter()
                .filter(|w| w.current_state == WorkerState::Active)
                .count();

            if active_count == num_workers {
                info!("all {num_workers} workers active");
                return;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "workers did not become Active within {WORKER_REGISTRATION_TIMEOUT:?} ({active_count}/{num_workers} active)"
            );

            info!("waiting for workers to become active ({active_count}/{num_workers})");
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    pub fn restart_worker(&self, index: usize) {
        let name = self.worker_name(index);
        Handle::current().restart(&name);
    }

    pub async fn active_fragments_by_worker(
        &self,
    ) -> HashMap<GrpcAddr, HashSet<u64>> {
        let workers: Vec<worker::Model> = self.send(GetWorker::all()).await.unwrap();

        let (tx, rx) = flume::bounded(1);
        let addrs: Vec<GrpcAddr> = workers
            .into_iter()
            .filter(|w| w.current_state == WorkerState::Active)
            .map(|w| w.grpc_addr)
            .collect();

        let coordinator_node = Handle::current()
            .get_node(COORDINATOR_NAME)
            .expect("coordinator node not found");
        coordinator_node.spawn(async move {
            let mut result = HashMap::new();
            for addr in &addrs {
                let endpoint = match Endpoint::from_shared(format!("http://{}", addr)) {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                let channel = match endpoint.connect().await {
                    Ok(c) => c,
                    Err(e) => {
                        info!("failed to connect to worker {addr}: {e}");
                        continue;
                    }
                };
                let mut client = WorkerRpcServiceClient::new(channel);
                let req = tonic::Request::new(worker_rpc_service::WorkerStatusRequest {
                    after_unix_timestamp_in_ms: 0,
                });
                match client.request_status(req).await {
                    Ok(resp) => {
                        let ids: HashSet<u64> = resp
                            .into_inner()
                            .active_queries
                            .iter()
                            .map(|aq| aq.query_id)
                            .collect();
                        result.insert(addr.clone(), ids);
                    }
                    Err(e) => {
                        info!("failed to get status from worker {addr}: {e}");
                    }
                }
            }
            let _ = tx.send_async(result).await;
        });

        rx.recv_async().await.unwrap_or_default()
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.test_dir);
    }
}
