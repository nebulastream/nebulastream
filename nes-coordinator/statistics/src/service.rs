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
use crate::placement;
use crate::query_gen::{
    self, COLLECTION_PROBE_ID, CollectRequest, GenerateError, QueryShape, VALUE,
};
use crate::registry::{
    CollectionDomain, Deploying, Deregistration, Key, Metric, Registered, Registry, Report,
    Reservation, Slot, Trigger,
};
use futures::FutureExt;
use model::worker::endpoint::NetworkAddr;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

pub const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_PROBE_SETTLE: Duration = Duration::from_millis(500);
pub const PROBE_SOURCE_PORT_BASE: u16 = 10000;
/// Probe sources take the ports above the base in turn, so probes deployed together or at the same time do not ask one
/// worker for the same port.
pub const PROBE_SOURCE_PORT_RANGE: u16 = 100;
pub const PROBE_SOURCE_PORT_ATTEMPTS: u16 = 10;

/// Cloned because every collect awaiting one deployment receives its error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum StatisticError {
    #[error(transparent)]
    Generate(#[from] GenerateError),
    #[error("statistic key is not registered")]
    UnknownKey,
    #[error(
        "statistic {statistic_id} is already collected with {differences}; deregister it to collect it differently"
    )]
    CollectedDifferently {
        statistic_id: u64,
        differences: String,
    },
    #[error("statistic {statistic_id} is being deregistered")]
    Deregistering { statistic_id: u64 },
    #[error("statistic {statistic_id} was deregistered before its query was deployed")]
    DeregisteredWhileDeploying { statistic_id: u64 },
    #[error("a statistic collected without a condition sends no reports to watch")]
    NeverReports,
    #[error("the deployment of statistic {statistic_id} ended abruptly: {reason}")]
    DeploymentAborted { statistic_id: u64, reason: String },
    #[error(
        "stopping query {query_id} of statistic {statistic_id} failed, so the statistic stays registered: {reason}"
    )]
    StopFailed {
        query_id: u64,
        statistic_id: u64,
        reason: String,
    },
    #[error("the statistic service has no address; start its gRPC server first")]
    NoServiceAddress,
    #[error("logical source {0} has no physical source to collect a statistic over")]
    NoPhysicalSource(String),
    #[error("no worker can take the sink of the statistic query over {logical_source}: {reason}")]
    NoPlacement {
        logical_source: String,
        reason: String,
    },
    #[error("looking up the workers failed: {0}")]
    Catalog(String),
    #[error("the deployed query {query_id} writes statistic {statistic_id} on no worker")]
    WriterNotPlaced { query_id: u64, statistic_id: u64 },
    #[error("no probe query could be deployed")]
    ProbeNotDeployed,
    #[error("submitting the statistic query failed: {0}")]
    Submit(String),
}

#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    /// The planner found no placement for the query, for example because its sink is not downstream of its sources.
    #[error("{0}")]
    Placement(String),
    #[error("{0}")]
    Other(String),
}

/// One fragment of a deployed query: the worker it runs on and its serialized plan.
#[derive(Clone, Debug)]
pub struct Fragment {
    pub worker: NetworkAddr,
    pub plan: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct Deployment {
    pub query_id: u64,
    pub fragments: Vec<Fragment>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectResult {
    pub query_id: u64,
    pub statistic_id: u64,
    pub already_existed: bool,
}

#[async_trait::async_trait]
pub trait QuerySubmitter: Send + Sync {
    async fn submit(&self, sql: String) -> Result<Deployment, SubmitError>;
    async fn stop(&self, query_id: u64) -> Result<(), String>;
}

/// What the service needs to know about the workers to place its queries.
#[async_trait::async_trait]
pub trait WorkerCatalog: Send + Sync {
    /// The workers hosting a physical source of the logical source.
    async fn source_workers(&self, logical_source: &str) -> Result<Vec<NetworkAddr>, String>;
    /// Every registered worker.
    async fn workers(&self) -> Result<Vec<NetworkAddr>, String>;
}

#[async_trait::async_trait]
pub trait SchemaResolver: Send + Sync {
    async fn field_type(&self, logical_source: &str, field: &str) -> Option<String>;
}

#[async_trait::async_trait]
pub trait ProbeImpulse: Send + Sync {
    /// Asks the probe source listening at `endpoint` for the statistic's windows within [start_ts, end_ts].
    async fn request(
        &self,
        endpoint: &NetworkAddr,
        statistic_id: u64,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<(), String>;
}

#[derive(Default, Clone, Copy)]
struct PendingProbe {
    sum: f64,
    reports: u64,
    probe_id: u64,
}

/// Frees a reserved key when its deployment ends, unless the deployment registered its statistic first.
struct ReservationGuard<'a> {
    registry: &'a Registry,
    key: &'a Key,
    statistic_id: u64,
}

impl Drop for ReservationGuard<'_> {
    fn drop(&mut self) {
        self.registry.abandon(self.key, self.statistic_id);
    }
}

/// A deployed probe query and the endpoint its source listens on.
struct ProbeQuery {
    statistic_id: u64,
    query_id: u64,
    source: NetworkAddr,
}

pub struct StatisticService {
    registry: Registry,
    next_statistic_id: AtomicU64,
    next_probe_id: AtomicU64,
    next_probe_port: AtomicU16,
    submitter: Arc<dyn QuerySubmitter>,
    catalog: Arc<dyn WorkerCatalog>,
    resolver: Arc<dyn SchemaResolver>,
    impulse: Arc<dyn ProbeImpulse>,
    service_address: String,
    pending: Mutex<HashMap<u64, PendingProbe>>,
    reported: Notify,
    probe_timeout: Duration,
    probe_settle: Duration,
}

impl StatisticService {
    pub fn new(
        submitter: Arc<dyn QuerySubmitter>,
        catalog: Arc<dyn WorkerCatalog>,
        resolver: Arc<dyn SchemaResolver>,
        impulse: Arc<dyn ProbeImpulse>,
        service_address: String,
    ) -> Self {
        Self {
            registry: Registry::default(),
            next_statistic_id: AtomicU64::new(1),
            next_probe_id: AtomicU64::new(COLLECTION_PROBE_ID + 1),
            next_probe_port: AtomicU16::new(0),
            submitter,
            catalog,
            resolver,
            impulse,
            service_address,
            pending: Mutex::new(HashMap::new()),
            reported: Notify::new(),
            probe_timeout: DEFAULT_PROBE_TIMEOUT,
            probe_settle: DEFAULT_PROBE_SETTLE,
        }
    }

    pub fn with_probe_timings(mut self, timeout: Duration, settle: Duration) -> Self {
        self.probe_timeout = timeout;
        self.probe_settle = settle;
        self
    }

    fn address(&self) -> Result<&str, StatisticError> {
        if self.service_address.is_empty() {
            return Err(StatisticError::NoServiceAddress);
        }
        Ok(&self.service_address)
    }

    pub fn key_of(request: &CollectRequest) -> Key {
        Key {
            metric: request.metric,
            domain: request.domain.clone(),
            window_size_ms: request.window.size().as_millis(),
        }
    }

    /// Collects the statistic `request` asks for. Its key is reserved before the query is deployed, so concurrent
    /// requests for one statistic share a single deployment and its outcome.
    pub async fn collect_new_statistic(
        self: &Arc<Self>,
        request: &CollectRequest,
        trigger: Option<Trigger>,
    ) -> Result<CollectResult, StatisticError> {
        let address = self.address()?;
        // A request this port cannot serve is rejected before any worker is looked up.
        query_gen::collected_field(request)?;
        query_gen::aggregation(request.metric)?;

        let key = Self::key_of(request);
        let shape = QueryShape::of(request);
        if trigger.is_some() && !shape.reports() {
            return Err(StatisticError::NeverReports);
        }
        let (holder, already_existed) = match self
            .registry
            .reserve(&key, || self.start_deployment(&key, request, address))
        {
            Reservation::Reserved(deploying) => (Slot::Deploying(deploying), false),
            Reservation::Taken(slot) => (slot, true),
        };

        let existing = match &holder {
            Slot::Stopping(registered) => {
                return Err(StatisticError::Deregistering {
                    statistic_id: registered.statistic_id,
                });
            }
            Slot::Deploying(deploying) => &deploying.shape,
            Slot::Registered(registered) => &registered.shape,
        };
        // The query keeps the shape it was deployed with, so a request asking for another one would silently get the
        // wrong statistic or the wrong reports.
        if let Some(differences) = shape.differences_from(existing) {
            return Err(StatisticError::CollectedDifferently {
                statistic_id: holder.statistic_id(),
                differences,
            });
        }
        if let Some(trigger) = trigger {
            // Fails only if the statistic was deregistered meanwhile, which the outcome below reports.
            let _ = self.registry.add_trigger(&key, trigger);
        }

        let registered = match holder {
            Slot::Deploying(deploying) => deploying.outcome.await?,
            Slot::Registered(registered) | Slot::Stopping(registered) => registered,
        };
        Ok(CollectResult {
            query_id: registered.query_id,
            statistic_id: registered.statistic_id,
            already_existed,
        })
    }

    /// Allocates the statistic for a reserved key and deploys its query in a task of its own, which settles the
    /// reservation even if every collect awaiting it goes away. Runs under the registry's lock, so it only spawns.
    fn start_deployment(
        self: &Arc<Self>,
        key: &Key,
        request: &CollectRequest,
        address: &str,
    ) -> Deploying {
        let statistic_id = self.next_statistic_id.fetch_add(1, Ordering::Relaxed);
        let shape = QueryShape::of(request);
        let deployment = tokio::spawn(Arc::clone(self).deploy(
            key.clone(),
            request.clone(),
            address.to_string(),
            statistic_id,
        ));
        let outcome = async move {
            deployment.await.unwrap_or_else(|error| {
                Err(StatisticError::DeploymentAborted {
                    statistic_id,
                    reason: error.to_string(),
                })
            })
        };
        Deploying {
            statistic_id,
            shape,
            outcome: outcome.boxed().shared(),
        }
    }

    /// Deploys the query of a reserved statistic and registers it in place of the reservation.
    async fn deploy(
        self: Arc<Self>,
        key: Key,
        request: CollectRequest,
        address: String,
        statistic_id: u64,
    ) -> Result<Registered, StatisticError> {
        // Frees the key when the deployment ends without registering, whether it fails or panics.
        let _reservation = ReservationGuard {
            registry: &self.registry,
            key: &key,
            statistic_id,
        };
        let registered = self.deploy_query(&request, &address, statistic_id).await?;
        if self.registry.finish(&key, registered.clone()) {
            return Ok(registered);
        }
        // The statistic was deregistered while its query was deploying, so nothing holds the query any more.
        if let Err(error) = self.submitter.stop(registered.query_id).await {
            tracing::warn!(query_id = registered.query_id, %error, "the query of a statistic deregistered while deploying could not be stopped");
        }
        Err(StatisticError::DeregisteredWhileDeploying { statistic_id })
    }

    async fn deploy_query(
        &self,
        request: &CollectRequest,
        address: &str,
        statistic_id: u64,
    ) -> Result<Registered, StatisticError> {
        let (logical_source, _) = query_gen::collected_field(request)?;
        let payload_type = self
            .scalar_payload_type(request.metric, &request.domain)
            .await;

        let mut reason = String::new();
        for worker in self.sink_candidates(logical_source).await? {
            let sql = query_gen::collection_query(
                request,
                statistic_id,
                address,
                payload_type.as_deref(),
                &worker,
            )?;
            match self.submitter.submit(sql).await {
                Ok(deployment) => {
                    let query_id = deployment.query_id;
                    // The statistic is read back where it is written, which is up to the planner and need not be the
                    // sink's worker.
                    let Some(writer) =
                        placement::writer_worker(&deployment.fragments, statistic_id)
                    else {
                        if let Err(error) = self.submitter.stop(query_id).await {
                            tracing::warn!(query_id, %error, "the statistic query could not be stopped");
                        }
                        return Err(StatisticError::WriterNotPlaced {
                            query_id,
                            statistic_id,
                        });
                    };
                    tracing::debug!(statistic_id, sink = %worker, %writer, "statistic query deployed");
                    return Ok(Registered {
                        query_id,
                        statistic_id,
                        worker: writer,
                        shape: QueryShape::of(request),
                    });
                }
                Err(SubmitError::Placement(error)) => {
                    tracing::debug!(%worker, %error, "the statistic query cannot be placed with its sink on this worker");
                    reason = error;
                }
                Err(SubmitError::Other(error)) => return Err(StatisticError::Submit(error)),
            }
        }
        Err(StatisticError::NoPlacement {
            logical_source: logical_source.to_string(),
            reason,
        })
    }

    /// The workers to try the sink of a statistic query on, best first.
    ///
    /// The source's own workers come first: with one physical source, a sink on its worker keeps the whole query there.
    /// With several on different workers only a worker downstream of all of them can take the sink, and the planner is
    /// what knows the topology, so the remaining workers follow for it to accept or reject.
    async fn sink_candidates(
        &self,
        logical_source: &str,
    ) -> Result<Vec<NetworkAddr>, StatisticError> {
        let mut candidates: Vec<NetworkAddr> = Vec::new();
        for worker in self
            .catalog
            .source_workers(logical_source)
            .await
            .map_err(StatisticError::Catalog)?
        {
            if !candidates.contains(&worker) {
                candidates.push(worker);
            }
        }
        if candidates.is_empty() {
            return Err(StatisticError::NoPhysicalSource(logical_source.to_string()));
        }

        let mut others: Vec<NetworkAddr> = self
            .catalog
            .workers()
            .await
            .map_err(StatisticError::Catalog)?
            .into_iter()
            .filter(|worker| !candidates.contains(worker))
            .collect();
        others.sort_by_key(ToString::to_string);
        candidates.extend(others);
        Ok(candidates)
    }

    async fn scalar_payload_type(
        &self,
        metric: Metric,
        domain: &CollectionDomain,
    ) -> Option<String> {
        if let Some(intrinsic) = query_gen::intrinsic_payload_type(metric) {
            return Some(intrinsic.to_string());
        }
        match domain {
            CollectionDomain::Data {
                logical_source_name,
                field_name,
            } => {
                self.resolver
                    .field_type(logical_source_name, field_name)
                    .await
            }
            _ => None,
        }
    }

    pub fn add_condition_trigger(&self, key: &Key, trigger: Trigger) -> Result<(), StatisticError> {
        self.registry.add_trigger(key, trigger)
    }

    /// Stops the statistic's query and forgets the statistic; false if no statistic holds the key. A statistic still
    /// deploying is forgotten at once, and its deployment stops the query once it is up. If the query cannot be
    /// stopped, the statistic stays registered so that its query is not lost track of.
    pub async fn deregister_statistic(self: &Arc<Self>, key: &Key) -> Result<bool, StatisticError> {
        let registered = match self.registry.begin_deregister(key) {
            Deregistration::Unknown => return Ok(false),
            Deregistration::Forgotten => return Ok(true),
            Deregistration::AlreadyStopping { statistic_id } => {
                return Err(StatisticError::Deregistering { statistic_id });
            }
            Deregistration::Stop(registered) => registered,
        };
        let (query_id, statistic_id) = (registered.query_id, registered.statistic_id);
        // Stopped in a task of its own, so the statistic does not stay stopping if the caller goes away.
        let service = Arc::clone(self);
        let key = key.clone();
        let stopping = tokio::spawn(async move {
            let stopped = service.submitter.stop(query_id).await;
            service
                .registry
                .end_deregister(&key, statistic_id, stopped.is_ok());
            stopped
        });
        let stopped = stopping
            .await
            .unwrap_or_else(|error| Err(error.to_string()));
        stopped.map_err(|reason| StatisticError::StopFailed {
            query_id,
            statistic_id,
            reason,
        })?;
        Ok(true)
    }

    pub fn registered_statistics(&self) -> usize {
        self.registry.len()
    }

    pub async fn get_statistics(
        &self,
        keys: &[Key],
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Option<f64>, StatisticError> {
        let first = keys.first().ok_or(StatisticError::UnknownKey)?;
        let metric = first.metric;
        if keys.iter().any(|key| key.metric != metric) {
            return Err(StatisticError::Generate(GenerateError::InvalidRequest(
                "a probe decodes one metric at a time, but the request mixes several".into(),
            )));
        }
        let targets = keys
            .iter()
            .map(|key| self.registry.find(key).ok_or(StatisticError::UnknownKey))
            .collect::<Result<Vec<_>, _>>()?;
        let aggregation = query_gen::aggregation(metric)?;

        let domain = first.domain.clone();
        let payload_type = self
            .scalar_payload_type(metric, &domain)
            .await
            .ok_or_else(|| {
                StatisticError::Generate(GenerateError::NotImplemented(format!(
                    "probing a {metric:?} statistic needs a declared payload type, which this deployment cannot resolve"
                )))
            })?;
        let payload = vec![(VALUE.to_string(), payload_type)];

        self.run_probe(&targets, aggregation, &payload, start_ts, end_ts)
            .await
    }

    pub fn on_report(&self, report: Report) {
        let claimed = {
            let mut pending = self.pending.lock().expect("pending probes poisoned");
            match pending.get_mut(&report.statistic_id) {
                Some(entry) if entry.probe_id == report.probe_id => {
                    entry.sum += report.value;
                    entry.reports += 1;
                    true
                }
                _ => false,
            }
        };
        if claimed {
            self.reported.notify_waiters();
        }
        // A probe's answer belongs to the get that deployed the probe; watchers receive what the statistic's own query
        // reports while its condition holds.
        if report.probe_id == COLLECTION_PROBE_ID {
            self.registry.dispatch(report);
        }
    }

    /// Reads each target statistic back through a probe query of its own, placed on the worker that holds it.
    async fn run_probe(
        &self,
        targets: &[Registered],
        aggregation: &str,
        payload: &[(String, String)],
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Option<f64>, StatisticError> {
        let address = self.address()?;
        let probe_id = self.next_probe_id.fetch_add(1, Ordering::Relaxed);

        let mut probes = Vec::with_capacity(targets.len());
        for target in targets {
            match self
                .deploy_probe(target, aggregation, payload, address, probe_id)
                .await
            {
                Ok(probe) => probes.push(probe),
                Err(error) => {
                    self.stop_probes(&probes).await;
                    return Err(error);
                }
            }
        }

        {
            let mut pending = self.pending.lock().expect("pending probes poisoned");
            for probe in &probes {
                pending.insert(
                    probe.statistic_id,
                    PendingProbe {
                        probe_id,
                        ..PendingProbe::default()
                    },
                );
            }
        }

        futures::future::join_all(probes.iter().map(|probe| async move {
            let deadline = tokio::time::Instant::now() + self.probe_timeout;
            while tokio::time::Instant::now() < deadline {
                if self
                    .impulse
                    .request(&probe.source, probe.statistic_id, start_ts, end_ts)
                    .await
                    .is_ok()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            tracing::warn!(
                statistic_id = probe.statistic_id,
                source = %probe.source,
                "the probe source never accepted a request"
            );
        }))
        .await;

        let statistic_ids: Vec<u64> = probes.iter().map(|probe| probe.statistic_id).collect();
        let all_reported = tokio::time::timeout(self.probe_timeout, async {
            loop {
                let notified = self.reported.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.all_reported(&statistic_ids) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .is_ok();

        if all_reported {
            tokio::time::sleep(self.probe_settle).await;
        }

        let mut sum = 0.0;
        {
            let mut pending = self.pending.lock().expect("pending probes poisoned");
            for statistic_id in &statistic_ids {
                if let Some(entry) = pending.remove(statistic_id) {
                    sum += entry.sum;
                    if entry.reports == 0 {
                        tracing::warn!(statistic_id, "no report arrived");
                    }
                }
            }
        }

        self.stop_probes(&probes).await;
        Ok(all_reported.then_some(sum))
    }

    async fn deploy_probe(
        &self,
        target: &Registered,
        aggregation: &str,
        payload: &[(String, String)],
        address: &str,
        probe_id: u64,
    ) -> Result<ProbeQuery, StatisticError> {
        for _ in 0..PROBE_SOURCE_PORT_ATTEMPTS {
            let port = PROBE_SOURCE_PORT_BASE
                + self.next_probe_port.fetch_add(1, Ordering::Relaxed) % PROBE_SOURCE_PORT_RANGE;
            let sql = query_gen::probe_query(
                target.statistic_id,
                aggregation,
                payload,
                port,
                address,
                probe_id,
                &target.worker,
            )?;
            match self.submitter.submit(sql).await {
                Ok(deployment) => {
                    return Ok(ProbeQuery {
                        statistic_id: target.statistic_id,
                        query_id: deployment.query_id,
                        source: NetworkAddr {
                            host: target.worker.host.clone(),
                            port,
                        },
                    });
                }
                Err(error) => {
                    tracing::warn!(port, worker = %target.worker, %error, "probe query could not be deployed");
                }
            }
        }
        Err(StatisticError::ProbeNotDeployed)
    }

    async fn stop_probes(&self, probes: &[ProbeQuery]) {
        for probe in probes {
            if let Err(error) = self.submitter.stop(probe.query_id).await {
                tracing::warn!(probe_query_id = probe.query_id, %error, "the probe query could not be stopped");
            }
        }
    }

    fn all_reported(&self, statistic_ids: &[u64]) -> bool {
        let pending = self.pending.lock().expect("pending probes poisoned");
        statistic_ids.iter().all(|id| {
            pending
                .get(id)
                .map(|entry| entry.reports > 0)
                .unwrap_or(false)
        })
    }
}
