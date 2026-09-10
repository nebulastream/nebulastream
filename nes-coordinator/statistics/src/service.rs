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

use crate::query_gen::{self, CollectRequest, GenerateError, VALUE};
use crate::registry::{CollectionDomain, Key, Metric, Registry, Report, Trigger};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

pub const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_PROBE_SETTLE: Duration = Duration::from_millis(500);
pub const PROBE_SOURCE_PORT_BASE: u16 = 10000;
pub const PROBE_SOURCE_PORT_ATTEMPTS: u16 = 10;

#[derive(Debug, thiserror::Error)]
pub enum StatisticError {
    #[error(transparent)]
    Generate(#[from] GenerateError),
    #[error("statistic key is not registered")]
    UnknownKey,
    #[error("the statistic service has no address; start its gRPC server first")]
    NoServiceAddress,
    #[error("no probe query could be deployed")]
    ProbeNotDeployed,
    #[error("submitting the statistic query failed: {0}")]
    Submit(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectResult {
    pub query_id: u64,
    pub statistic_id: u64,
    pub already_existed: bool,
}

#[async_trait::async_trait]
pub trait QuerySubmitter: Send + Sync {
    async fn submit(&self, sql: String) -> Result<u64, String>;
    async fn stop(&self, query_id: u64) -> Result<(), String>;
}

#[async_trait::async_trait]
pub trait SchemaResolver: Send + Sync {
    async fn field_type(&self, logical_source: &str, field: &str) -> Option<String>;
}

#[async_trait::async_trait]
pub trait ProbeImpulse: Send + Sync {
    async fn request(
        &self,
        port: u16,
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

pub struct StatisticService {
    registry: Registry,
    next_statistic_id: AtomicU64,
    next_probe_id: AtomicU64,
    submitter: Arc<dyn QuerySubmitter>,
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
        resolver: Arc<dyn SchemaResolver>,
        impulse: Arc<dyn ProbeImpulse>,
        service_address: String,
    ) -> Self {
        Self {
            registry: Registry::default(),
            next_statistic_id: AtomicU64::new(1),
            next_probe_id: AtomicU64::new(1),
            submitter,
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

    pub async fn collect_new_statistic(
        &self,
        request: &CollectRequest,
        trigger: Option<Trigger>,
    ) -> Result<CollectResult, StatisticError> {
        let address = self.address()?;
        let key = Self::key_of(request);

        if let Some((query_id, statistic_id)) = self.registry.find(&key) {
            if let Some(trigger) = trigger {
                self.registry.add_trigger(&key, trigger);
            }
            return Ok(CollectResult {
                query_id,
                statistic_id,
                already_existed: true,
            });
        }

        let statistic_id = self.next_statistic_id.fetch_add(1, Ordering::Relaxed);
        let payload_type = self
            .scalar_payload_type(request.metric, &request.domain)
            .await;
        let sql =
            query_gen::collection_query(request, statistic_id, &address, payload_type.as_deref())?;
        let query_id = self
            .submitter
            .submit(sql)
            .await
            .map_err(StatisticError::Submit)?;

        self.registry.register(key, query_id, statistic_id, trigger);
        Ok(CollectResult {
            query_id,
            statistic_id,
            already_existed: false,
        })
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

    pub fn add_condition_trigger(&self, key: &Key, trigger: Trigger) -> bool {
        self.registry.add_trigger(key, trigger)
    }

    pub fn deregister_statistic(&self, key: &Key) -> bool {
        self.registry.deregister(key)
    }

    pub fn registered_statistics(&self) -> usize {
        self.registry.len()
    }

    fn resolve_ids(&self, keys: &[Key]) -> Result<Vec<u64>, StatisticError> {
        keys.iter()
            .map(|key| {
                self.registry
                    .find(key)
                    .map(|(_, id)| id)
                    .ok_or(StatisticError::UnknownKey)
            })
            .collect()
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
        let statistic_ids = self.resolve_ids(keys)?;
        let blob = query_gen::blob_type(metric)?;

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

        self.run_probe(&statistic_ids, blob, &payload, start_ts, end_ts)
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
        self.registry.dispatch(report);
    }

    async fn run_probe(
        &self,
        statistic_ids: &[u64],
        blob: &str,
        payload: &[(String, String)],
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Option<f64>, StatisticError> {
        let address = self.address()?;
        let probe_id = self.next_probe_id.fetch_add(1, Ordering::Relaxed);

        let mut deployed = None;
        for attempt in 0..PROBE_SOURCE_PORT_ATTEMPTS {
            let port = PROBE_SOURCE_PORT_BASE + attempt;
            let sql =
                query_gen::probe_query(statistic_ids, blob, payload, port, address, probe_id)?;
            match self.submitter.submit(sql).await {
                Ok(query_id) => {
                    deployed = Some((port, query_id));
                    break;
                }
                Err(error) => tracing::warn!(port, %error, "probe query could not be deployed"),
            }
        }
        let Some((source_port, probe_query_id)) = deployed else {
            return Err(StatisticError::ProbeNotDeployed);
        };

        {
            let mut pending = self.pending.lock().expect("pending probes poisoned");
            for statistic_id in statistic_ids {
                pending.insert(
                    *statistic_id,
                    PendingProbe {
                        probe_id,
                        ..PendingProbe::default()
                    },
                );
            }
        }

        futures::future::join_all(statistic_ids.iter().map(|statistic_id| async move {
            let deadline = tokio::time::Instant::now() + self.probe_timeout;
            while tokio::time::Instant::now() < deadline {
                if self
                    .impulse
                    .request(source_port, *statistic_id, start_ts, end_ts)
                    .await
                    .is_ok()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            tracing::warn!(statistic_id, "the probe source never accepted a request");
        }))
        .await;

        let all_reported = tokio::time::timeout(self.probe_timeout, async {
            loop {
                let notified = self.reported.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.all_reported(statistic_ids) {
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
            for statistic_id in statistic_ids {
                if let Some(entry) = pending.remove(statistic_id) {
                    sum += entry.sum;
                    if entry.reports == 0 {
                        tracing::warn!(statistic_id, "no report arrived");
                    }
                }
            }
        }

        if let Err(error) = self.submitter.stop(probe_query_id).await {
            tracing::warn!(probe_query_id, %error, "the probe query could not be stopped");
        }

        Ok(all_reported.then_some(sum))
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
