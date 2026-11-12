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

//! Fake gRPC worker used by the simulation.
//!
//! Implements the RPC surface of a real worker (start, stop, per-fragment and aggregate status, health check)
//! against an in-memory map of fragments.
//! Like a real worker, it accepts a start or a stop in the handler and applies it later in a background task,
//! so the controller can read a fragment's status between the reply and the state change.
//! State transitions are deterministic given a seed.
//! The handlers have buggify points so the simulator exercises the controller's retry, reconnect,
//! and crash-recovery paths without real worker processes.

use madsim::buggify::buggify;
use madsim::rand::{Rng, thread_rng};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};
use tracing::{debug, instrument};

use controller::remote::health_proto;
use controller::remote::worker_rpc_service;

use worker_rpc_service::nes::SerializableQueryId;
use worker_rpc_service::worker_rpc_service_server::WorkerRpcService;
pub use worker_rpc_service::worker_rpc_service_server::WorkerRpcServiceServer;
use worker_rpc_service::worker_status_response::ActiveQuery;
use worker_rpc_service::{
    QueryMetrics, QueryState, QueryStatusReply, QueryStatusRequest, StartQueryReply,
    StartQueryRequest, StopQueryRequest, VersionResponse, WorkerStatusRequest,
    WorkerStatusResponse,
};

pub use health_proto::health_server::HealthServer;

type QueryFragmentId = i64;
type Fragments = Arc<RwLock<HashMap<QueryFragmentId, QueryFragment>>>;

/// Upper bounds of the waits between a fragment's state changes.
/// The simulator only switches tasks at an await,
/// so without a wait, a state change would happen at the same instant as the reply.
/// The start bound covers two of the controller's 5 s status reads.
const MAX_START_DELAY: Duration = Duration::from_secs(10);
const MAX_RUNNING_DELAY: Duration = Duration::from_secs(2);
const MAX_STOP_DELAY: Duration = Duration::from_secs(2);

fn random_delay(max: Duration) -> Duration {
    max.mul_f64(thread_rng().gen_range(0.0..1.0))
}

/// How long a slow handler waits.
/// The upper part of the range exceeds the controller's 2 s deadline for one call,
/// so the controller repeats a call that may already have taken effect.
fn slow_reply_delay() -> Duration {
    Duration::from_millis(thread_rng().gen_range(1000..=4000))
}

fn extract_fragment_id(req_id: &Option<SerializableQueryId>) -> QueryFragmentId {
    req_id
        .as_ref()
        .and_then(SerializableQueryId::fragment_id)
        .expect("request carries a fragment id")
}

/// A fragment exists from its start call on. It never fails on its own.
struct QueryFragment {
    state: QueryState,
    started_at: Option<u64>,
    running_at: Option<u64>,
    stopped_at: Option<u64>,
}

impl QueryFragment {
    fn registered() -> Self {
        Self {
            state: QueryState::Registered,
            started_at: None,
            running_at: None,
            stopped_at: None,
        }
    }

    fn get_metrics(&self) -> QueryMetrics {
        QueryMetrics {
            start_unix_time_in_ms: self.started_at,
            running_unix_time_in_ms: self.running_at,
            stop_unix_time_in_ms: self.stopped_at,
            error: None,
        }
    }
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

/// Moves a fragment from one state to the next and records the time of the change.
/// A fragment in any other state is left alone, so a stop that happened in the meantime is kept.
fn advance(fragments: &Fragments, id: QueryFragmentId, from: QueryState, to: QueryState) -> bool {
    let mut map = fragments.write().unwrap();
    let Some(fragment) = map.get_mut(&id) else {
        return false;
    };
    if fragment.state != from {
        return false;
    }
    fragment.state = to;
    let now = Some(current_timestamp_ms());
    match to {
        QueryState::Started => fragment.started_at = now,
        QueryState::Running => fragment.running_at = now,
        _ => {}
    }
    debug!("fragment {id} is {to:?}");
    true
}

pub struct HealthServiceImpl;

#[tonic::async_trait]
impl health_proto::health_server::Health for HealthServiceImpl {
    async fn check(
        &self,
        _request: Request<health_proto::HealthCheckRequest>,
    ) -> Result<Response<health_proto::HealthCheckResponse>, Status> {
        if buggify() {
            return Err(Status::unavailable("unavailable"));
        }
        Ok(Response::new(health_proto::HealthCheckResponse {
            status: health_proto::health_check_response::ServingStatus::Serving as i32,
        }))
    }
}

pub struct SingleNodeWorker {
    fragments: Fragments,
}

impl SingleNodeWorker {
    pub fn new() -> Self {
        Self {
            fragments: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl WorkerRpcService for SingleNodeWorker {
    /// Registers and starts in one call, so the fragment is created here and its id comes from the plan.
    /// Three faults are injected: a rejected call, a slow call before the fragment is registered,
    /// and a slow reply after it is registered.
    #[instrument(skip(self, request))]
    async fn start_query(
        &self,
        request: Request<StartQueryRequest>,
    ) -> Result<Response<StartQueryReply>, Status> {
        if buggify() {
            return Err(Status::unavailable("buggify"));
        }
        if buggify() {
            tokio::time::sleep(slow_reply_delay()).await;
        }
        let query_id = request
            .get_ref()
            .query_plan
            .as_ref()
            .and_then(|plan| plan.query_id.as_ref())
            .and_then(SerializableQueryId::fragment_id)
            .expect("start request carries a fragment id");

        {
            let mut fragments = self.fragments.write().unwrap();
            // A repeated start is rejected and changes nothing, whatever the fragment's state,
            // so a late repeat cannot restart a stopped fragment.
            if fragments.contains_key(&query_id) {
                debug!("fragment {query_id} already exists, rejecting");
                return Err(Status::already_exists(format!(
                    "fragment {query_id} already registered"
                )));
            }

            fragments.insert(query_id, QueryFragment::registered());
            debug!("registered fragment {query_id}");
        }

        let fragments = self.fragments.clone();
        let start_delay = random_delay(MAX_START_DELAY);
        let running_delay = random_delay(MAX_RUNNING_DELAY);
        tokio::spawn(async move {
            tokio::time::sleep(start_delay).await;
            if !advance(
                &fragments,
                query_id,
                QueryState::Registered,
                QueryState::Started,
            ) {
                return;
            }
            tokio::time::sleep(running_delay).await;
            advance(
                &fragments,
                query_id,
                QueryState::Started,
                QueryState::Running,
            );
        });

        if buggify() {
            tokio::time::sleep(slow_reply_delay()).await;
        }

        Ok(Response::new(StartQueryReply {
            query_id: Some(SerializableQueryId::from_fragment_id(query_id)),
        }))
    }

    #[instrument(skip(self), fields(query_id = extract_fragment_id(&request.get_ref().query_id)))]
    async fn stop_query(&self, request: Request<StopQueryRequest>) -> Result<Response<()>, Status> {
        if buggify() {
            tokio::time::sleep(slow_reply_delay()).await;
        }
        let id = extract_fragment_id(&request.get_ref().query_id);

        // An unknown fragment is reported, so the caller learns that nothing was stopped
        // and that a start with an unknown outcome may still take effect.
        // A repeated stop is accepted.
        let state = self
            .fragments
            .read()
            .unwrap()
            .get(&id)
            .map(|fragment| fragment.state);
        match state {
            None => return Err(Status::not_found(format!("fragment {id} not found"))),
            Some(QueryState::Stopped) => {
                debug!("fragment {id} already stopped, ignoring");
                return Ok(Response::new(()));
            }
            Some(_) => {}
        }

        // A real worker accepts a stop that its engine can still drop,
        // for example while the fragment is compiling, so the fragment keeps running after the reply.
        if buggify() {
            debug!("buggify: dropping the stop of fragment {id}");
            return Ok(Response::new(()));
        }

        let fragments = self.fragments.clone();
        let stop_delay = random_delay(MAX_STOP_DELAY);
        tokio::spawn(async move {
            tokio::time::sleep(stop_delay).await;
            let mut map = fragments.write().unwrap();
            if let Some(fragment) = map.get_mut(&id)
                && fragment.state != QueryState::Stopped
            {
                fragment.state = QueryState::Stopped;
                fragment.stopped_at = Some(current_timestamp_ms());
                debug!("stopped fragment {id}");
            }
        });

        if buggify() {
            tokio::time::sleep(slow_reply_delay()).await;
        }
        Ok(Response::new(()))
    }

    #[instrument(skip(self), fields(query_id = extract_fragment_id(&request.get_ref().query_id)))]
    async fn request_query_status(
        &self,
        request: Request<QueryStatusRequest>,
    ) -> Result<Response<QueryStatusReply>, Status> {
        if buggify() {
            return Err(Status::unavailable("buggify"));
        }
        if buggify() {
            tokio::time::sleep(slow_reply_delay()).await;
        }
        let id = extract_fragment_id(&request.get_ref().query_id);
        let fragments = self.fragments.read().unwrap();
        let fragment = fragments
            .get(&id)
            .ok_or_else(|| Status::not_found(format!("fragment {id} not found")))?;
        debug!("fragment {id} reported as {:?}", fragment.state);

        Ok(Response::new(QueryStatusReply {
            query_id: Some(SerializableQueryId::from_fragment_id(id)),
            state: fragment.state as i32,
            metrics: Some(fragment.get_metrics()),
        }))
    }

    #[instrument(skip(self))]
    async fn request_status(
        &self,
        request: Request<WorkerStatusRequest>,
    ) -> Result<Response<WorkerStatusResponse>, Status> {
        if buggify() {
            return Err(Status::unavailable("buggify"));
        }
        if buggify() {
            tokio::time::sleep(slow_reply_delay()).await;
        }
        let after = request.get_ref().after_unix_timestamp_in_milli_seconds;
        let fragments = self.fragments.read().unwrap();

        let mut active_queries = Vec::new();

        // Like the real worker: a registered fragment is not listed,
        // a started fragment is filtered by its start time and listed without a time,
        // a running one is filtered by and listed with the time at which it began to run.
        for (&id, fragment) in fragments.iter() {
            let since = match fragment.state {
                QueryState::Started if fragment.started_at >= Some(after) => None,
                QueryState::Running if fragment.running_at >= Some(after) => fragment.running_at,
                _ => continue,
            };
            active_queries.push(ActiveQuery {
                query_id: Some(SerializableQueryId::from_fragment_id(id)),
                started_unix_timestamp_in_milli_seconds: since,
            });
        }

        // Only the active fragments are read by the simulation.
        Ok(Response::new(WorkerStatusResponse {
            after_unix_timestamp_in_milli_seconds: after,
            until_unix_timestamp_in_milli_seconds: current_timestamp_ms(),
            active_queries,
            terminated_queries: Vec::new(),
        }))
    }

    /// A real worker reports its binary's version.
    /// The simulation has no binary, so a fixed string is sufficient.
    #[instrument(skip(self, _request))]
    async fn request_version(
        &self,
        _request: Request<()>,
    ) -> Result<Response<VersionResponse>, Status> {
        Ok(Response::new(VersionResponse {
            version: "simulation".to_string(),
        }))
    }
}
