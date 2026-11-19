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
//! Implements the RPC surface of a real worker (start, stop, per-fragment and aggregate status)
//! against an in-memory map of fragments.
//! Like a real worker, it accepts a start or a stop in the handler and applies it later in a background task,
//! so the controller can read a fragment's status between the reply and the state change.
//! State transitions are deterministic given a seed.
//! The handlers have buggify points so the simulator exercises the controller's retry, reconnect,
//! and crash-recovery paths without real worker processes.

use controller::worker_client::remote::worker_rpc_service::nes::SerializableQueryId;
use controller::worker_client::remote::worker_rpc_service::worker_rpc_service_server::WorkerRpcService;
pub use controller::worker_client::remote::worker_rpc_service::worker_rpc_service_server::WorkerRpcServiceServer;
use controller::worker_client::remote::worker_rpc_service::worker_status_response::{
    ActiveQuery, TerminatedQuery,
};
use controller::worker_client::remote::worker_rpc_service::{
    QueryMetrics, QueryState, QueryStatusReply, QueryStatusRequest, StartQueryReply,
    StartQueryRequest, StopQueryRequest, VersionResponse, WorkerStatusRequest,
    WorkerStatusResponse,
};
use madsim::buggify::buggify;
use madsim::rand::{Rng, thread_rng};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};
use tracing::{debug, instrument};

type Fragments = Arc<RwLock<HashMap<i64, Fragment>>>;

// Upper bounds of the waits between a fragment's state changes.
// The simulator only switches tasks at an await, so without a wait, a state change would happen at the same instant as the reply.
// The start bound covers two of the controller's status reads of a fragment.
const MAX_START_DELAY: Duration = Duration::from_secs(10);
const MAX_RUNNING_DELAY: Duration = Duration::from_secs(2);
const MAX_STOP_DELAY: Duration = Duration::from_secs(2);

fn pick_delay(max: Duration) -> Duration {
    max.mul_f64(thread_rng().gen_range(0.0..1.0))
}

/// How long a slow handler waits.
/// The upper part of the range exceeds the controller's deadline for one call,
/// so the controller repeats a call that may already have taken effect.
fn pick_slow_reply_delay() -> Duration {
    Duration::from_millis(thread_rng().gen_range(1000..=4000))
}

fn extract_fragment_id(req_id: Option<&SerializableQueryId>) -> i64 {
    req_id
        .and_then(SerializableQueryId::get_fragment_id)
        .expect("the request has a fragment id")
}

/// A fragment exists from its start call on.
/// It never fails on its own.
struct Fragment {
    state: QueryState,
    started_at: Option<u64>,
    running_at: Option<u64>,
    stopped_at: Option<u64>,
}

impl Fragment {
    const fn registered() -> Self {
        Self {
            state: QueryState::Registered,
            started_at: None,
            running_at: None,
            stopped_at: None,
        }
    }

    const fn get_metrics(&self) -> QueryMetrics {
        QueryMetrics {
            start_unix_time_in_ms: self.started_at,
            running_unix_time_in_ms: self.running_at,
            stop_unix_time_in_ms: self.stopped_at,
            error: None,
        }
    }

    fn enter(&mut self, state: QueryState) {
        self.state = state;
        let now = Some(current_timestamp_ms());
        match state {
            QueryState::Started => self.started_at = now,
            QueryState::Running => self.running_at = now,
            QueryState::Stopped => self.stopped_at = now,
            _ => {}
        }
    }
}

fn current_timestamp_ms() -> u64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX)
}

/// Moves the fragment to `to` if it is in one of the `from` states, records the time of the change, and returns whether it moved.
/// A fragment in any other state stays as it is, so a stop that happened in the meantime is kept.
fn advance(fragments: &Fragments, id: i64, from: &[QueryState], to: QueryState) -> bool {
    let mut map = fragments.write().unwrap();
    let Some(fragment) = map
        .get_mut(&id)
        .filter(|fragment| from.contains(&fragment.state))
    else {
        return false;
    };
    fragment.enter(to);
    debug!("fragment {id} is {to:?}");
    true
}

/// The fragments are in memory, so a restarted worker has none.
pub struct WorkerService {
    fragments: Fragments,
}

impl WorkerService {
    pub fn new() -> Self {
        Self {
            fragments: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl WorkerRpcService for WorkerService {
    /// Registers and starts in one call, so the fragment is created here and its id comes from the plan.
    /// The handler injects three faults: a rejected call, a slow call before registration, and a slow reply after it.
    #[instrument(skip(self, request))]
    async fn start_query(
        &self,
        request: Request<StartQueryRequest>,
    ) -> Result<Response<StartQueryReply>, Status> {
        if buggify() {
            return Err(Status::unavailable("buggify"));
        }
        if buggify() {
            tokio::time::sleep(pick_slow_reply_delay()).await;
        }
        let id = extract_fragment_id(
            request
                .get_ref()
                .query_plan
                .as_ref()
                .and_then(|plan| plan.query_id.as_ref()),
        );

        {
            let mut fragments = self.fragments.write().unwrap();
            // A repeated start is rejected and changes nothing, whatever the fragment's state,
            // so a late repeat cannot restart a stopped fragment.
            if fragments.contains_key(&id) {
                debug!("fragment {id} already exists, rejecting");
                return Err(Status::already_exists(format!(
                    "fragment {id} already registered"
                )));
            }

            fragments.insert(id, Fragment::registered());
            debug!("registered fragment {id}");
        }

        let fragments = self.fragments.clone();
        let start_delay = pick_delay(MAX_START_DELAY);
        let running_delay = pick_delay(MAX_RUNNING_DELAY);
        tokio::spawn(async move {
            tokio::time::sleep(start_delay).await;
            if !advance(
                &fragments,
                id,
                &[QueryState::Registered],
                QueryState::Started,
            ) {
                return;
            }
            tokio::time::sleep(running_delay).await;
            advance(&fragments, id, &[QueryState::Started], QueryState::Running);
        });

        if buggify() {
            tokio::time::sleep(pick_slow_reply_delay()).await;
        }

        Ok(Response::new(StartQueryReply {
            query_id: Some(SerializableQueryId::from_fragment_id(id)),
        }))
    }

    #[instrument(skip(self), fields(query_id = extract_fragment_id(request.get_ref().query_id.as_ref())))]
    async fn stop_query(&self, request: Request<StopQueryRequest>) -> Result<Response<()>, Status> {
        if buggify() {
            tokio::time::sleep(pick_slow_reply_delay()).await;
        }
        let id = extract_fragment_id(request.get_ref().query_id.as_ref());

        // An unknown fragment is reported,
        // so the reply tells the caller that nothing was stopped and that a start with an unknown outcome may still take effect.
        // A repeated stop is accepted.
        let state = self.fragments.read().unwrap().get(&id).map(|f| f.state);
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
        let stop_delay = pick_delay(MAX_STOP_DELAY);
        tokio::spawn(async move {
            tokio::time::sleep(stop_delay).await;
            advance(
                &fragments,
                id,
                &[
                    QueryState::Registered,
                    QueryState::Started,
                    QueryState::Running,
                ],
                QueryState::Stopped,
            );
        });

        if buggify() {
            tokio::time::sleep(pick_slow_reply_delay()).await;
        }
        Ok(Response::new(()))
    }

    #[instrument(skip(self), fields(query_id = extract_fragment_id(request.get_ref().query_id.as_ref())))]
    async fn request_query_status(
        &self,
        request: Request<QueryStatusRequest>,
    ) -> Result<Response<QueryStatusReply>, Status> {
        if buggify() {
            return Err(Status::unavailable("buggify"));
        }
        if buggify() {
            tokio::time::sleep(pick_slow_reply_delay()).await;
        }
        let id = extract_fragment_id(request.get_ref().query_id.as_ref());
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
            tokio::time::sleep(pick_slow_reply_delay()).await;
        }
        let after = request.get_ref().after_unix_timestamp_in_milli_seconds;
        let fragments = self.fragments.read().unwrap();

        // NOTE: Like the real worker, the status lists every active fragment, registered ones included,
        // and a stopped fragment only if it stopped at or after the requested time.
        // For a request after 100: Registered is listed, Running since 90 is listed with 90,
        // Stopped at 120 is listed, and Stopped at 90 is left out.
        let mut active_queries = Vec::new();
        let mut terminated_queries = Vec::new();
        for (&id, fragment) in fragments.iter() {
            let query_id = Some(SerializableQueryId::from_fragment_id(id));
            match fragment.state {
                QueryState::Registered | QueryState::Started | QueryState::Running => {
                    active_queries.push(ActiveQuery {
                        query_id,
                        started_unix_timestamp_in_milli_seconds: fragment.running_at,
                    });
                }
                QueryState::Stopped => {
                    if let Some(stop) = fragment.stopped_at.filter(|&stop| stop >= after) {
                        terminated_queries.push(TerminatedQuery {
                            query_id,
                            started_unix_timestamp_in_milli_seconds: fragment.running_at,
                            terminated_unix_timestamp_in_milli_seconds: stop,
                            error: None,
                        });
                    }
                }
                _ => {}
            }
        }
        Ok(Response::new(WorkerStatusResponse {
            after_unix_timestamp_in_milli_seconds: after,
            until_unix_timestamp_in_milli_seconds: current_timestamp_ms(),
            active_queries,
            terminated_queries,
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
