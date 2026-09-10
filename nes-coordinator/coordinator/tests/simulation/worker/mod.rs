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
//! Implements the same RPC surface a real worker exposes (start, stop,
//! per-fragment and aggregate status, plus health-check) against an
//! in-memory map of fragments. State transitions are
//! deterministic given a seed; each handler carries `buggify` points
//! so the simulator can exercise the controller's retry, reconnect, and
//! crash-recovery paths without standing up real worker processes.

use madsim::buggify::buggify;
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
use worker_rpc_service::worker_status_response::{ActiveQuery, TerminatedQuery};
use worker_rpc_service::{
    QueryMetrics, QueryState, QueryStatusReply, QueryStatusRequest, StartQueryReply,
    StartQueryRequest, StopQueryRequest, VersionResponse, WorkerStatusRequest,
    WorkerStatusResponse,
};

pub use health_proto::health_server::HealthServer;

type QueryFragmentId = i64;

fn fragment_id(id: QueryFragmentId) -> Option<SerializableQueryId> {
    Some(SerializableQueryId::from_fragment_id(id))
}

fn extract_id(req_id: &Option<SerializableQueryId>) -> QueryFragmentId {
    req_id
        .as_ref()
        .and_then(SerializableQueryId::fragment_id)
        .unwrap_or(0)
}

struct QueryQueryFragment {
    state: i32,
    started_at: u64,
    running_at: u64,
    stopped_at: u64,
    error: Option<worker_rpc_service::Error>,
}

impl QueryQueryFragment {
    fn new() -> Self {
        Self {
            state: QueryState::Registered as i32,
            started_at: 0,
            running_at: 0,
            stopped_at: 0,
            error: None,
        }
    }

    fn record_transition(&mut self, state: i32, error: Option<worker_rpc_service::Error>) {
        let now = current_timestamp_ms();
        self.state = state;
        if state == QueryState::Started as i32 {
            self.started_at = now;
        } else if state == QueryState::Running as i32 {
            self.running_at = now;
        } else if state == QueryState::Stopped as i32 || state == QueryState::Failed as i32 {
            self.stopped_at = now;
            self.error = error;
        }
    }

    fn metrics(&self) -> QueryMetrics {
        QueryMetrics {
            start_unix_time_in_ms: if self.started_at > 0 {
                Some(self.started_at)
            } else {
                None
            },
            running_unix_time_in_ms: if self.running_at > 0 {
                Some(self.running_at)
            } else {
                None
            },
            stop_unix_time_in_ms: if self.stopped_at > 0 {
                Some(self.stopped_at)
            } else {
                None
            },
            error: self.error.clone(),
        }
    }
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
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
    fragments: Arc<RwLock<HashMap<QueryFragmentId, QueryQueryFragment>>>,
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
    /// Registers and starts in one call, so the fragment is created here rather
    /// than by a preceding RPC, and its id comes from the plan. Both fault
    /// injections the two former handlers carried are kept: a rejected call and
    /// a slow one.
    #[instrument(skip(self, request))]
    async fn start_query(
        &self,
        request: Request<StartQueryRequest>,
    ) -> Result<Response<StartQueryReply>, Status> {
        if buggify() {
            return Err(Status::unavailable("buggify"));
        }
        if buggify() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let query_id = request
            .get_ref()
            .query_plan
            .as_ref()
            .and_then(|plan| plan.query_id.as_ref())
            .and_then(SerializableQueryId::fragment_id)
            .unwrap_or(0);

        {
            let mut fragments = self.fragments.write().unwrap();
            let fragment = fragments
                .entry(query_id)
                .or_insert_with(QueryQueryFragment::new);

            if fragment.state != QueryState::Registered as i32 {
                debug!("{query_id} not in Registered state, ignoring");
                return Ok(Response::new(StartQueryReply {
                    query_id: fragment_id(query_id),
                }));
            }

            fragment.record_transition(QueryState::Started as i32, None);
            debug!("started fragment {query_id}");
        }

        let fragments = self.fragments.clone();
        tokio::spawn(async move {
            let mut map = fragments.write().unwrap();
            if let Some(fragment) = map.get_mut(&query_id)
                && fragment.state == QueryState::Started as i32
            {
                fragment.record_transition(QueryState::Running as i32, None);
                debug!("fragment {query_id} running");
            }
        });

        Ok(Response::new(StartQueryReply {
            query_id: fragment_id(query_id),
        }))
    }

    #[instrument(skip(self), fields(query_id = extract_id(&request.get_ref().query_id)))]
    async fn stop_query(&self, request: Request<StopQueryRequest>) -> Result<Response<()>, Status> {
        if buggify() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let id = extract_id(&request.get_ref().query_id);

        let mut fragments = self.fragments.write().unwrap();
        let Some(fragment) = fragments.get_mut(&id) else {
            debug!("fragment {id} not found, ignoring");
            return Err(Status::not_found(format!("fragment {id} not found")));
        };

        if fragment.state == QueryState::Stopped as i32
            || fragment.state == QueryState::Failed as i32
        {
            debug!("fragment {id} already terminal, ignoring");
            return Ok(Response::new(()));
        }

        fragment.record_transition(QueryState::Stopped as i32, None);
        debug!("stopped fragment {id}");
        Ok(Response::new(()))
    }

    #[instrument(skip(self), fields(query_id = extract_id(&request.get_ref().query_id)))]
    async fn request_query_status(
        &self,
        request: Request<QueryStatusRequest>,
    ) -> Result<Response<QueryStatusReply>, Status> {
        if buggify() {
            return Err(Status::unavailable("buggify"));
        }
        let id = extract_id(&request.get_ref().query_id);
        let fragments = self.fragments.read().unwrap();
        let fragment = fragments
            .get(&id)
            .ok_or_else(|| Status::not_found(format!("fragment {id} not found")))?;

        Ok(Response::new(QueryStatusReply {
            query_id: fragment_id(id),
            state: fragment.state,
            metrics: Some(fragment.metrics()),
        }))
    }

    #[instrument(skip(self))]
    async fn request_status(
        &self,
        request: Request<WorkerStatusRequest>,
    ) -> Result<Response<WorkerStatusResponse>, Status> {
        let after = request.get_ref().after_unix_timestamp_in_milli_seconds;
        let fragments = self.fragments.read().unwrap();

        let mut active_queries = Vec::new();
        let mut terminated_queries = Vec::new();

        for (&id, fragment) in fragments.iter() {
            let state = fragment.state;
            if state == QueryState::Registered as i32 {
                continue;
            }
            if (state == QueryState::Started as i32 || state == QueryState::Running as i32)
                && fragment.started_at >= after
            {
                active_queries.push(ActiveQuery {
                    query_id: fragment_id(id),
                    started_unix_timestamp_in_milli_seconds: Some(fragment.started_at),
                });
            } else if (state == QueryState::Stopped as i32 || state == QueryState::Failed as i32)
                && fragment.stopped_at >= after
            {
                terminated_queries.push(TerminatedQuery {
                    query_id: fragment_id(id),
                    started_unix_timestamp_in_milli_seconds: Some(fragment.started_at),
                    terminated_unix_timestamp_in_milli_seconds: fragment.stopped_at,
                    error: fragment.error.clone(),
                });
            }
        }

        Ok(Response::new(WorkerStatusResponse {
            after_unix_timestamp_in_milli_seconds: after,
            until_unix_timestamp_in_milli_seconds: current_timestamp_ms(),
            active_queries,
            terminated_queries,
        }))
    }

    /// A real worker reports the version of the binary it runs. The simulation has no binary and
    /// nothing here asserts on the answer, so a fixed string is all this has to supply.
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
