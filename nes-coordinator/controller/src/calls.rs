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

//! The calls that a worker loop makes to its worker.
//! Each start and stop runs as a task of its own, so a long compile or a slow stop delays neither the status requests nor the other calls.

use crate::config::MAX_FAILED_STARTS;
use crate::join_set::JoinSet;
use crate::remote::worker_rpc_service::WorkerStatusResponse;
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use model::database::Database;
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{self, DesiredQueryFragmentState, QueryFragmentError};
use prost::Message;
use sea_orm::EntityTrait;
use std::collections::HashSet;
use std::panic;
use std::time::Duration;
use tokio_retry2::strategy::FixedInterval;
use tokio_retry2::{Retry, RetryError};
use tracing::warn;

/// A failed worker call, classified by what the loop does next.
pub(crate) enum CallError {
    /// The worker already accepted a start for the fragment's id.
    AlreadyExists,
    /// Repeating the call may succeed.
    Transient(QueryFragmentError),
    /// Repeating the call cannot succeed, so a failed start fails the fragment.
    Permanent(QueryFragmentError),
}

pub(crate) trait Transport: Clone + Send + Sync + 'static {
    fn start(
        &self,
        plan: SerializableQueryPlan,
    ) -> impl Future<Output = Result<(), CallError>> + Send;

    fn stop(&self, id: QueryFragmentId) -> impl Future<Output = Result<(), CallError>> + Send;

    /// Lists every active fragment, and the fragments that terminated at or after `after`, in the worker's milliseconds.
    fn get_status(
        &self,
        after: u64,
    ) -> impl Future<Output = anyhow::Result<WorkerStatusResponse>> + Send;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Call {
    Start(QueryFragmentId),
    Stop(QueryFragmentId),
}

/// The calls to one worker that have not returned yet.
pub(crate) struct InFlightCalls {
    db: Database,
    retry_delay: Duration,
    calls: HashSet<Call>,
    tasks: JoinSet<(Call, Result<(), CallError>)>,
}

impl InFlightCalls {
    pub(crate) fn new(db: Database, retry_delay: Duration) -> Self {
        Self {
            db,
            retry_delay,
            calls: HashSet::new(),
            tasks: JoinSet::new(),
        }
    }

    /// Skips a call that is still in flight, so a fragment is never started twice, or stopped twice, in parallel.
    pub(crate) fn spawn<T: Transport>(&mut self, transport: &T, call: Call) {
        if !self.calls.insert(call) {
            return;
        }
        let (transport, db) = (transport.clone(), self.db.clone());
        let delays = FixedInterval::new(self.retry_delay).take(MAX_FAILED_STARTS - 1);
        self.tasks.spawn(async move {
            let result = match call {
                // Repeats a transient failure until the start succeeds, fails for good, or the row no longer wants the fragment running.
                Call::Start(id) => Retry::spawn(delays, || try_start(&transport, &db, id)).await,
                Call::Stop(id) => transport.stop(id).await,
            };
            (call, result)
        });
    }

    /// Returns `None` while no call is in flight.
    /// A cancelled wait leaves every call in flight.
    pub(crate) async fn join_next(&mut self) -> Option<(Call, Result<(), CallError>)> {
        let joined = self.tasks.join_next().await?;
        // A call task is never aborted, so it ends with its result or a panic, which is raised again here.
        let (call, result) = joined.unwrap_or_else(|err| panic::resume_unwind(err.into_panic()));
        self.calls.remove(&call);
        Some((call, result))
    }
}

async fn try_start<T: Transport>(
    transport: &T,
    db: &Database,
    id: QueryFragmentId,
) -> Result<(), RetryError<CallError>> {
    let started = async {
        let row = query_fragment::Entity::find_by_id(id)
            .one(db)
            .await
            .map_err(|err| {
                CallError::Transient(QueryFragmentError::Transport {
                    msg: err.to_string(),
                })
            })?;
        let Some(row) = row.filter(|row| {
            row.desired_state == DesiredQueryFragmentState::Completed
                && !row.current_state.is_terminal()
        }) else {
            return Ok(());
        };
        let mut plan = SerializableQueryPlan::decode(row.plan.as_slice()).map_err(|err| {
            CallError::Permanent(QueryFragmentError::Internal(CodedError::new(
                ErrorCode::UnknownException,
                format!("failed to decode stored query plan: {err}"),
            )))
        })?;
        // The stored plan has no id, and the worker identifies the plan by the fragment's id.
        plan.query_id = Some(SerializableQueryId::from_fragment_id(*id));
        transport.start(plan).await
    };
    started.await.map_err(|err| match err {
        CallError::Transient(err) => {
            warn!(fragment = %id, error = %err, "start failed");
            RetryError::transient(CallError::Transient(err))
        }
        err => RetryError::permanent(err),
    })
}
