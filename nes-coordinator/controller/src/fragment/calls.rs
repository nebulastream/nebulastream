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
//! Each start and stop runs as a task of its own,
//! so a long compile or a slow stop delays neither the status requests nor the other calls.

use crate::config::MAX_FAILED_STARTS;
use crate::join_set::JoinSet;
use crate::worker_client::remote::worker_rpc_service::nes::{
    SerializableQueryId, SerializableQueryPlan,
};
use crate::worker_client::{CallError, WorkerClient};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Call {
    Start(QueryFragmentId),
    Stop(QueryFragmentId),
}

/// A runtime that shut down ended a call before the call returned.
#[derive(Debug)]
pub(crate) struct Cancelled;

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
    /// The result of a start is final, because a failed start is already repeated.
    pub(crate) fn spawn<T: WorkerClient>(&mut self, client: &T, call: Call) {
        if !self.calls.insert(call) {
            return;
        }
        let (client, db) = (client.clone(), self.db.clone());
        let delays = FixedInterval::new(self.retry_delay).take(MAX_FAILED_STARTS - 1);
        self.tasks.spawn(async move {
            let result = match call {
                // Each attempt reads the row first, so the repeats end once the row no longer wants the fragment running.
                // Rounds keep no count of failed starts, so leaving the repeats to them would never fail a start that always fails transiently.
                Call::Start(id) => Retry::spawn(delays, || try_start(&client, &db, id)).await,
                Call::Stop(id) => client.stop(id).await,
            };
            (call, result)
        });
    }

    /// Returns `None` while no call is in flight.
    /// A canceled wait leaves every call in flight.
    pub(crate) async fn join_next(
        &mut self,
    ) -> Option<Result<(Call, Result<(), CallError>), Cancelled>> {
        let (call, result) = match self.tasks.join_next().await? {
            Ok(joined) => joined,
            Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
            // Nothing aborts a single call while the set exists, so only a runtime that shuts down ends a call without a result.
            Err(_) => return Some(Err(Cancelled)),
        };
        self.calls.remove(&call);
        Some(Ok((call, result)))
    }
}

async fn try_start<T: WorkerClient>(
    client: &T,
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
        client.start(plan).await
    };
    started.await.map_err(|err| match err {
        CallError::Transient(err) => {
            warn!(fragment = %id, error = %err, "start failed");
            RetryError::transient(CallError::Transient(err))
        }
        err => RetryError::permanent(err),
    })
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::*;
    use crate::worker_client::remote::worker_rpc_service::WorkerStatusResponse;
    use std::future::pending;
    use tokio::runtime::Builder;

    #[derive(Clone)]
    struct NeverReturns;

    impl WorkerClient for NeverReturns {
        async fn start(&self, _plan: SerializableQueryPlan) -> Result<(), CallError> {
            pending().await
        }

        async fn stop(&self, _id: QueryFragmentId) -> Result<(), CallError> {
            pending().await
        }

        async fn get_status(&self, _after: u64) -> anyhow::Result<WorkerStatusResponse> {
            pending().await
        }
    }

    #[test]
    fn a_runtime_that_shuts_down_cancels_a_call_instead_of_raising_a_panic() {
        let waiting = Builder::new_current_thread().enable_time().build().unwrap();
        let calling = Builder::new_current_thread().build().unwrap();
        let mut calls = InFlightCalls::new(waiting.block_on(Database::for_test()), Duration::ZERO);
        {
            let _context = calling.enter();
            calls.spawn(&NeverReturns, Call::Stop(QueryFragmentId::new(1)));
        }

        drop(calling);

        assert!(matches!(
            waiting.block_on(calls.join_next()),
            Some(Err(Cancelled))
        ));
    }
}
