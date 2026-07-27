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

//! Moves one fragment from start to a finished state by asking its worker for
//! the next step and saving the result after each step.

use super::{Client, Outcome, QueryFragmentStatus};
use crate::config::{MAX_CONSECUTIVE_FAILURES, RETRY_INTERVAL};
use crate::util::buggify::{buggify, buggify_return};
use anyhow::Context;
use chrono::{DateTime, Utc};
use model::query::query_fragment;
use model::query::query_fragment::{
    DesiredQueryFragmentState, QueryFragmentError, QueryFragmentState, QueryFragmentTransition,
};
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, error, warn};

fn unix_ms_to_datetime(ms: u64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(i64::try_from(ms).unwrap_or(i64::MAX)).unwrap_or_default()
}

/// Counts down on consecutive retryable failures and converts the last
/// error into a terminal transport error once the budget hits zero. Any
/// successful outcome resets the counter.
struct RetryBudget {
    remaining: u32,
}

impl RetryBudget {
    const fn new() -> Self {
        Self {
            remaining: MAX_CONSECUTIVE_FAILURES,
        }
    }

    const fn reset(&mut self) {
        self.remaining = MAX_CONSECUTIVE_FAILURES;
    }

    fn record_failure(&mut self, error: &QueryFragmentError) -> Option<QueryFragmentError> {
        self.remaining = self.remaining.saturating_sub(1);
        if self.remaining > 0 {
            return None;
        }
        let cause = format!("retry budget exhausted, last error: {error}");
        error!("{cause}");
        Some(QueryFragmentError::Transport { msg: cause })
    }
}

/// Moves a single fragment through its lifecycle from pending through to a
/// terminal state. Each iteration asks the worker for the next step, applies
/// the result to a staged copy of the row, and persists. Between iterations,
/// it refreshes from the DB so external desired-state changes (such as a
/// stop request) take effect.
pub struct FragmentTask<C> {
    fragment: query_fragment::Model,
    update: query_fragment::ActiveModel,
    db: DatabaseConnection,
    client: C,
    state_tx: Arc<watch::Sender<()>>,
    budget: RetryBudget,
}

impl<C: Client> FragmentTask<C> {
    pub fn new(
        fragment: query_fragment::Model,
        db: DatabaseConnection,
        client: C,
        state_tx: Arc<watch::Sender<()>>,
    ) -> Self {
        let update = fragment.clone().into();
        Self {
            fragment,
            update,
            db,
            client,
            state_tx,
            budget: RetryBudget::new(),
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        debug!(state = %self.fragment.current_state, "starting");
        if self.fragment.desired_state != DesiredQueryFragmentState::Stopped {
            self.up().await?;
        }
        if !self.fragment.current_state.is_terminal() {
            self.down().await?;
        }
        Ok(())
    }

    async fn up(&mut self) -> anyhow::Result<()> {
        while !self.fragment.current_state.is_terminal() {
            // A short query can already be finished by the time the start returns, so the first
            // status read follows it without waiting. Only a read that found the fragment still
            // running pays the interval before the next one.
            let starting = self.fragment.current_state == QueryFragmentState::Pending;
            let outcome = match self.fragment.current_state {
                QueryFragmentState::Pending => {
                    self.client
                        .start(self.fragment.id, &self.fragment.plan)
                        .await
                }
                // Started is what a worker reports once it has taken the plan but before it is
                // producing; both states advance only by asking the worker again.
                QueryFragmentState::Started | QueryFragmentState::Running => {
                    self.client
                        .observe(self.fragment.id, self.fragment.desired_state)
                        .await
                }
                _ => unreachable!("terminal state"),
            };
            self.apply(outcome).await?;
            // A stop request arrived while the fragment was advancing. Leave the loop so the
            // stop path takes over rather than continuing to poll toward running.
            if self.fragment.desired_state == DesiredQueryFragmentState::Stopped {
                break;
            }
            if !starting
                && matches!(
                    self.fragment.current_state,
                    QueryFragmentState::Started | QueryFragmentState::Running
                )
            {
                tokio::time::sleep(self.client.poll_interval()).await;
            }
        }
        Ok(())
    }

    async fn down(&mut self) -> anyhow::Result<()> {
        // A fragment still recorded as pending is not skipped here: starting it is a single call,
        // so a lost reply leaves it live on the worker while the catalog has not caught up, and
        // skipping the stop would leave it there. A stop for a fragment the worker never
        // received answers not-found, which is recorded as stopped just the same.
        //
        // Send the stop request, retrying until the worker takes it or the
        // fragment reaches a terminal state on its own.
        while !self.fragment.current_state.is_terminal() {
            match self.client.stop(self.fragment.id).await {
                Outcome::Accepted => break,
                outcome => self.apply(outcome).await?,
            }
        }
        // The stop was taken but the fragment is not done yet; poll until it
        // reaches a terminal state.
        while !self.fragment.current_state.is_terminal() {
            let outcome = self
                .client
                .observe(self.fragment.id, self.fragment.desired_state)
                .await;
            self.apply(outcome).await?;
            // Waiting after the read that found the fragment terminal would delay the loop's exit
            // by a whole interval without asking anything.
            if !self.fragment.current_state.is_terminal() {
                tokio::time::sleep(self.client.poll_interval()).await;
            }
        }
        Ok(())
    }

    /// Stages the row changes a worker's answer implies, then persists them. A successful
    /// answer of any kind refills the retry budget, so the budget counts consecutive
    /// failures rather than failures over the whole lifetime.
    async fn apply(&mut self, outcome: Outcome) -> anyhow::Result<()> {
        let prev = self.fragment.current_state;
        let now = Utc::now();
        match outcome {
            Outcome::Transition(transition) => {
                self.budget.reset();
                self.update.apply_transition(transition);
                self.update.mark_observed(now);
            }
            Outcome::Status(status) => {
                self.budget.reset();
                self.apply_status(status, now);
                self.update.mark_observed(now);
            }
            Outcome::Accepted => {
                self.budget.reset();
                self.update.mark_observed(now);
            }
            // The fragment failed in an unrecoverable way, or the worker is permanently unreachable/failed
            Outcome::Failed(err) => {
                error!("fatal: {err}");
                self.update
                    .apply_transition(QueryFragmentTransition::failed_now(err));
            }
            Outcome::Retry(err) => {
                warn!("{err}");
                if let Some(exhaustion) = self.budget.record_failure(&err) {
                    self.update
                        .apply_transition(QueryFragmentTransition::failed_now(exhaustion));
                } else {
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        }

        self.refresh().await?;
        if self.fragment.current_state != prev {
            debug!(from = %prev, to = %self.fragment.current_state, "transition");
        }
        buggify_return!(Ok(()));
        let _ = self.state_tx.send(());
        Ok(())
    }

    /// This is not really efficient
    /// (we write and read from the DB row for every fragment after every poll/transition),
    /// but it's easy to reason about.
    /// Once it becomes a performance issue, we can revisit.
    async fn refresh(&mut self) -> anyhow::Result<()> {
        if buggify!() {
            buggify_return!(Ok(()));
            return Err(anyhow::anyhow!("buggify: refresh failed"));
        }
        self.update.clone().save(&self.db).await?;
        self.fragment = query_fragment::Entity::find_by_id(self.fragment.id)
            .one(&self.db)
            .await?
            .context("fragment not found")?;
        self.update = self.fragment.clone().into();
        Ok(())
    }

    fn apply_status(&mut self, status: QueryFragmentStatus, now: DateTime<Utc>) {
        let mapped_state = if status.state == QueryFragmentState::Completed
            && self.fragment.desired_state == DesiredQueryFragmentState::Stopped
        {
            QueryFragmentState::Stopped
        } else {
            status.state
        };
        let start_ts = status.start_timestamp.map(unix_ms_to_datetime);
        let stop_ts = status.stop_timestamp.map(unix_ms_to_datetime);
        // A worker that omits a required field is likely a version mismatch.
        // Fill it in with the current time so the fragment still advances,
        // and warn. Retrying would just get the same report back.
        let fallback_ts = |field: &str| {
            warn!("worker reported {mapped_state} without {field}; using current time");
            now
        };
        let transition = match mapped_state {
            QueryFragmentState::Pending => QueryFragmentTransition::Pending,
            QueryFragmentState::Started => QueryFragmentTransition::Started,
            QueryFragmentState::Running => QueryFragmentTransition::Running {
                start_timestamp: start_ts.unwrap_or_else(|| fallback_ts("start_timestamp")),
            },
            QueryFragmentState::Completed => QueryFragmentTransition::Completed {
                stop_timestamp: stop_ts.unwrap_or_else(|| fallback_ts("stop_timestamp")),
            },
            QueryFragmentState::Stopped => QueryFragmentTransition::Stopped {
                stop_timestamp: stop_ts.unwrap_or_else(|| fallback_ts("stop_timestamp")),
            },
            QueryFragmentState::Failed => QueryFragmentTransition::Failed {
                stop_timestamp: stop_ts.unwrap_or_else(|| fallback_ts("stop_timestamp")),
                error: status.error.unwrap_or_else(|| {
                    warn!("worker reported Failed without error; using a generic error");
                    QueryFragmentError::Transport {
                        msg: "worker reported failure without detail".to_string(),
                    }
                }),
            },
        };
        self.update.apply_transition(transition);
    }
}
