use super::{FragmentClient, FragmentStatus, Outcome, POLL_INTERVAL};
use anyhow::{Context, anyhow};
use chrono::{DateTime, Local};
use model::query::fragment;
use model::query::fragment::{
    DesiredFragmentState, FragmentError, FragmentState, FragmentTransition,
};
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, warn};

const RETRY_INTERVAL: Duration = Duration::from_secs(2);
const MAX_CONSECUTIVE_FAILURES: u32 = 10;

fn unix_ms_to_datetime(ms: u64) -> DateTime<Local> {
    DateTime::from_timestamp_millis(ms as i64)
        .unwrap_or_default()
        .with_timezone(&Local)
}

struct RetryBudget {
    remaining: u32,
    last_error: Option<FragmentError>,
}

impl RetryBudget {
    fn new() -> Self {
        Self {
            remaining: MAX_CONSECUTIVE_FAILURES,
            last_error: None,
        }
    }

    fn reset(&mut self) {
        self.remaining = MAX_CONSECUTIVE_FAILURES;
    }

    fn record_failure(&mut self, error: FragmentError) -> Option<FragmentError> {
        self.last_error = Some(error);
        self.remaining = self.remaining.saturating_sub(1);
        if self.remaining > 0 {
            None
        } else {
            let cause = self
                .last_error
                .as_ref()
                .map(|err| format!("retry budget exhausted, last error: {err}"))
                .unwrap_or_else(|| "retry budget exhausted".to_string());
            error!("{cause}");
            Some(FragmentError::Transport { msg: cause })
        }
    }
}

pub struct FragmentTask<C> {
    fragment: fragment::Model,
    update: fragment::ActiveModel,
    db: DatabaseConnection,
    client: C,
    state_tx: Arc<watch::Sender<()>>,
    budget: RetryBudget,
}

impl<C: FragmentClient> FragmentTask<C> {
    pub fn new(
        fragment: fragment::Model,
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
        if self.fragment.desired_state != DesiredFragmentState::Stopped {
            self.up().await?;
        }
        if !self.fragment.current_state.is_terminal() {
            self.down().await?;
        }
        Ok(())
    }

    async fn up(&mut self) -> anyhow::Result<()> {
        while !self.fragment.current_state.is_terminal() {
            let outcome = match self.fragment.current_state {
                FragmentState::Pending => {
                    self.client
                        .register(self.fragment.id, &self.fragment.plan)
                        .await
                }
                FragmentState::Registered => self.client.start(self.fragment.id).await,
                FragmentState::Running => {
                    self.client
                        .poll(self.fragment.id, self.fragment.desired_state)
                        .await
                }
                _ => unreachable!("terminal state"),
            };
            self.apply(outcome).await?;
            if self.fragment.desired_state == DesiredFragmentState::Stopped {
                break;
            }
            if self.fragment.current_state == FragmentState::Running {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
        Ok(())
    }

    async fn down(&mut self) -> anyhow::Result<()> {
        if self.fragment.current_state == FragmentState::Pending {
            self.apply(Outcome::Transition(FragmentTransition::stopped_now()))
                .await?;
            return Ok(());
        }
        let mode = self.fragment.stop_mode.unwrap_or_default();
        while !self.fragment.current_state.is_terminal() {
            match self.client.stop(self.fragment.id, mode).await {
                Outcome::Accepted => break,
                outcome => self.apply(outcome).await?,
            }
        }
        while !self.fragment.current_state.is_terminal() {
            let outcome = self
                .client
                .poll(self.fragment.id, self.fragment.desired_state)
                .await;
            self.apply(outcome).await?;
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        Ok(())
    }

    async fn apply(&mut self, outcome: Outcome) -> anyhow::Result<()> {
        let prev = self.fragment.current_state;
        let now = Local::now();
        match outcome {
            Outcome::Transition(transition) => {
                self.budget.reset();
                self.update.apply_transition(transition);
                self.update.mark_observed(now);
            }
            Outcome::Status(status) => {
                self.budget.reset();
                self.apply_status(status)?;
                self.update.mark_observed(now);
            }
            Outcome::Accepted => {
                self.budget.reset();
                self.update.mark_observed(now);
            }
            Outcome::Failed(err) => {
                error!("fatal: {err}");
                self.update.apply_transition(FragmentTransition::failed_now(err));
            }
            Outcome::Retry(err) => {
                warn!("{err}");
                if let Some(exhaustion) = self.budget.record_failure(err) {
                    self.update
                        .apply_transition(FragmentTransition::failed_now(exhaustion));
                } else {
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        }

        self.refresh().await?;
        if self.fragment.current_state != prev {
            debug!(from = %prev, to = %self.fragment.current_state, "transition");
        }
        #[cfg(madsim)]
        if tokio::madsim::buggify::buggify() {
            return Ok(());
        }
        self.state_tx.send(()).expect("state channel closed");
        Ok(())
    }

    async fn refresh(&mut self) -> anyhow::Result<()> {
        #[cfg(madsim)]
        if tokio::madsim::buggify::buggify() {
            if tokio::madsim::buggify::buggify() {
                return Ok(());
            }
            return Err(anyhow::anyhow!("buggify: refresh failed"));
        }
        self.update.clone().save(&self.db).await?;
        self.fragment = fragment::Entity::find_by_id(self.fragment.id)
            .one(&self.db)
            .await?
            .context("fragment not found")?;
        self.update = self.fragment.clone().into();
        Ok(())
    }

    fn apply_status(&mut self, status: FragmentStatus) -> anyhow::Result<()> {
        let mapped_state = if status.state == FragmentState::Completed
            && self.fragment.desired_state == DesiredFragmentState::Stopped
        {
            FragmentState::Stopped
        } else {
            status.state
        };
        let start_ts = status.start_timestamp.map(unix_ms_to_datetime);
        let stop_ts = status.stop_timestamp.map(unix_ms_to_datetime);
        let transition = match mapped_state {
            FragmentState::Pending => FragmentTransition::Pending,
            FragmentState::Registered => FragmentTransition::Registered,
            FragmentState::Running => FragmentTransition::Running {
                start_timestamp: start_ts
                    .ok_or_else(|| anyhow!("worker reported Running without start_timestamp"))?,
            },
            FragmentState::Completed => FragmentTransition::Completed {
                stop_timestamp: stop_ts
                    .ok_or_else(|| anyhow!("worker reported Completed without stop_timestamp"))?,
            },
            FragmentState::Stopped => FragmentTransition::Stopped {
                stop_timestamp: stop_ts
                    .ok_or_else(|| anyhow!("worker reported Stopped without stop_timestamp"))?,
            },
            FragmentState::Failed => FragmentTransition::Failed {
                stop_timestamp: stop_ts
                    .ok_or_else(|| anyhow!("worker reported Failed without stop_timestamp"))?,
                error: status
                    .error
                    .ok_or_else(|| anyhow!("worker reported Failed without error"))?,
            },
        };
        self.update.apply_transition(transition);
        Ok(())
    }
}
