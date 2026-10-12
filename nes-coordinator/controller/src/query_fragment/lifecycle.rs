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

//! Owns the lifecycle of the coordinator representation of a single query query_fragment.
//! Does not interact with the catalog or the worker hosting the query_fragment.
//! Instead, has a purely functional interface where the caller passes in the current/desired
//! state of the fragment and .

use super::{CallError, RawStatus};
use crate::config::{MAX_CONSECUTIVE_FAILURES, RETRY_INTERVAL};
use chrono::{DateTime, Utc};
use model::query::query_fragment::{
    self, DesiredQueryFragmentState, QueryFragmentError, QueryFragmentState,
    QueryFragmentTransition,
};
use std::time::Duration;
use tracing::{error, warn};

pub(super) enum WorkerReply {
    Start(Result<(), CallError>),
    Stop(Result<(), CallError>),
    Status(Result<RawStatus, CallError>),
}

enum WorkerRpcOutcome {
    Transition(QueryFragmentTransition),
    Status(QueryFragmentStatus),
    /// The request succeeded, but no state change is implied by it.
    Accepted,
    /// Terminal error: mark the query_fragment failed.
    Failed(QueryFragmentError),
    /// Transient error: retry against a budget.
    Retry(QueryFragmentError),
}

struct QueryFragmentStatus {
    state: QueryFragmentState,
    start_timestamp: Option<u64>,
    stop_timestamp: Option<u64>,
    error: Option<QueryFragmentError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WorkerCall {
    Start,
    Stop,
    Status,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum LifecycleAction {
    Call(WorkerCall),
    Wait(Duration),
    Done,
}

pub(super) struct LifecycleStep {
    pub transition: Option<QueryFragmentTransition>,
    pub observed: bool,
    pub retry_wait: Option<Duration>,
}

impl LifecycleStep {
    const fn observed(transition: Option<QueryFragmentTransition>) -> Self {
        Self {
            transition,
            observed: true,
            retry_wait: None,
        }
    }

    const fn failed(transition: QueryFragmentTransition) -> Self {
        Self {
            transition: Some(transition),
            observed: false,
            retry_wait: None,
        }
    }

    const fn retry(wait: Duration) -> Self {
        Self {
            transition: None,
            observed: false,
            retry_wait: Some(wait),
        }
    }
}

/// Counts down on consecutive retryable failures, so we don't retry forever.
/// Converts the last error into a terminal transport error when the budget is exhausted.
/// A successful outcome resets the counter.
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

    fn record(&mut self, error: &QueryFragmentError) -> Option<QueryFragmentError> {
        self.remaining = self.remaining.saturating_sub(1);
        if self.remaining > 0 {
            return None;
        }
        let cause = format!("retry budget exhausted, last error: {error}");
        error!("{cause}");
        Some(QueryFragmentError::Transport { msg: cause })
    }
}

pub(super) struct QueryFragmentLifecycle {
    curr_state: QueryFragmentState,
    des_state: DesiredQueryFragmentState,
    stop_taken: bool,
    budget: RetryBudget,
    poll_interval: Duration,
    wait_due: Duration,
}

impl QueryFragmentLifecycle {
    pub(super) fn new(row: &query_fragment::Model, poll_interval: Duration) -> Self {
        Self {
            curr_state: row.current_state,
            des_state: row.desired_state,
            stop_taken: false,
            budget: RetryBudget::new(),
            poll_interval,
            wait_due: Duration::ZERO,
        }
    }

    pub(super) fn next(&mut self) -> LifecycleAction {
        if self.curr_state.is_terminal() {
            return LifecycleAction::Done;
        }
        if !self.wait_due.is_zero() {
            return LifecycleAction::Wait(std::mem::take(&mut self.wait_due));
        }
        let call = match (self.des_state, self.curr_state) {
            (DesiredQueryFragmentState::Completed, QueryFragmentState::Pending) => {
                WorkerCall::Start
            }
            (DesiredQueryFragmentState::Completed, _) => WorkerCall::Status,
            (DesiredQueryFragmentState::Stopped, _) if self.stop_taken => WorkerCall::Status,
            (DesiredQueryFragmentState::Stopped, _) => WorkerCall::Stop,
        };
        LifecycleAction::Call(call)
    }

    /// A successful answer of any kind refills the retry budget,
    /// so the budget counts consecutive failures rather than failures over the whole lifetime.
    pub(super) fn apply(&mut self, reply: WorkerReply, now: DateTime<Utc>) -> LifecycleStep {
        let call = match reply {
            WorkerReply::Start(_) => WorkerCall::Start,
            WorkerReply::Stop(_) => WorkerCall::Stop,
            WorkerReply::Status(_) => WorkerCall::Status,
        };
        let step = match outcome(reply, self.des_state, now) {
            WorkerRpcOutcome::Transition(transition) => {
                self.budget.reset();
                LifecycleStep::observed(Some(transition))
            }
            WorkerRpcOutcome::Status(status) => {
                self.budget.reset();
                LifecycleStep::observed(Some(status_to_transition(status, self.des_state, now)))
            }
            WorkerRpcOutcome::Accepted => {
                self.budget.reset();
                self.stop_taken = true;
                LifecycleStep::observed(None)
            }
            // The query_fragment failed in an unrecoverable way,
            // or the worker is permanently unreachable or failed.
            WorkerRpcOutcome::Failed(err) => {
                error!("fatal: {err}");
                LifecycleStep::failed(failed_at(now, err))
            }
            WorkerRpcOutcome::Retry(err) => {
                warn!("{err}");
                match self.budget.record(&err) {
                    Some(exhaustion) => LifecycleStep::failed(failed_at(now, exhaustion)),
                    None => LifecycleStep::retry(RETRY_INTERVAL),
                }
            }
        };
        if let Some(transition) = &step.transition {
            self.curr_state = state_of(transition);
        }
        let live = matches!(
            self.curr_state,
            QueryFragmentState::Started | QueryFragmentState::Running
        );
        let polling = match self.des_state {
            DesiredQueryFragmentState::Completed => live,
            DesiredQueryFragmentState::Stopped => self.stop_taken,
        };
        self.wait_due = if call == WorkerCall::Status && polling {
            self.poll_interval
        } else {
            Duration::ZERO
        };
        step
    }

    pub(super) fn desired(&mut self, desired: DesiredQueryFragmentState) {
        self.des_state = desired;
    }
}

fn state_of(transition: &QueryFragmentTransition) -> QueryFragmentState {
    match transition {
        QueryFragmentTransition::Pending => QueryFragmentState::Pending,
        QueryFragmentTransition::Started => QueryFragmentState::Started,
        QueryFragmentTransition::Running { .. } => QueryFragmentState::Running,
        QueryFragmentTransition::Completed { .. } => QueryFragmentState::Completed,
        QueryFragmentTransition::Stopped { .. } => QueryFragmentState::Stopped,
        QueryFragmentTransition::Failed { .. } => QueryFragmentState::Failed,
    }
}

fn outcome(
    reply: WorkerReply,
    desired: DesiredQueryFragmentState,
    now: DateTime<Utc>,
) -> WorkerRpcOutcome {
    let stopped = || QueryFragmentTransition::Stopped {
        start_timestamp: None,
        stop_timestamp: now,
    };
    match reply {
        // Started rather than Running: the worker has only accepted the query_fragment at this point,
        // and the state machine advances a pending query_fragment to started before anything else.
        // The status read promotes it once the worker reports that it is producing.
        WorkerReply::Start(Ok(())) => {
            WorkerRpcOutcome::Transition(QueryFragmentTransition::Started)
        }
        WorkerReply::Start(Err(CallError::NotFound)) => {
            WorkerRpcOutcome::Transition(QueryFragmentTransition::Pending)
        }
        WorkerReply::Stop(Ok(())) => WorkerRpcOutcome::Accepted,
        WorkerReply::Stop(Err(CallError::NotFound)) => WorkerRpcOutcome::Transition(stopped()),
        WorkerReply::Status(Ok(raw)) => match QueryFragmentState::try_from(raw.state) {
            Ok(state) => WorkerRpcOutcome::Status(QueryFragmentStatus {
                state,
                start_timestamp: raw.start_ms,
                stop_timestamp: raw.stop_ms,
                error: raw.error,
            }),
            // A state value that this build does not know is read again rather than acted on,
            // since acting on it would change the query_fragment's state on a guess.
            Err(unknown) => WorkerRpcOutcome::Retry(QueryFragmentError::Transport {
                msg: format!("worker reported unknown query_fragment state: {unknown}"),
            }),
        },
        WorkerReply::Status(Err(CallError::NotFound)) => match desired {
            DesiredQueryFragmentState::Completed => {
                WorkerRpcOutcome::Transition(QueryFragmentTransition::Pending)
            }
            DesiredQueryFragmentState::Stopped => WorkerRpcOutcome::Transition(stopped()),
        },
        WorkerReply::Start(Err(CallError::Transient(err)))
        | WorkerReply::Stop(Err(CallError::Transient(err)))
        | WorkerReply::Status(Err(CallError::Transient(err))) => WorkerRpcOutcome::Retry(err),
        WorkerReply::Start(Err(CallError::Failed(err)))
        | WorkerReply::Stop(Err(CallError::Failed(err)))
        | WorkerReply::Status(Err(CallError::Failed(err))) => WorkerRpcOutcome::Failed(err),
    }
}

fn failed_at(now: DateTime<Utc>, error: QueryFragmentError) -> QueryFragmentTransition {
    QueryFragmentTransition::Failed {
        start_timestamp: None,
        stop_timestamp: now,
        error,
    }
}

fn unix_ms_to_datetime(ms: u64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(i64::try_from(ms).unwrap_or(i64::MAX)).unwrap_or_default()
}

fn status_to_transition(
    status: QueryFragmentStatus,
    desired: DesiredQueryFragmentState,
    now: DateTime<Utc>,
) -> QueryFragmentTransition {
    let state = if status.state == QueryFragmentState::Completed
        && desired == DesiredQueryFragmentState::Stopped
    {
        QueryFragmentState::Stopped
    } else {
        status.state
    };
    let start = status.start_timestamp.map(unix_ms_to_datetime);
    let stop = status.stop_timestamp.map(unix_ms_to_datetime);
    let fallback = |field: &str| {
        warn!("worker reported {state} without {field}; using current time");
        now
    };
    match state {
        QueryFragmentState::Pending => QueryFragmentTransition::Pending,
        QueryFragmentState::Started => QueryFragmentTransition::Started,
        QueryFragmentState::Running => QueryFragmentTransition::Running {
            start_timestamp: start.unwrap_or_else(|| fallback("start_timestamp")),
        },
        QueryFragmentState::Completed => QueryFragmentTransition::Completed {
            start_timestamp: start,
            stop_timestamp: stop.unwrap_or_else(|| fallback("stop_timestamp")),
        },
        QueryFragmentState::Stopped => QueryFragmentTransition::Stopped {
            start_timestamp: start,
            stop_timestamp: stop.unwrap_or_else(|| fallback("stop_timestamp")),
        },
        QueryFragmentState::Failed => QueryFragmentTransition::Failed {
            start_timestamp: start,
            stop_timestamp: stop.unwrap_or_else(|| fallback("stop_timestamp")),
            error: status.error.unwrap_or_else(|| {
                warn!("worker reported Failed without error; using a generic error");
                QueryFragmentError::Transport {
                    msg: "worker reported failure without detail".to_string(),
                }
            }),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::identifier::{QueryFragmentId, QueryId};

    const POLL: Duration = Duration::from_millis(25);
    const UNKNOWN_STATE: i32 = 99;

    use DesiredQueryFragmentState::{Completed as WantCompleted, Stopped as WantStopped};
    use QueryFragmentState::{Completed, Pending, Running, Started, Stopped};

    fn row(
        current: QueryFragmentState,
        desired: DesiredQueryFragmentState,
    ) -> query_fragment::Model {
        query_fragment::Model {
            id: QueryFragmentId::new(1),
            query_id: QueryId::new(1),
            host_addr: "127.0.0.1:1".parse().unwrap(),
            plan: Vec::new(),
            num_operators: 1,
            has_source: true,
            current_state: current,
            desired_state: desired,
            start_timestamp: None,
            stop_timestamp: None,
            error: None,
            last_observed_at: None,
        }
    }

    fn lifecycle(
        current: QueryFragmentState,
        desired: DesiredQueryFragmentState,
    ) -> QueryFragmentLifecycle {
        QueryFragmentLifecycle::new(&row(current, desired), POLL)
    }

    fn transient(msg: &str) -> CallError {
        CallError::Transient(QueryFragmentError::Transport {
            msg: msg.to_string(),
        })
    }

    fn wire(state: QueryFragmentState) -> i32 {
        (0..16)
            .find(|value| QueryFragmentState::try_from(*value) == Ok(state))
            .unwrap()
    }

    fn raw(state: QueryFragmentState, start: Option<u64>, stop: Option<u64>) -> RawStatus {
        RawStatus {
            state: wire(state),
            start_ms: start,
            stop_ms: stop,
            error: None,
        }
    }

    fn report(
        state: QueryFragmentState,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> QueryFragmentStatus {
        QueryFragmentStatus {
            state,
            start_timestamp: start,
            stop_timestamp: stop,
            error: None,
        }
    }

    fn apply(lifecycle: &mut QueryFragmentLifecycle, reply: WorkerReply) -> LifecycleStep {
        lifecycle.apply(reply, Utc::now())
    }

    #[test]
    fn a_pending_fragment_is_started_and_a_live_one_observed() {
        assert_eq!(
            lifecycle(Pending, WantCompleted).next(),
            LifecycleAction::Call(WorkerCall::Start)
        );
        assert_eq!(
            lifecycle(Started, WantCompleted).next(),
            LifecycleAction::Call(WorkerCall::Status)
        );
        assert_eq!(
            lifecycle(Running, WantCompleted).next(),
            LifecycleAction::Call(WorkerCall::Status)
        );
    }

    #[test]
    fn a_terminal_fragment_is_done_whatever_was_wanted() {
        assert_eq!(
            lifecycle(Completed, WantCompleted).next(),
            LifecycleAction::Done
        );
        assert_eq!(
            lifecycle(Stopped, WantStopped).next(),
            LifecycleAction::Done
        );
        assert_eq!(
            lifecycle(QueryFragmentState::Failed, WantStopped).next(),
            LifecycleAction::Done
        );
    }

    #[test]
    fn a_start_is_followed_by_an_observation_without_a_wait() {
        let mut lifecycle = lifecycle(Pending, WantCompleted);
        let step = apply(&mut lifecycle, WorkerReply::Start(Ok(())));
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Started)
        ));
        assert!(step.observed);
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Status));
    }

    #[test]
    fn a_start_of_a_fragment_the_worker_does_not_know_is_repeated_at_once() {
        let mut lifecycle = lifecycle(Pending, WantCompleted);
        let step = apply(&mut lifecycle, WorkerReply::Start(Err(CallError::NotFound)));
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Pending)
        ));
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Start));
    }

    #[test]
    fn an_observation_that_finds_the_fragment_live_earns_the_poll_wait_once() {
        let mut lifecycle = lifecycle(Started, WantCompleted);
        let step = apply(
            &mut lifecycle,
            WorkerReply::Status(Ok(raw(Running, Some(1_000), None))),
        );
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Running { start_timestamp }) if start_timestamp.timestamp_millis() == 1_000
        ));
        assert_eq!(lifecycle.next(), LifecycleAction::Wait(POLL));
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Status));
    }

    #[test]
    fn an_observation_that_finds_the_fragment_unknown_starts_it_again_at_once() {
        let mut lifecycle = lifecycle(Started, WantCompleted);
        let step = apply(
            &mut lifecycle,
            WorkerReply::Status(Err(CallError::NotFound)),
        );
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Pending)
        ));
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Start));
    }

    #[test]
    fn an_unknown_fragment_that_should_stop_is_recorded_stopped() {
        let mut lifecycle = lifecycle(Running, WantStopped);
        apply(&mut lifecycle, WorkerReply::Stop(Ok(())));
        let step = apply(
            &mut lifecycle,
            WorkerReply::Status(Err(CallError::NotFound)),
        );
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Stopped { .. })
        ));
        assert_eq!(lifecycle.next(), LifecycleAction::Done);
    }

    #[test]
    fn an_unknown_state_is_read_again() {
        let mut lifecycle = lifecycle(Started, WantCompleted);
        let step = apply(
            &mut lifecycle,
            WorkerReply::Status(Ok(RawStatus {
                state: UNKNOWN_STATE,
                start_ms: None,
                stop_ms: None,
                error: None,
            })),
        );
        assert!(step.transition.is_none());
        assert!(!step.observed);
        assert_eq!(step.retry_wait, Some(RETRY_INTERVAL));
        assert_eq!(lifecycle.next(), LifecycleAction::Wait(POLL));
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Status));
    }

    #[test]
    fn a_stop_request_turns_the_next_call_into_a_stop_without_a_wait() {
        let mut lifecycle = lifecycle(Started, WantCompleted);
        apply(
            &mut lifecycle,
            WorkerReply::Status(Ok(raw(Running, Some(1), None))),
        );
        lifecycle.desired(WantStopped);
        assert_eq!(lifecycle.next(), LifecycleAction::Wait(POLL));
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Stop));
    }

    #[test]
    fn a_fragment_wanted_stopped_begins_by_stopping() {
        assert_eq!(
            lifecycle(Pending, WantStopped).next(),
            LifecycleAction::Call(WorkerCall::Stop)
        );
        assert_eq!(
            lifecycle(Running, WantStopped).next(),
            LifecycleAction::Call(WorkerCall::Stop)
        );
    }

    #[test]
    fn a_stop_that_was_not_taken_is_repeated_after_the_retry_pause() {
        let mut lifecycle = lifecycle(Running, WantStopped);
        let step = apply(
            &mut lifecycle,
            WorkerReply::Stop(Err(transient("worker busy"))),
        );
        assert!(step.transition.is_none());
        assert!(!step.observed);
        assert_eq!(step.retry_wait, Some(RETRY_INTERVAL));
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Stop));
    }

    #[test]
    fn an_accepted_stop_drains_the_fragment_with_polls_in_between() {
        let mut lifecycle = lifecycle(Running, WantStopped);
        let step = apply(&mut lifecycle, WorkerReply::Stop(Ok(())));
        assert!(step.transition.is_none());
        assert!(step.observed);
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Status));
        apply(
            &mut lifecycle,
            WorkerReply::Status(Ok(raw(Running, Some(1), None))),
        );
        assert_eq!(lifecycle.next(), LifecycleAction::Wait(POLL));
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Status));
        let step = apply(
            &mut lifecycle,
            WorkerReply::Status(Ok(raw(Completed, Some(1), Some(2)))),
        );
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Stopped { .. })
        ));
        assert_eq!(lifecycle.next(), LifecycleAction::Done);
    }

    #[test]
    fn a_stop_of_a_fragment_the_worker_does_not_know_records_it_stopped() {
        let mut lifecycle = lifecycle(Pending, WantStopped);
        let step = apply(&mut lifecycle, WorkerReply::Stop(Err(CallError::NotFound)));
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Stopped { .. })
        ));
        assert_eq!(lifecycle.next(), LifecycleAction::Done);
    }

    #[test]
    fn retryable_failures_count_until_the_budget_is_spent() {
        let mut lifecycle = lifecycle(Pending, WantCompleted);
        for _ in 1..MAX_CONSECUTIVE_FAILURES {
            let step = apply(
                &mut lifecycle,
                WorkerReply::Start(Err(transient("worker busy"))),
            );
            assert!(step.transition.is_none());
            assert_eq!(step.retry_wait, Some(RETRY_INTERVAL));
            assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Start));
        }
        let step = apply(
            &mut lifecycle,
            WorkerReply::Start(Err(transient("worker busy"))),
        );
        assert!(step.retry_wait.is_none());
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Failed { error: QueryFragmentError::Transport { msg }, .. })
                if msg.starts_with("retry budget exhausted, last error: ") && msg.contains("worker busy")
        ));
        assert_eq!(lifecycle.next(), LifecycleAction::Done);
    }

    #[test]
    fn a_successful_answer_refills_the_budget() {
        let mut lifecycle = lifecycle(Pending, WantCompleted);
        for _ in 1..MAX_CONSECUTIVE_FAILURES {
            apply(
                &mut lifecycle,
                WorkerReply::Start(Err(transient("worker busy"))),
            );
        }
        apply(&mut lifecycle, WorkerReply::Start(Ok(())));
        for _ in 1..MAX_CONSECUTIVE_FAILURES {
            let step = apply(
                &mut lifecycle,
                WorkerReply::Status(Err(transient("worker busy"))),
            );
            assert_eq!(step.retry_wait, Some(RETRY_INTERVAL));
        }
    }

    #[test]
    fn a_failed_call_is_terminal_with_the_reported_error() {
        let mut lifecycle = lifecycle(Started, WantCompleted);
        let now = Utc::now();
        let step = lifecycle.apply(
            WorkerReply::Status(Err(CallError::Failed(QueryFragmentError::Transport {
                msg: "crashed".to_string(),
            }))),
            now,
        );
        assert!(!step.observed);
        assert!(step.retry_wait.is_none());
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Failed { stop_timestamp, error: QueryFragmentError::Transport { msg }, .. })
                if stop_timestamp == now && msg == "crashed"
        ));
        assert_eq!(lifecycle.next(), LifecycleAction::Done);
    }

    #[test]
    fn a_report_maps_to_the_transition_of_its_state() {
        let now = Utc::now();
        let cases = [
            (Pending, "Pending"),
            (Started, "Started"),
            (Running, "Running"),
            (Completed, "Completed"),
            (Stopped, "Stopped"),
            (QueryFragmentState::Failed, "Failed"),
        ];
        for (state, name) in cases {
            let transition =
                status_to_transition(report(state, Some(1_000), Some(2_000)), WantCompleted, now);
            assert_eq!(
                format!("{transition:?}").split([' ', '{']).next().unwrap(),
                name
            );
            assert_eq!(state_of(&transition), state);
        }
    }

    #[test]
    fn a_completion_while_a_stop_is_wanted_is_recorded_as_stopped() {
        let transition = status_to_transition(
            report(Completed, Some(1_000), Some(2_000)),
            WantStopped,
            Utc::now(),
        );
        assert!(matches!(
            transition,
            QueryFragmentTransition::Stopped { start_timestamp: Some(start), stop_timestamp }
                if start.timestamp_millis() == 1_000 && stop_timestamp.timestamp_millis() == 2_000
        ));
    }

    #[test]
    fn a_report_missing_a_timestamp_gets_the_current_time() {
        let now = Utc::now();
        assert!(matches!(
            status_to_transition(report(Running, None, None), WantCompleted, now),
            QueryFragmentTransition::Running { start_timestamp } if start_timestamp == now
        ));
        assert!(matches!(
            status_to_transition(report(Completed, Some(1_000), None), WantCompleted, now),
            QueryFragmentTransition::Completed { start_timestamp: Some(_), stop_timestamp } if stop_timestamp == now
        ));
    }

    #[test]
    fn a_failure_reported_without_an_error_gets_a_generic_one() {
        let transition = status_to_transition(
            report(QueryFragmentState::Failed, None, Some(2_000)),
            WantCompleted,
            Utc::now(),
        );
        assert!(matches!(
            transition,
            QueryFragmentTransition::Failed { error: QueryFragmentError::Transport { msg }, .. }
                if msg == "worker reported failure without detail"
        ));
    }
}
