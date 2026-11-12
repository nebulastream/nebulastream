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

//! The state machine of one fragment, without I/O:
//! the caller passes in the desired state and the worker's replies, and gets back the next action.

use super::{CallError, RawStatus};
use crate::config::{MAX_CONSECUTIVE_FAILURES, OBSERVATION_WRITE_INTERVAL, RETRY_INTERVAL};
use chrono::{DateTime, Utc};
use model::error::{CodedError, ErrorCode};
use model::query::query_fragment::{
    DesiredQueryFragmentState, QueryFragmentError, QueryFragmentState, QueryFragmentTransition,
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
    /// The transition that a status read reports; unlike other transitions, it may repeat the current state.
    Status(QueryFragmentTransition),
    StopAccepted,
    Failed(QueryFragmentError),
    Retry(QueryFragmentError),
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
}

impl LifecycleStep {
    const fn observed(transition: Option<QueryFragmentTransition>) -> Self {
        Self {
            transition,
            observed: true,
        }
    }

    const fn failed(now: DateTime<Utc>, error: QueryFragmentError) -> Self {
        Self {
            transition: Some(QueryFragmentTransition::Failed {
                start_timestamp: None,
                stop_timestamp: now,
                error,
            }),
            observed: false,
        }
    }

    const fn unchanged() -> Self {
        Self {
            transition: None,
            observed: false,
        }
    }
}

pub(super) struct QueryFragmentLifecycle {
    current: QueryFragmentState,
    desired: DesiredQueryFragmentState,
    /// A stop was sent and no status has been read since.
    /// A status read clears it, so a fragment that is still reported live gets the stop again after the poll wait.
    stop_sent: bool,
    /// Consecutive retryable failures; the fragment fails when they reach the limit.
    failures: u32,
    poll_interval: Duration,
    wait_due: Duration,
    last_written: Option<DateTime<Utc>>,
}

impl QueryFragmentLifecycle {
    pub(super) const fn new(
        current: QueryFragmentState,
        desired: DesiredQueryFragmentState,
        poll_interval: Duration,
    ) -> Self {
        Self {
            current,
            desired,
            stop_sent: false,
            failures: 0,
            poll_interval,
            wait_due: Duration::ZERO,
            last_written: None,
        }
    }

    fn observation_due(&self, now: DateTime<Utc>) -> bool {
        self.last_written.is_none_or(|written| {
            (now - written)
                .to_std()
                .is_ok_and(|elapsed| elapsed >= OBSERVATION_WRITE_INTERVAL)
        })
    }

    pub(super) fn next(&mut self) -> LifecycleAction {
        if self.current.is_terminal() {
            return LifecycleAction::Done;
        }
        if !self.wait_due.is_zero() {
            return LifecycleAction::Wait(std::mem::take(&mut self.wait_due));
        }
        let call = match (self.desired, self.current) {
            (DesiredQueryFragmentState::Completed, QueryFragmentState::Pending) => {
                WorkerCall::Start
            }
            (DesiredQueryFragmentState::Completed, _) => WorkerCall::Status,
            (DesiredQueryFragmentState::Stopped, _) if self.stop_sent => WorkerCall::Status,
            (DesiredQueryFragmentState::Stopped, _) => WorkerCall::Stop,
        };
        LifecycleAction::Call(call)
    }

    pub(super) fn apply(&mut self, reply: WorkerReply, now: DateTime<Utc>) -> LifecycleStep {
        self.wait_due = Duration::ZERO;
        let outcome = outcome(reply, self.desired, now);
        if !matches!(outcome, WorkerRpcOutcome::Retry(_)) {
            self.failures = 0;
        }
        let step = match outcome {
            WorkerRpcOutcome::Transition(transition) => LifecycleStep::observed(Some(transition)),
            WorkerRpcOutcome::Status(transition) => {
                self.stop_sent = false;
                let state = state_of(&transition);
                if matches!(
                    state,
                    QueryFragmentState::Started | QueryFragmentState::Running
                ) {
                    self.wait_due = self.poll_interval;
                }
                if state != self.current {
                    LifecycleStep::observed(Some(transition))
                } else if self.observation_due(now) {
                    LifecycleStep::observed(None)
                } else {
                    LifecycleStep::unchanged()
                }
            }
            WorkerRpcOutcome::StopAccepted => {
                self.stop_sent = true;
                LifecycleStep::observed(None)
            }
            WorkerRpcOutcome::Failed(err) => {
                error!(error = %err, "fatal");
                LifecycleStep::failed(now, err)
            }
            WorkerRpcOutcome::Retry(err) => {
                warn!(error = %err, "retryable failure");
                self.failures += 1;
                if self.failures < MAX_CONSECUTIVE_FAILURES {
                    self.wait_due = RETRY_INTERVAL;
                    LifecycleStep::unchanged()
                } else {
                    let cause = format!("retry budget exhausted, last error: {err}");
                    error!("{cause}");
                    LifecycleStep::failed(now, QueryFragmentError::Transport { msg: cause })
                }
            }
        };
        if step.transition.is_some() || step.observed {
            self.last_written = Some(now);
        }
        if let Some(transition) = &step.transition {
            self.current = state_of(transition);
        }
        step
    }

    pub(super) fn set_desired(&mut self, desired: DesiredQueryFragmentState) {
        if desired != self.desired {
            self.wait_due = Duration::ZERO;
        }
        self.desired = desired;
    }
}

const fn state_of(transition: &QueryFragmentTransition) -> QueryFragmentState {
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
        // The worker has only accepted the fragment, so it is Started, not Running,
        // until a status read reports it running.
        // An already accepted start means that an earlier start reached the worker but its reply was lost.
        // Starting again cannot succeed, so the fragment counts as started in that case too.
        WorkerReply::Start(Ok(()) | Err(CallError::AlreadyExists)) => {
            WorkerRpcOutcome::Transition(QueryFragmentTransition::Started)
        }
        // Only a start can find the id already accepted, so a working worker never sends this reply to a stop or a status read.
        WorkerReply::Stop(Err(CallError::AlreadyExists))
        | WorkerReply::Status(Err(CallError::AlreadyExists)) => {
            WorkerRpcOutcome::Failed(QueryFragmentError::Internal(CodedError::new(
                ErrorCode::QueryAlreadyRegistered,
                "worker reported the fragment as already registered outside a start",
            )))
        }
        // A start has nothing to look up, so a working worker never sends this reply.
        // Fail the fragment, because repeating the call would loop without a limit.
        WorkerReply::Start(Err(CallError::NotFound)) => {
            WorkerRpcOutcome::Failed(QueryFragmentError::Internal(CodedError::new(
                ErrorCode::QueryNotFound,
                "worker reported the fragment as unknown on start",
            )))
        }
        WorkerReply::Stop(Ok(())) => WorkerRpcOutcome::StopAccepted,
        // The worker not knowing the fragment only says that it does not run there now.
        // An earlier start with an unknown outcome can still take effect afterwards;
        // the per-worker comparison of active fragments with the catalog stops it then.
        WorkerReply::Stop(Err(CallError::NotFound)) => WorkerRpcOutcome::Transition(stopped()),
        WorkerReply::Status(Ok(raw)) => match status_to_transition(raw, desired, now) {
            Ok(transition) => WorkerRpcOutcome::Status(transition),
            // An unknown state value is read again, not acted on,
            // because acting on it would change the fragment's state on a guess.
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

fn unix_ms_to_datetime(ms: u64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(i64::try_from(ms).unwrap_or(i64::MAX)).unwrap_or_default()
}

/// Fails with the state value if this build does not know it.
fn status_to_transition(
    raw: RawStatus,
    desired: DesiredQueryFragmentState,
    now: DateTime<Utc>,
) -> Result<QueryFragmentTransition, i32> {
    let reported = QueryFragmentState::try_from(raw.state)?;
    let state = if reported == QueryFragmentState::Completed
        && desired == DesiredQueryFragmentState::Stopped
    {
        QueryFragmentState::Stopped
    } else {
        reported
    };
    let start = raw.start_ms.map(unix_ms_to_datetime);
    let stop = raw.stop_ms.map(unix_ms_to_datetime);
    let fallback = |field: &str| {
        warn!("worker reported {state} without {field}; using current time");
        now
    };
    Ok(match state {
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
            error: raw.error.unwrap_or_else(|| {
                warn!("worker reported Failed without error; using a generic error");
                QueryFragmentError::Transport {
                    msg: "worker reported failure without detail".to_string(),
                }
            }),
        },
    })
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    const POLL: Duration = Duration::from_millis(25);
    const UNKNOWN_STATE: i32 = 99;

    use DesiredQueryFragmentState::{Completed as WantCompleted, Stopped as WantStopped};
    use QueryFragmentState::{Completed, Pending, Running, Started, Stopped};

    fn lifecycle(
        current: QueryFragmentState,
        desired: DesiredQueryFragmentState,
    ) -> QueryFragmentLifecycle {
        QueryFragmentLifecycle::new(current, desired, POLL)
    }

    pub(in crate::query_fragment) fn transient(msg: &str) -> CallError {
        CallError::Transient(QueryFragmentError::Transport {
            msg: msg.to_string(),
        })
    }

    fn wire(state: QueryFragmentState) -> i32 {
        (0..16)
            .find(|value| QueryFragmentState::try_from(*value) == Ok(state))
            .unwrap()
    }

    pub(in crate::query_fragment) fn raw(
        state: QueryFragmentState,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> RawStatus {
        RawStatus {
            state: wire(state),
            start_ms: start,
            stop_ms: stop,
            error: None,
        }
    }

    fn apply(lifecycle: &mut QueryFragmentLifecycle, reply: WorkerReply) -> LifecycleStep {
        lifecycle.apply(reply, Utc::now())
    }

    #[test]
    fn a_fresh_lifecycle_starts_with_the_call_for_its_states() {
        use LifecycleAction::{Call, Done};
        use WorkerCall::{Start, Status, Stop};
        for (current, desired, expected) in [
            (Pending, WantCompleted, Call(Start)),
            (Started, WantCompleted, Call(Status)),
            (Running, WantCompleted, Call(Status)),
            (Pending, WantStopped, Call(Stop)),
            (Running, WantStopped, Call(Stop)),
            (Completed, WantCompleted, Done),
            (Stopped, WantStopped, Done),
            (QueryFragmentState::Failed, WantStopped, Done),
        ] {
            assert_eq!(
                lifecycle(current, desired).next(),
                expected,
                "{current} {desired:?}"
            );
        }
    }

    #[test]
    fn a_start_or_an_already_accepted_start_is_followed_by_an_observation_without_a_wait() {
        for reply in [Ok(()), Err(CallError::AlreadyExists)] {
            let mut lifecycle = lifecycle(Pending, WantCompleted);
            let step = apply(&mut lifecycle, WorkerReply::Start(reply));
            assert!(matches!(
                step.transition,
                Some(QueryFragmentTransition::Started)
            ));
            assert!(step.observed);
            assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Status));
        }
    }

    #[test]
    fn a_reply_that_a_working_worker_never_sends_fails_the_fragment() {
        for (current, reply, code) in [
            (
                Pending,
                WorkerReply::Start(Err(CallError::NotFound)),
                ErrorCode::QueryNotFound,
            ),
            (
                Running,
                WorkerReply::Status(Err(CallError::AlreadyExists)),
                ErrorCode::QueryAlreadyRegistered,
            ),
        ] {
            let mut lifecycle = lifecycle(current, WantCompleted);
            let step = apply(&mut lifecycle, reply);
            assert!(matches!(
                step.transition,
                Some(QueryFragmentTransition::Failed { error: QueryFragmentError::Internal(coded), .. })
                    if coded.code == code
            ));
            assert_eq!(lifecycle.next(), LifecycleAction::Done);
        }
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
        for reply in [
            WorkerReply::Stop(Err(CallError::NotFound)),
            WorkerReply::Status(Err(CallError::NotFound)),
        ] {
            let mut lifecycle = lifecycle(Running, WantStopped);
            let step = apply(&mut lifecycle, reply);
            assert!(matches!(
                step.transition,
                Some(QueryFragmentTransition::Stopped { .. })
            ));
            assert_eq!(lifecycle.next(), LifecycleAction::Done);
        }
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
        assert_eq!(lifecycle.next(), LifecycleAction::Wait(RETRY_INTERVAL));
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Status));
    }

    #[test]
    fn a_stop_request_turns_the_next_call_into_a_stop_without_a_wait() {
        let mut lifecycle = lifecycle(Started, WantCompleted);
        apply(
            &mut lifecycle,
            WorkerReply::Status(Ok(raw(Running, Some(1), None))),
        );
        lifecycle.set_desired(WantStopped);
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Stop));
    }

    #[test]
    fn a_repeated_status_writes_only_an_observation_and_only_once_per_interval() {
        let mut lifecycle = lifecycle(Running, WantCompleted);
        let start = Utc::now();
        let report = || WorkerReply::Status(Ok(raw(Running, Some(1), None)));

        let first = lifecycle.apply(report(), start);
        assert!(first.observed && first.transition.is_none());

        let within = lifecycle.apply(report(), start + chrono::Duration::milliseconds(50));
        assert!(!within.observed && within.transition.is_none());

        let after = lifecycle.apply(report(), start + OBSERVATION_WRITE_INTERVAL);
        assert!(after.observed && after.transition.is_none());
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
        assert_eq!(lifecycle.next(), LifecycleAction::Wait(RETRY_INTERVAL));
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Stop));
    }

    #[test]
    fn an_accepted_stop_is_sent_again_while_the_fragment_is_reported_live() {
        let mut lifecycle = lifecycle(Started, WantStopped);
        let step = apply(&mut lifecycle, WorkerReply::Stop(Ok(())));
        assert!(step.transition.is_none());
        assert!(step.observed);
        assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Status));
        for live in [raw(Started, None, None), raw(Running, Some(1), None)] {
            apply(&mut lifecycle, WorkerReply::Status(Ok(live)));
            assert_eq!(lifecycle.next(), LifecycleAction::Wait(POLL));
            assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Stop));
            apply(&mut lifecycle, WorkerReply::Stop(Ok(())));
            assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Status));
        }
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
    fn retryable_failures_count_until_the_budget_is_spent() {
        let mut lifecycle = lifecycle(Pending, WantCompleted);
        for _ in 1..MAX_CONSECUTIVE_FAILURES {
            let step = apply(
                &mut lifecycle,
                WorkerReply::Start(Err(transient("worker busy"))),
            );
            assert!(step.transition.is_none());
            assert_eq!(lifecycle.next(), LifecycleAction::Wait(RETRY_INTERVAL));
            assert_eq!(lifecycle.next(), LifecycleAction::Call(WorkerCall::Start));
        }
        let step = apply(
            &mut lifecycle,
            WorkerReply::Start(Err(transient("worker busy"))),
        );
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Failed { error: QueryFragmentError::Transport { msg }, .. })
                if msg.starts_with("retry budget exhausted, last error: ") && msg.contains("worker busy")
        ));
        assert_eq!(lifecycle.next(), LifecycleAction::Done);
    }

    #[test]
    fn every_kind_of_success_refills_the_budget() {
        let mut lifecycle = lifecycle(Started, WantCompleted);
        let busy = || WorkerReply::Status(Err(transient("worker busy")));
        for success in [
            WorkerReply::Start(Ok(())),
            WorkerReply::Status(Ok(raw(Started, None, None))),
            WorkerReply::Stop(Ok(())),
        ] {
            for _ in 1..MAX_CONSECUTIVE_FAILURES {
                assert!(apply(&mut lifecycle, busy()).transition.is_none());
            }
            apply(&mut lifecycle, success);
        }
        assert!(apply(&mut lifecycle, busy()).transition.is_none());
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
        assert!(matches!(
            step.transition,
            Some(QueryFragmentTransition::Failed { stop_timestamp, error: QueryFragmentError::Transport { msg }, .. })
                if stop_timestamp == now && msg == "crashed"
        ));
        assert_eq!(lifecycle.next(), LifecycleAction::Done);
    }

    #[test]
    fn a_report_missing_a_timestamp_gets_the_current_time() {
        let now = Utc::now();
        assert!(matches!(
            status_to_transition(raw(Running, None, None), WantCompleted, now),
            Ok(QueryFragmentTransition::Running { start_timestamp }) if start_timestamp == now
        ));
        assert!(matches!(
            status_to_transition(raw(Completed, Some(1_000), None), WantCompleted, now),
            Ok(QueryFragmentTransition::Completed { start_timestamp: Some(_), stop_timestamp }) if stop_timestamp == now
        ));
    }

    #[test]
    fn a_failure_reported_without_an_error_gets_a_generic_one() {
        let transition = status_to_transition(
            raw(QueryFragmentState::Failed, None, Some(2_000)),
            WantCompleted,
            Utc::now(),
        );
        assert!(matches!(
            transition,
            Ok(QueryFragmentTransition::Failed { error: QueryFragmentError::Transport { msg }, .. })
                if msg == "worker reported failure without detail"
        ));
    }
}
