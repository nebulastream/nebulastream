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

//! The parking rules: which replies wait, what they wait for, and when they are released.
//! Everything here is a function over values (catalog rows, worker states, a clock reading),
//! so the rules are tested without a database or a runtime.

use chrono::{DateTime, Utc};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryId;
use model::query::QueryWithFragments;
use model::query::query_fragment::QueryFragmentState;
use model::query::query_state::QueryState;
use model::request::Wait;
use model::statement::StatementResult;
use model::worker::WorkerState;
use model::worker::endpoint::NetworkAddr;
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

/// Returned when a blocking create finds the query in a terminal state other than the one that it waited for.
/// Nothing is left to wait for, and reporting success would hide that the query never reached the requested state.
/// The query row carries the failure that ended it; the fragments come along as they do with every query read.
#[derive(Error, Debug)]
#[error("Query '{}' terminated early with state {}", .0.query.id, .0.query.state)]
pub struct EarlyTermination(pub QueryWithFragments);

/// Returned when a blocking wait's timeout elapses before its condition is met.
/// The reply cannot report success, since the query never reached the state that the caller waited for.
/// The rows come along as last observed, in the shape that a satisfied wait would have returned,
/// for a caller that answers with the current state rather than with an error.
#[derive(Debug)]
pub struct WaitTimeout(pub StatementResult);

impl std::error::Error for WaitTimeout {}

impl fmt::Display for WaitTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            StatementResult::CreatedQuery(created) => write!(
                f,
                "query '{}' was in state {} at the deadline",
                created.query.id, created.query.state
            ),
            StatementResult::DroppedQueries(queries) => {
                let unfinished = queries.iter().filter(|q| !q.state.is_terminal()).count();
                write!(
                    f,
                    "{unfinished} dropped queries had not terminated at the deadline"
                )
            }
            StatementResult::Queries(rows) => write!(
                f,
                "{} queries were still waited on at the deadline",
                rows.len()
            ),
            _ => write!(f, "the wait ended at its deadline"),
        }
    }
}

const TIMEOUT_MESSAGE: &str = "timed out waiting for the blocking condition to be satisfied";

/// The condition that holds a parked reply back, checked against its queries' catalog rows.
#[derive(Clone, Copy)]
pub(super) enum Until {
    /// A create: its query reaches this state, or terminates before it does.
    Reaches(QueryState),
    AllReach(QueryState),
    /// A drop: every query has terminated. The reply lists the queries without their fragments.
    Dropped,
    /// A read: every query has terminated. The reply lists the queries with their fragments.
    Terminated,
    /// A status read: its fragments' status has stopped changing, judged from observations made after this time.
    /// A passed deadline releases the rows as they are instead of an error.
    Converged(DateTime<Utc>),
}

/// One wake-up's view of the catalog: the rows of every query that a parked reply waits on,
/// and the state of every worker that hosts one of their fragments.
pub(super) struct Observation {
    /// When the rows were read. Deadlines are compared against it.
    pub at: Instant,
    pub queries: HashMap<QueryId, QueryWithFragments>,
    pub worker_states: HashMap<NetworkAddr, WorkerState>,
}

/// The wait that holds back one reply, resolved against the statement's own result at submission.
pub(super) struct Hold {
    pub ids: Vec<QueryId>,
    pub until: Until,
    pub timeout: Option<Duration>,
}

/// What happens to a reply once its statement has been applied to the catalog.
pub(super) enum ReplyPlan {
    Now(StatementResult),
    Later(Hold),
}

/// A wait that does not apply to the result (a state wait on a drop, say) is ignored and the reply is sent now.
/// A status read whose rows have no fragments has nothing that can still change, so it is answered now as well.
pub(super) fn plan_reply(
    result: StatementResult,
    wait: Wait,
    submitted_at: DateTime<Utc>,
) -> ReplyPlan {
    let (ids, until, timeout) = match (result, wait) {
        (StatementResult::CreatedQuery(created), Wait::UntilState { state, timeout }) => {
            (vec![created.query.id], Until::Reaches(state), timeout)
        }
        (StatementResult::DroppedQueries(queries), Wait::UntilTerminated { timeout }) => (
            queries.iter().map(|query| query.id).collect(),
            Until::Dropped,
            timeout,
        ),
        (StatementResult::Queries(rows), Wait::UntilTerminated { timeout }) => (
            rows.iter().map(|row| row.query.id).collect(),
            Until::Terminated,
            timeout,
        ),
        (StatementResult::Queries(rows), Wait::UntilState { state, timeout }) => (
            rows.iter().map(|row| row.query.id).collect(),
            Until::AllReach(state),
            timeout,
        ),
        (StatementResult::Queries(rows), Wait::Poll { timeout })
            if rows.iter().any(|row| !row.fragments.is_empty()) =>
        {
            (
                rows.iter().map(|row| row.query.id).collect(),
                Until::Converged(submitted_at),
                timeout,
            )
        }
        (result, _) => return ReplyPlan::Now(result),
    };
    ReplyPlan::Later(Hold {
        ids,
        until,
        timeout,
    })
}

/// The replies that wait for the catalog, in arrival order.
/// The reply handle is a type parameter so the tests can park a label instead of a channel.
pub(super) struct ParkedReplies<R>(pub Vec<ParkedReply<R>>);

pub(super) struct ParkedReply<R> {
    ids: Vec<QueryId>,
    until: Until,
    deadline: Option<Instant>,
    reply_to: R,
}

impl<R> ParkedReplies<R> {
    pub fn park(&mut self, hold: Hold, reply_to: R, now: Instant) {
        self.0.push(ParkedReply {
            ids: hold.ids,
            until: hold.until,
            deadline: hold.timeout.map(|timeout| now + timeout),
            reply_to,
        });
    }

    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The ids that the next observation has to cover: every query that some parked reply waits on.
    pub fn query_ids(&self) -> Vec<QueryId> {
        self.0
            .iter()
            .flat_map(|entry| entry.ids.iter().copied())
            .collect()
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.0.iter().filter_map(|entry| entry.deadline).min()
    }

    /// Returns the reply handles that are ready, with their outcomes;
    /// sending them is left to the caller so this stays free of side effects.
    pub fn release(
        &mut self,
        observation: &Observation,
    ) -> Vec<(R, anyhow::Result<StatementResult>)> {
        let mut released = Vec::new();
        for entry in std::mem::take(&mut self.0) {
            match entry.outcome(observation) {
                Some(outcome) => released.push((entry.reply_to, outcome)),
                None => self.0.push(entry),
            }
        }
        released
    }
}

impl<R> ParkedReply<R> {
    fn outcome(&self, observation: &Observation) -> Option<anyhow::Result<StatementResult>> {
        let expired = self
            .deadline
            .is_some_and(|deadline| observation.at >= deadline);
        let rows = || self.ids.iter().filter_map(|id| observation.queries.get(id));
        // A query that is missing from the observation satisfies nothing, so its reply stays parked until the deadline.
        let satisfied = match self.until {
            Until::Reaches(target) => rows()
                .next()
                .is_some_and(|created| wait_resolved(created.query.state, target)),
            Until::AllReach(target) => self.ids.iter().all(|id| {
                observation
                    .queries
                    .get(id)
                    .is_some_and(|row| wait_resolved(row.query.state, target))
            }),
            Until::Dropped | Until::Terminated => self.ids.iter().all(|id| {
                observation
                    .queries
                    .get(id)
                    .is_some_and(|row| row.query.state.is_terminal())
            }),
            // No fragment's status can still change before the caller reads it:
            // the fragment has terminated, its host is not active (so no further updates can arrive),
            // or it is running and was observed at least once after the poll was submitted.
            // A fragment that was observed but is still mid-transition (for example, redeploying onto a host that came back)
            // keeps the poll open, so the caller gets a converged state instead of a snapshot taken mid-transition.
            Until::Converged(submitted_at) => {
                expired
                    || rows().flat_map(|row| &row.fragments).all(|f| {
                        f.current_state.is_terminal()
                            || observation.worker_states.get(&f.host_addr)
                                != Some(&WorkerState::Active)
                            || (f.current_state == QueryFragmentState::Running
                                && f.last_observed_at.is_some_and(|t| t >= submitted_at))
                    })
            }
        };
        if !satisfied {
            return expired.then(|| Err(self.timeout(observation)));
        }
        Some(match (self.until, self.current(observation)?) {
            (Until::Reaches(target), StatementResult::CreatedQuery(created)) => {
                // Only abnormal terminations (Stopped, Failed) count as early.
                // Reaching Completed past a less advanced wait target is still a success from the caller's point of view.
                let early_termination = matches!(
                    created.query.state,
                    QueryState::Stopped | QueryState::Failed
                ) && created.query.state != target;
                if early_termination {
                    Err(anyhow::Error::new(EarlyTermination(created)))
                } else {
                    Ok(StatementResult::CreatedQuery(created))
                }
            }
            (_, result) => Ok(result),
        })
    }

    fn current(&self, observation: &Observation) -> Option<StatementResult> {
        let rows = || self.ids.iter().filter_map(|id| observation.queries.get(id));
        Some(match self.until {
            Until::Reaches(_) => StatementResult::CreatedQuery(rows().next()?.clone()),
            Until::Dropped => {
                StatementResult::DroppedQueries(rows().map(|row| row.query.clone()).collect())
            }
            Until::AllReach(_) | Until::Terminated | Until::Converged(_) => {
                StatementResult::Queries(rows().cloned().collect())
            }
        })
    }

    fn timeout(&self, observation: &Observation) -> anyhow::Error {
        let coded = CodedError::new(ErrorCode::QueryWaitTimeout, TIMEOUT_MESSAGE);
        match self.current(observation) {
            Some(result) => anyhow::Error::new(WaitTimeout(result)).context(coded),
            None => anyhow::Error::new(coded),
        }
    }
}

/// True once the wait can stop parking: `current` has reached `target`,
/// or has terminated some other way and cannot make further progress toward it.
/// A terminal state that is not the target still resolves the wait; the caller decides whether that is a success.
pub(super) fn wait_resolved(current: QueryState, target: QueryState) -> bool {
    matches!(
        (current, target),
        (QueryState::Pending, QueryState::Pending)
            | (
                QueryState::Started,
                QueryState::Pending | QueryState::Started
            )
            | (
                QueryState::Running,
                QueryState::Pending | QueryState::Started | QueryState::Running
            )
    ) || current.is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::identifier::QueryFragmentId;
    use model::query;
    use model::query::query_fragment::{self, DesiredQueryFragmentState};

    const UNTIL_RUNNING: Wait = Wait::UntilState {
        state: QueryState::Running,
        timeout: None,
    };
    const ONE_SECOND: Duration = Duration::from_secs(1);

    fn row(id: i64, state: QueryState) -> QueryWithFragments {
        QueryWithFragments {
            query: query::Model {
                id: QueryId::new(id),
                name: None,
                sql: String::new(),
                state,
                start_timestamp: None,
                stop_timestamp: None,
                error: None,
            },
            fragments: Vec::new(),
        }
    }

    fn running_on(
        host: &NetworkAddr,
        last_observed_at: Option<DateTime<Utc>>,
    ) -> QueryWithFragments {
        let mut row = row(1, QueryState::Running);
        row.fragments.push(query_fragment::Model {
            id: QueryFragmentId::new(1),
            query_id: QueryId::new(1),
            host_addr: host.clone(),
            plan: Vec::new(),
            num_operators: 1,
            has_source: true,
            current_state: QueryFragmentState::Running,
            desired_state: DesiredQueryFragmentState::Completed,
            start_timestamp: None,
            stop_timestamp: None,
            error: None,
            last_observed_at,
        });
        row
    }

    fn observation(
        at: Instant,
        rows: Vec<QueryWithFragments>,
        workers: Vec<(NetworkAddr, WorkerState)>,
    ) -> Observation {
        Observation {
            at,
            queries: rows.into_iter().map(|row| (row.query.id, row)).collect(),
            worker_states: workers.into_iter().collect(),
        }
    }

    fn host() -> NetworkAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    fn parked(
        ids: Vec<QueryId>,
        until: Until,
        timeout: Option<Duration>,
        now: Instant,
    ) -> ParkedReplies<&'static str> {
        let mut parked = ParkedReplies(Vec::new());
        parked.park(
            Hold {
                ids,
                until,
                timeout,
            },
            "reply",
            now,
        );
        parked
    }

    fn is_timeout(err: &anyhow::Error) -> bool {
        err.downcast_ref::<CodedError>()
            .is_some_and(|coded| coded.code == ErrorCode::QueryWaitTimeout)
    }

    #[test]
    fn a_wait_that_does_not_fit_the_result_replies_now() {
        let dropped = StatementResult::DroppedQueries(vec![row(1, QueryState::Running).query]);
        assert!(matches!(
            plan_reply(dropped, UNTIL_RUNNING, Utc::now()),
            ReplyPlan::Now(_)
        ));
        let created = StatementResult::CreatedQuery(row(1, QueryState::Pending));
        assert!(matches!(
            plan_reply(created, Wait::None, Utc::now()),
            ReplyPlan::Now(_)
        ));
    }

    #[test]
    fn a_status_poll_without_fragments_replies_now() {
        let queries = StatementResult::Queries(vec![row(1, QueryState::Running)]);
        assert!(matches!(
            plan_reply(queries, Wait::Poll { timeout: None }, Utc::now()),
            ReplyPlan::Now(_)
        ));
    }

    #[test]
    fn a_blocking_create_parks_on_its_query() {
        let created = StatementResult::CreatedQuery(row(7, QueryState::Pending));
        let ReplyPlan::Later(Hold {
            ids,
            until,
            timeout,
        }) = plan_reply(created, UNTIL_RUNNING, Utc::now())
        else {
            panic!("expected the reply to be parked");
        };
        assert_eq!(ids, vec![QueryId::new(7)]);
        assert!(matches!(until, Until::Reaches(QueryState::Running)));
        assert_eq!(timeout, None);
    }

    #[test]
    fn a_create_is_released_at_its_target_state_or_past_it() {
        for state in [QueryState::Running, QueryState::Completed] {
            let now = Instant::now();
            let mut parked = parked(
                vec![QueryId::new(1)],
                Until::Reaches(QueryState::Running),
                None,
                now,
            );
            let released = parked.release(&observation(now, vec![row(1, state)], vec![]));
            assert!(matches!(
                released.as_slice(),
                [("reply", Ok(StatementResult::CreatedQuery(_)))]
            ));
            assert!(parked.is_empty());
        }
    }

    #[test]
    fn a_create_is_released_with_an_error_when_its_query_terminates_abnormally() {
        for state in [QueryState::Stopped, QueryState::Failed] {
            let now = Instant::now();
            let mut parked = parked(
                vec![QueryId::new(1)],
                Until::Reaches(QueryState::Completed),
                None,
                now,
            );
            let released = parked.release(&observation(now, vec![row(1, state)], vec![]));
            assert!(matches!(
                released.as_slice(),
                [("reply", Err(e))] if e.is::<EarlyTermination>()
            ));
        }
    }

    #[test]
    fn a_wait_for_a_terminal_state_is_satisfied_by_that_state() {
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1)],
            Until::Reaches(QueryState::Stopped),
            None,
            now,
        );
        let released = parked.release(&observation(now, vec![row(1, QueryState::Stopped)], vec![]));
        assert!(matches!(
            released.as_slice(),
            [("reply", Ok(StatementResult::CreatedQuery(_)))]
        ));
    }

    #[test]
    fn a_missing_row_stays_parked_until_the_deadline() {
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1)],
            Until::Reaches(QueryState::Running),
            Some(ONE_SECOND),
            now,
        );
        assert!(parked.release(&observation(now, vec![], vec![])).is_empty());
        assert!(!parked.is_empty());
        let released = parked.release(&observation(now + ONE_SECOND, vec![], vec![]));
        assert!(matches!(
            released.as_slice(),
            [("reply", Err(e))] if is_timeout(e) && e.downcast_ref::<WaitTimeout>().is_none()
        ));
        assert!(parked.is_empty());
    }

    #[test]
    fn an_expired_create_reports_its_query() {
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1)],
            Until::Reaches(QueryState::Completed),
            Some(ONE_SECOND),
            now,
        );
        let expired = observation(now + ONE_SECOND, vec![row(1, QueryState::Running)], vec![]);
        let released = parked.release(&expired);
        let [("reply", Err(e))] = released.as_slice() else {
            panic!("expected one timed-out reply");
        };
        assert!(is_timeout(e));
        assert!(matches!(
            e.downcast_ref::<WaitTimeout>(),
            Some(WaitTimeout(StatementResult::CreatedQuery(created)))
                if created.query.state == QueryState::Running
        ));
        assert_eq!(
            format!("{e:#}"),
            format!("{TIMEOUT_MESSAGE}: query '1' was in state Running at the deadline")
        );
    }

    #[test]
    fn an_expired_drop_reports_its_queries_without_fragments() {
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1), QueryId::new(2)],
            Until::Dropped,
            Some(ONE_SECOND),
            now,
        );
        let expired = observation(
            now + ONE_SECOND,
            vec![row(1, QueryState::Stopped), row(2, QueryState::Running)],
            vec![],
        );
        let released = parked.release(&expired);
        let [("reply", Err(e))] = released.as_slice() else {
            panic!("expected one timed-out reply");
        };
        assert!(matches!(
            e.downcast_ref::<WaitTimeout>(),
            Some(WaitTimeout(StatementResult::DroppedQueries(queries))) if queries.len() == 2
        ));
    }

    #[test]
    fn an_expired_read_reports_the_rows_it_last_saw() {
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1)],
            Until::Terminated,
            Some(ONE_SECOND),
            now,
        );
        let expired = observation(now + ONE_SECOND, vec![running_on(&host(), None)], vec![]);
        let released = parked.release(&expired);
        let [("reply", Err(e))] = released.as_slice() else {
            panic!("expected one timed-out reply");
        };
        assert!(is_timeout(e));
        assert!(matches!(
            e.downcast_ref::<WaitTimeout>(),
            Some(WaitTimeout(StatementResult::Queries(rows)))
                if rows.len() == 1 && rows[0].fragments.len() == 1
        ));
    }

    #[test]
    fn a_read_with_a_state_wait_parks_on_every_query() {
        let queries = StatementResult::Queries(vec![
            row(1, QueryState::Pending),
            row(2, QueryState::Pending),
        ]);
        let ReplyPlan::Later(Hold { ids, until, .. }) =
            plan_reply(queries, UNTIL_RUNNING, Utc::now())
        else {
            panic!("expected the reply to be parked");
        };
        assert_eq!(ids, vec![QueryId::new(1), QueryId::new(2)]);
        assert!(matches!(until, Until::AllReach(QueryState::Running)));
    }

    #[test]
    fn a_state_wait_on_a_read_needs_every_query() {
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1), QueryId::new(2)],
            Until::AllReach(QueryState::Running),
            None,
            now,
        );
        let half_there = observation(
            now,
            vec![row(1, QueryState::Running), row(2, QueryState::Pending)],
            vec![],
        );
        assert!(parked.release(&half_there).is_empty());
        let there = observation(
            now,
            vec![row(1, QueryState::Running), row(2, QueryState::Completed)],
            vec![],
        );
        assert!(matches!(
            parked.release(&there).as_slice(),
            [("reply", Ok(StatementResult::Queries(rows)))] if rows.len() == 2
        ));
    }

    #[test]
    fn a_state_wait_on_a_read_is_released_by_a_termination_without_an_error() {
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1)],
            Until::AllReach(QueryState::Running),
            None,
            now,
        );
        let failed = observation(now, vec![row(1, QueryState::Failed)], vec![]);
        assert!(matches!(
            parked.release(&failed).as_slice(),
            [("reply", Ok(StatementResult::Queries(rows)))]
                if rows.len() == 1 && rows[0].query.state == QueryState::Failed
        ));
    }

    #[test]
    fn a_read_that_matched_nothing_is_released_at_once() {
        let now = Instant::now();
        let mut parked = parked(vec![], Until::AllReach(QueryState::Running), None, now);
        assert!(matches!(
            parked.release(&observation(now, vec![], vec![])).as_slice(),
            [("reply", Ok(StatementResult::Queries(rows)))] if rows.is_empty()
        ));
    }

    #[test]
    fn a_drop_that_matched_nothing_is_released_at_once() {
        let now = Instant::now();
        let mut parked = parked(vec![], Until::Dropped, None, now);
        let released = parked.release(&observation(now, vec![], vec![]));
        assert!(matches!(
            released.as_slice(),
            [("reply", Ok(StatementResult::DroppedQueries(dropped)))] if dropped.is_empty()
        ));
    }

    #[test]
    fn a_wait_for_termination_needs_every_query() {
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1), QueryId::new(2)],
            Until::Terminated,
            None,
            now,
        );
        let half_done = observation(
            now,
            vec![row(1, QueryState::Completed), row(2, QueryState::Running)],
            vec![],
        );
        assert!(parked.release(&half_done).is_empty());
        let done = observation(
            now,
            vec![row(1, QueryState::Completed), row(2, QueryState::Failed)],
            vec![],
        );
        assert!(matches!(
            parked.release(&done).as_slice(),
            [("reply", Ok(StatementResult::Queries(rows)))] if rows.len() == 2
        ));
    }

    #[test]
    fn a_status_poll_waits_for_an_observation_after_its_submission() {
        let submitted_at = Utc::now();
        let stale = Some(submitted_at - chrono::Duration::seconds(1));
        let fresh = Some(submitted_at + chrono::Duration::seconds(1));
        let active = vec![(host(), WorkerState::Active)];
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1)],
            Until::Converged(submitted_at),
            None,
            now,
        );
        let before = observation(now, vec![running_on(&host(), stale)], active.clone());
        assert!(parked.release(&before).is_empty());
        let after = observation(now, vec![running_on(&host(), fresh)], active);
        assert!(matches!(
            parked.release(&after).as_slice(),
            [("reply", Ok(StatementResult::Queries(_)))]
        ));
    }

    #[test]
    fn a_status_poll_is_released_once_its_host_is_not_active() {
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1)],
            Until::Converged(Utc::now()),
            None,
            now,
        );
        let unreachable = vec![(host(), WorkerState::Unreachable)];
        let never_observed = observation(now, vec![running_on(&host(), None)], unreachable);
        assert!(matches!(
            parked.release(&never_observed).as_slice(),
            [("reply", Ok(StatementResult::Queries(_)))]
        ));
    }

    #[test]
    fn a_status_poll_past_its_deadline_replies_with_the_current_rows() {
        let now = Instant::now();
        let mut parked = parked(
            vec![QueryId::new(1)],
            Until::Converged(Utc::now()),
            Some(ONE_SECOND),
            now,
        );
        let active = vec![(host(), WorkerState::Active)];
        let expired = observation(now + ONE_SECOND, vec![running_on(&host(), None)], active);
        assert!(matches!(
            parked.release(&expired).as_slice(),
            [("reply", Ok(StatementResult::Queries(rows)))] if rows.len() == 1
        ));
    }

    #[test]
    fn release_keeps_the_unready_entries_and_reports_the_earliest_deadline() {
        let now = Instant::now();
        let mut parked = ParkedReplies(Vec::new());
        parked.park(
            Hold {
                ids: vec![QueryId::new(1)],
                until: Until::Reaches(QueryState::Running),
                timeout: Some(5 * ONE_SECOND),
            },
            "create",
            now,
        );
        parked.park(
            Hold {
                ids: vec![QueryId::new(2)],
                until: Until::Terminated,
                timeout: Some(2 * ONE_SECOND),
            },
            "read",
            now,
        );
        parked.park(
            Hold {
                ids: vec![],
                until: Until::Dropped,
                timeout: None,
            },
            "drop",
            now,
        );
        assert_eq!(parked.next_deadline(), Some(now + 2 * ONE_SECOND));
        assert_eq!(parked.query_ids(), vec![QueryId::new(1), QueryId::new(2)]);

        let nothing_moved = observation(
            now,
            vec![row(1, QueryState::Pending), row(2, QueryState::Running)],
            vec![],
        );
        assert!(matches!(
            parked.release(&nothing_moved).as_slice(),
            [("drop", Ok(_))]
        ));
        assert_eq!(parked.query_ids(), vec![QueryId::new(1), QueryId::new(2)]);
        assert_eq!(parked.next_deadline(), Some(now + 2 * ONE_SECOND));
    }
}
