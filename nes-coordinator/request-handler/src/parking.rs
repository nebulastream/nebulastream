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
use model::identifier::QueryId;
use model::query::QueryWithFragments;
use model::query::query_fragment::QueryFragmentState;
use model::query::query_state::QueryState;
use model::request::{Until, Watched};
use model::worker::WorkerState;
use model::worker::endpoint::NetworkAddr;
use std::collections::{BTreeSet, HashMap};
use tokio::time::Instant;

/// One wake-up's catalog read for the parked replies.
pub(super) struct Observation {
    /// Deadlines are compared against this time.
    pub at: Instant,
    pub queries: HashMap<QueryId, QueryWithFragments>,
    pub worker_states: HashMap<NetworkAddr, WorkerState>,
}

/// What a parked reply waits for.
#[derive(Clone, Copy)]
pub(super) enum Condition {
    Running,
    Terminated,
    /// Observations count as fresh from this time on.
    FreshSince(DateTime<Utc>),
}

impl Condition {
    pub fn new(until: Until, submitted_at: DateTime<Utc>) -> Self {
        match until {
            Until::Running => Self::Running,
            Until::Terminated => Self::Terminated,
            Until::Fresh => Self::FreshSince(submitted_at),
        }
    }
}

/// The replies that wait for their condition or their deadline.
pub(super) struct ParkedReplies<R>(Vec<ParkedReply<R>>);

impl<R> Default for ParkedReplies<R> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

pub(super) struct ParkedReply<R> {
    ids: Vec<QueryId>,
    condition: Condition,
    deadline: Option<Instant>,
    reply_to: R,
}

impl<R> ParkedReplies<R> {
    pub fn park(
        &mut self,
        ids: Vec<QueryId>,
        condition: Condition,
        deadline: Option<Instant>,
        reply_to: R,
    ) {
        self.0.push(ParkedReply {
            ids,
            condition,
            deadline,
            reply_to,
        });
    }

    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Several replies can wait on the same query, and every listed id is one SQL parameter,
    /// so each id is returned once.
    pub fn query_ids(&self) -> Vec<QueryId> {
        self.0
            .iter()
            .flat_map(|entry| entry.ids.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.0.iter().filter_map(|entry| entry.deadline).min()
    }

    /// Used when the catalog read failed: no condition can be checked, so only the expired entries are removed.
    pub fn take_expired(&mut self, now: Instant) -> Vec<R> {
        self.0
            .extract_if(.., |entry| entry.expired(now))
            .map(|entry| entry.reply_to)
            .collect()
    }

    /// Removes the entries whose condition holds or whose deadline has passed, and returns each with its reply.
    pub fn release(&mut self, observation: &Observation) -> Vec<(R, Watched)> {
        let mut released = Vec::new();
        for entry in std::mem::take(&mut self.0) {
            match entry.outcome(observation) {
                Some(watched) => released.push((entry.reply_to, watched)),
                None => self.0.push(entry),
            }
        }
        released
    }
}

impl<R> ParkedReply<R> {
    fn expired(&self, now: Instant) -> bool {
        self.deadline.is_some_and(|deadline| now >= deadline)
    }

    fn outcome(&self, observation: &Observation) -> Option<Watched> {
        let rows = || self.ids.iter().filter_map(|id| observation.queries.get(id));
        // A query that is missing from the observation satisfies no state condition,
        // so its reply waits for the deadline, or indefinitely without one.
        let all_in = |accepted: fn(QueryState) -> bool| {
            self.ids.iter().all(|id| {
                observation
                    .queries
                    .get(id)
                    .is_some_and(|row| accepted(row.query.state))
            })
        };
        let satisfied = match self.condition {
            Condition::Running => {
                all_in(|state| state == QueryState::Running || state.is_terminal())
            }
            Condition::Terminated => all_in(|state| state.is_terminal()),
            // An observed fragment counts only once it runs.
            // A fragment that is still starting on an active host (after a redeploy, say) keeps the reply waiting until it runs or terminates.
            Condition::FreshSince(submitted_at) => rows().flat_map(|row| &row.fragments).all(|f| {
                f.current_state.is_terminal()
                    || observation.worker_states.get(&f.host_addr) != Some(&WorkerState::Active)
                    || (f.current_state == QueryFragmentState::Running
                        && f.last_observed_at.is_some_and(|t| t >= submitted_at))
            }),
        };
        let expired = self.expired(observation.at);
        (satisfied || expired).then(|| Watched {
            rows: rows().cloned().collect(),
            expired: !satisfied,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::identifier::QueryFragmentId;
    use model::query;
    use model::query::query_fragment::{self, DesiredQueryFragmentState};
    use std::time::Duration;

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

    fn with_fragment(
        mut row: QueryWithFragments,
        state: QueryFragmentState,
        last_observed_at: Option<DateTime<Utc>>,
    ) -> QueryWithFragments {
        let id = i64::try_from(row.fragments.len()).unwrap() + 1;
        row.fragments.push(query_fragment::Model {
            id: QueryFragmentId::new(id),
            query_id: row.query.id,
            host_addr: host(),
            plan: Vec::new(),
            num_operators: 1,
            has_source: true,
            current_state: state,
            desired_state: DesiredQueryFragmentState::Completed,
            start_timestamp: None,
            stop_timestamp: None,
            error: None,
            last_observed_at,
        });
        row
    }

    fn running_on_host(last_observed_at: Option<DateTime<Utc>>) -> QueryWithFragments {
        with_fragment(
            row(1, QueryState::Running),
            QueryFragmentState::Running,
            last_observed_at,
        )
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

    fn active() -> Vec<(NetworkAddr, WorkerState)> {
        vec![(host(), WorkerState::Active)]
    }

    fn ids(ids: &[i64]) -> Vec<QueryId> {
        ids.iter().copied().map(QueryId::new).collect()
    }

    fn parked(
        ids: Vec<QueryId>,
        condition: Condition,
        deadline: Option<Instant>,
    ) -> ParkedReplies<&'static str> {
        let mut parked = ParkedReplies::default();
        parked.park(ids, condition, deadline, "reply");
        parked
    }

    #[test]
    fn a_running_wait_is_released_by_running_or_any_terminal_state() {
        for state in [
            QueryState::Running,
            QueryState::Completed,
            QueryState::Stopped,
            QueryState::Failed,
        ] {
            let now = Instant::now();
            let mut parked = parked(ids(&[1]), Condition::Running, None);
            let released = parked.release(&observation(now, vec![row(1, state)], vec![]));
            assert!(
                matches!(released.as_slice(), [("reply", Watched { rows, expired: false })] if rows[0].query.state == state),
                "{state:?}"
            );
            assert!(parked.is_empty());
        }
    }

    #[test]
    fn a_state_wait_is_not_released_before_its_state() {
        let now = Instant::now();
        for (condition, state) in [
            (Condition::Running, QueryState::Pending),
            (Condition::Running, QueryState::Started),
            (Condition::Terminated, QueryState::Running),
        ] {
            let mut parked = parked(ids(&[1]), condition, None);
            assert!(
                parked
                    .release(&observation(now, vec![row(1, state)], vec![]))
                    .is_empty()
            );
        }
    }

    #[test]
    fn a_state_wait_needs_every_query() {
        let now = Instant::now();
        let mut parked = parked(ids(&[1, 2]), Condition::Terminated, None);
        let one_terminated = observation(
            now,
            vec![row(1, QueryState::Stopped), row(2, QueryState::Running)],
            vec![],
        );
        assert!(parked.release(&one_terminated).is_empty());
        let both = observation(
            now,
            vec![row(1, QueryState::Stopped), row(2, QueryState::Completed)],
            vec![],
        );
        assert!(matches!(
            parked.release(&both).as_slice(),
            [("reply", Watched { rows, expired: false })] if rows.len() == 2
        ));
    }

    #[test]
    fn an_unsatisfied_wait_is_released_at_its_deadline_with_the_rows_it_last_saw() {
        for rows in [
            vec![],
            vec![row(1, QueryState::Running), row(2, QueryState::Pending)],
        ] {
            let now = Instant::now();
            let expected = rows.len();
            let mut parked = parked(ids(&[1, 2]), Condition::Running, Some(now + ONE_SECOND));
            assert!(
                parked
                    .release(&observation(now, rows.clone(), vec![]))
                    .is_empty()
            );
            let at_deadline = observation(now + ONE_SECOND, rows, vec![]);
            assert!(matches!(
                parked.release(&at_deadline).as_slice(),
                [("reply", Watched { rows, expired: true })] if rows.len() == expected
            ));
        }
    }

    #[test]
    fn a_wait_satisfied_at_its_deadline_has_not_expired() {
        let now = Instant::now();
        let submitted_at = Utc::now();
        let fresh = Some(submitted_at + chrono::Duration::seconds(1));
        for (condition, row) in [
            (Condition::Running, row(1, QueryState::Running)),
            (Condition::FreshSince(submitted_at), running_on_host(fresh)),
        ] {
            let mut parked = parked(ids(&[1]), condition, Some(now + ONE_SECOND));
            let at_deadline = observation(now + ONE_SECOND, vec![row], active());
            assert!(matches!(
                parked.release(&at_deadline).as_slice(),
                [("reply", Watched { expired: false, .. })]
            ));
        }
    }

    #[test]
    fn a_fresh_wait_needs_an_observation_after_its_submission() {
        let submitted_at = Utc::now();
        let stale = Some(submitted_at - chrono::Duration::seconds(1));
        let fresh = Some(submitted_at + chrono::Duration::seconds(1));
        let now = Instant::now();
        let mut parked = parked(ids(&[1]), Condition::FreshSince(submitted_at), None);
        let before = observation(now, vec![running_on_host(stale)], active());
        assert!(parked.release(&before).is_empty());
        let after = observation(now, vec![running_on_host(fresh)], active());
        assert!(matches!(
            parked.release(&after).as_slice(),
            [("reply", Watched { expired: false, .. })]
        ));
    }

    #[test]
    fn a_fresh_wait_keeps_waiting_for_a_fragment_that_is_still_starting() {
        let submitted_at = Utc::now();
        let fresh = Some(submitted_at + chrono::Duration::seconds(1));
        let stale = Some(submitted_at - chrono::Duration::seconds(1));
        let now = Instant::now();
        let starting = with_fragment(
            row(1, QueryState::Started),
            QueryFragmentState::Started,
            fresh,
        );
        let one_stale = with_fragment(running_on_host(fresh), QueryFragmentState::Running, stale);
        for row in [starting, one_stale] {
            let mut parked = parked(ids(&[1]), Condition::FreshSince(submitted_at), None);
            assert!(
                parked
                    .release(&observation(now, vec![row], active()))
                    .is_empty()
            );
        }
    }

    #[test]
    fn a_fresh_wait_does_not_wait_for_fragments_that_cannot_be_updated() {
        let submitted_at = Utc::now();
        let stale = Some(submitted_at - chrono::Duration::seconds(1));
        let now = Instant::now();
        let terminated = with_fragment(
            row(1, QueryState::Failed),
            QueryFragmentState::Failed,
            stale,
        );
        let cases = [
            (terminated, active()),
            (
                running_on_host(None),
                vec![(host(), WorkerState::Unreachable)],
            ),
            (running_on_host(None), vec![]),
            (row(1, QueryState::Pending), vec![]),
        ];
        for (row, workers) in cases {
            let mut parked = parked(ids(&[1]), Condition::FreshSince(submitted_at), None);
            assert!(matches!(
                parked
                    .release(&observation(now, vec![row], workers))
                    .as_slice(),
                [("reply", Watched { expired: false, .. })]
            ));
        }
    }

    #[test]
    fn a_fresh_wait_with_a_missing_row_is_released_with_the_rows_it_found() {
        let now = Instant::now();
        let mut parked = parked(ids(&[1, 2]), Condition::FreshSince(Utc::now()), None);
        let one = observation(now, vec![row(1, QueryState::Pending)], vec![]);
        assert!(matches!(
            parked.release(&one).as_slice(),
            [("reply", Watched { rows, expired: false })] if rows.len() == 1
        ));
    }

    #[test]
    fn replies_on_the_same_query_are_released_by_their_own_condition() {
        let now = Instant::now();
        let mut parked = parked(ids(&[1]), Condition::Running, None);
        parked.park(ids(&[1]), Condition::Terminated, None, "terminated");
        parked.park(ids(&[1]), Condition::Running, None, "second");
        let running = observation(now, vec![row(1, QueryState::Running)], vec![]);
        let released: Vec<_> = parked
            .release(&running)
            .into_iter()
            .map(|(reply, _)| reply)
            .collect();
        assert_eq!(released, vec!["reply", "second"]);
        assert_eq!(parked.query_ids(), ids(&[1]));
    }

    #[test]
    fn release_keeps_the_unready_entries_and_reports_the_earliest_deadline() {
        let now = Instant::now();
        let mut parked = ParkedReplies::default();
        parked.park(
            ids(&[1]),
            Condition::Running,
            Some(now + 5 * ONE_SECOND),
            "running",
        );
        parked.park(
            ids(&[2]),
            Condition::Terminated,
            Some(now + 2 * ONE_SECOND),
            "terminated",
        );
        parked.park(ids(&[3]), Condition::Running, None, "ready");
        assert_eq!(parked.next_deadline(), Some(now + 2 * ONE_SECOND));

        let nothing_moved = observation(
            now,
            vec![
                row(1, QueryState::Pending),
                row(2, QueryState::Running),
                row(3, QueryState::Running),
            ],
            vec![],
        );
        assert!(matches!(
            parked.release(&nothing_moved).as_slice(),
            [("ready", _)]
        ));
        assert_eq!(parked.query_ids(), ids(&[1, 2]));
    }

    #[test]
    fn only_expired_entries_are_taken_without_an_observation() {
        let now = Instant::now();
        let mut parked = parked(ids(&[1]), Condition::Running, Some(now + ONE_SECOND));
        parked.park(ids(&[2]), Condition::Terminated, None, "unbounded");
        assert!(parked.take_expired(now).is_empty());
        assert_eq!(parked.take_expired(now + ONE_SECOND), vec!["reply"]);
        assert_eq!(parked.query_ids(), ids(&[2]));
    }

    #[test]
    fn query_ids_lists_each_id_once() {
        let mut parked = parked(ids(&[2, 1]), Condition::Running, None);
        parked.park(ids(&[1]), Condition::Terminated, None, "second");
        assert_eq!(parked.query_ids(), ids(&[1, 2]));
    }
}
