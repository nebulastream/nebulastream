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
use model::request::Watched;
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
pub(super) enum Condition {
    Running,
    Terminated,
    /// Observations count as fresh from this time on.
    /// For a watch submitted at 12:00:00, a fragment observed at 12:00:00 or later counts, and one observed at 11:59:59 does not.
    FreshSince(DateTime<Utc>),
}

/// Replies that wait for their condition or deadline.
pub(super) struct ParkedReplies<R>(Vec<ParkedReply<R>>);

impl<R> Default for ParkedReplies<R> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

struct ParkedReply<R> {
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

    /// Returns each parked query id once, in ascending order.
    /// For replies parked on `[2, 1]` and `[1]`, it returns `[1, 2]`.
    /// A client that stopped waiting, for example after its own timeout, would otherwise leave its reply parked
    /// until the condition holds or the deadline passes, which may be never.
    pub fn discard(&mut self, closed: impl Fn(&R) -> bool) {
        self.0.retain(|entry| !closed(&entry.reply_to));
    }

    pub fn query_ids(&self) -> Vec<QueryId> {
        // Several replies can wait on the same query, and every listed id is one SQL parameter.
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

    pub fn take_expired(&mut self, now: Instant) -> Vec<R> {
        self.0
            .extract_if(.., |entry| entry.expired(now))
            .map(|entry| entry.reply_to)
            .collect()
    }

    /// Removes the ready entries and returns each with its reply.
    /// An entry is ready when its condition holds or its deadline has passed.
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
        // Only ids that were read at submission are parked, so a missing row was deleted after a drop.
        // The catalog deletes a dropped query only once every fragment has terminated.
        let all_in = |accepted: fn(QueryState) -> bool| rows().all(|row| accepted(row.query.state));
        let satisfied = match self.condition {
            Condition::Running => {
                all_in(|state| state == QueryState::Running || state.is_terminal())
            }
            Condition::Terminated => all_in(|state| state.is_terminal()),
            // A fragment that is still starting on an active host (for example, after a redeploy)
            // keeps the reply waiting until it runs or terminates.
            Condition::FreshSince(submitted_at) => rows().flat_map(|row| &row.fragments).all(|f| {
                f.current_state.is_terminal()
                    || observation.worker_states.get(&f.host_addr) != Some(&WorkerState::Active)
                    || (f.current_state == QueryFragmentState::Running
                        && f.last_observed_at.is_some_and(|t| t >= submitted_at))
            }),
        };
        (satisfied || self.expired(observation.at)).then(|| Watched {
            rows: rows().cloned().collect(),
            expired: !satisfied,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::query::query_fragment;
    use proptest::collection::vec;
    use proptest::prelude::any;
    use sea_orm::Iterable;
    use std::time::Duration;
    use test_strategy::proptest;

    const ONE_SECOND: Duration = Duration::from_secs(1);

    #[proptest]
    fn releases_by_state(mut row: QueryWithFragments) {
        let id = row.query.id;
        for state in QueryState::iter() {
            row.query.state = state;
            let terminal = matches!(
                state,
                QueryState::Completed | QueryState::Stopped | QueryState::Failed
            );
            for (condition, releases) in [
                (Condition::Running, terminal || state == QueryState::Running),
                (Condition::Terminated, terminal),
            ] {
                let mut parked = ParkedReplies::default();
                parked.park(vec![id], condition, None, ());
                let observation = Observation {
                    at: Instant::now(),
                    queries: HashMap::from([(id, row.clone())]),
                    worker_states: HashMap::new(),
                };
                let released = parked.release(&observation);
                assert_eq!(!released.is_empty(), releases, "{state:?}");
                assert!(
                    released
                        .iter()
                        .all(|((), watched)| !watched.expired && watched.rows == [row.clone()])
                );
            }
        }
    }

    #[proptest]
    fn needs_every_query(
        #[strategy(vec(any::<QueryWithFragments>(), 2..8))] rows: Vec<QueryWithFragments>,
    ) {
        let mut queries: HashMap<_, _> = rows.into_iter().map(|row| (row.query.id, row)).collect();
        for row in queries.values_mut() {
            row.query.state = QueryState::Stopped;
        }
        let ids: Vec<_> = queries.keys().copied().collect();
        queries.get_mut(&ids[0]).unwrap().query.state = QueryState::Running;
        let mut parked = ParkedReplies::default();
        parked.park(ids.clone(), Condition::Terminated, None, ());
        let mut observation = Observation {
            at: Instant::now(),
            queries,
            worker_states: HashMap::new(),
        };
        assert!(parked.release(&observation).is_empty());

        observation.queries.get_mut(&ids[0]).unwrap().query.state = QueryState::Completed;
        assert!(matches!(
            parked.release(&observation).as_slice(),
            [((), Watched { rows, expired: false })] if rows.len() == ids.len()
        ));
    }

    #[proptest]
    fn expires_with_last_rows(mut row: QueryWithFragments) {
        row.query.state = QueryState::Pending;
        let id = row.query.id;
        let now = Instant::now();
        let mut parked = ParkedReplies::default();
        parked.park(vec![id], Condition::Running, Some(now + ONE_SECOND), ());
        let mut observation = Observation {
            at: now,
            queries: HashMap::from([(id, row.clone())]),
            worker_states: HashMap::new(),
        };
        assert!(parked.release(&observation).is_empty());

        observation.at = now + ONE_SECOND;
        assert!(matches!(
            parked.release(&observation).as_slice(),
            [((), Watched { rows, expired: true })] if *rows == [row]
        ));

        parked.park(vec![id], Condition::Running, Some(now + ONE_SECOND), ());
        observation.queries.get_mut(&id).unwrap().query.state = QueryState::Running;
        assert!(matches!(
            parked.release(&observation).as_slice(),
            [((), Watched { expired: false, .. })]
        ));
    }

    /// Every fragment runs on an active host, and the extra fragment makes sure that the query has one.
    #[proptest]
    fn fresh_by_state(mut row: QueryWithFragments, fragment: query_fragment::Model) {
        row.fragments.push(query_fragment::Model {
            query_id: row.query.id,
            ..fragment
        });
        let id = row.query.id;
        let worker_states: HashMap<_, _> = row
            .fragments
            .iter()
            .map(|fragment| (fragment.host_addr.clone(), WorkerState::Active))
            .collect();
        let submitted_at = Utc::now();
        for state in QueryFragmentState::iter() {
            let terminal = matches!(
                state,
                QueryFragmentState::Completed
                    | QueryFragmentState::Stopped
                    | QueryFragmentState::Failed
            );
            for (observed_at, fresh) in [
                (submitted_at - chrono::Duration::seconds(1), false),
                (submitted_at, state == QueryFragmentState::Running),
            ] {
                for fragment in &mut row.fragments {
                    fragment.current_state = state;
                    fragment.last_observed_at = Some(observed_at);
                }
                let mut parked = ParkedReplies::default();
                parked.park(vec![id], Condition::FreshSince(submitted_at), None, ());
                let observation = Observation {
                    at: Instant::now(),
                    queries: HashMap::from([(id, row.clone())]),
                    worker_states: worker_states.clone(),
                };
                assert_eq!(
                    !parked.release(&observation).is_empty(),
                    terminal || fresh,
                    "{state:?} observed at {observed_at}"
                );
            }
        }
    }

    /// Every fragment runs and is never observed, and the extra fragment makes sure that the query has one.
    #[proptest]
    fn fresh_by_host(mut row: QueryWithFragments, fragment: query_fragment::Model) {
        row.fragments.push(query_fragment::Model {
            query_id: row.query.id,
            ..fragment
        });
        for fragment in &mut row.fragments {
            fragment.current_state = QueryFragmentState::Running;
            fragment.last_observed_at = None;
        }
        let id = row.query.id;
        for host in std::iter::once(None).chain(WorkerState::iter().map(Some)) {
            let worker_states = row
                .fragments
                .iter()
                .filter_map(|fragment| Some((fragment.host_addr.clone(), host?)))
                .collect();
            let mut parked = ParkedReplies::default();
            parked.park(vec![id], Condition::FreshSince(Utc::now()), None, ());
            let observation = Observation {
                at: Instant::now(),
                queries: HashMap::from([(id, row.clone())]),
                worker_states,
            };
            assert_eq!(
                parked.release(&observation).is_empty(),
                host == Some(WorkerState::Active),
                "{host:?}"
            );
        }
    }

    /// The handler parks only ids that it read, so a missing row was deleted after a drop.
    #[test]
    fn missing_row_releases() {
        let id = QueryId::new(1);
        for condition in [
            Condition::Running,
            Condition::Terminated,
            Condition::FreshSince(Utc::now()),
        ] {
            let mut parked = ParkedReplies::default();
            parked.park(vec![id], condition, None, ());
            let observation = Observation {
                at: Instant::now(),
                queries: HashMap::new(),
                worker_states: HashMap::new(),
            };
            assert!(matches!(
                parked.release(&observation).as_slice(),
                [((), Watched { rows, expired: false })] if rows.is_empty()
            ));
        }
    }

    #[proptest]
    fn keeps_unready(mut row: QueryWithFragments) {
        row.query.state = QueryState::Running;
        let id = row.query.id;
        let now = Instant::now();
        let (later, sooner) = (now + 5 * ONE_SECOND, now + 2 * ONE_SECOND);
        let mut parked = ParkedReplies::default();
        parked.park(vec![id], Condition::Running, None, "first");
        parked.park(vec![id], Condition::Terminated, Some(later), "later");
        parked.park(vec![id], Condition::Terminated, Some(sooner), "sooner");
        parked.park(vec![id], Condition::Running, None, "second");
        assert_eq!(parked.next_deadline(), Some(sooner));

        let observation = Observation {
            at: now,
            queries: HashMap::from([(id, row)]),
            worker_states: HashMap::new(),
        };
        assert!(matches!(
            parked.release(&observation).as_slice(),
            [("first", _), ("second", _)]
        ));
        assert_eq!(parked.next_deadline(), Some(sooner));
    }

    #[proptest]
    fn discards_closed(open: QueryId, closed: QueryId) {
        let mut parked = ParkedReplies::default();
        parked.park(vec![closed], Condition::Running, None, true);
        parked.park(vec![open], Condition::Running, None, false);
        parked.discard(|&is_closed| is_closed);
        assert_eq!(parked.query_ids(), [open]);
    }

    #[test]
    fn takes_only_expired() {
        let (bounded, unbounded) = (QueryId::new(1), QueryId::new(2));
        let now = Instant::now();
        let deadline = now + ONE_SECOND;
        let mut parked = ParkedReplies::default();
        parked.park(vec![bounded], Condition::Running, Some(deadline), "bounded");
        parked.park(vec![unbounded], Condition::Terminated, None, "unbounded");
        assert!(parked.take_expired(now).is_empty());
        assert_eq!(parked.take_expired(deadline), ["bounded"]);
        assert_eq!(parked.query_ids(), [unbounded]);
    }

    #[proptest]
    fn dedups_query_ids(ids: Vec<QueryId>) {
        let mut parked = ParkedReplies::default();
        parked.park(ids.clone(), Condition::Running, None, ());
        parked.park(ids.clone(), Condition::Terminated, None, ());
        let mut expected = ids;
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(parked.query_ids(), expected);
    }
}
