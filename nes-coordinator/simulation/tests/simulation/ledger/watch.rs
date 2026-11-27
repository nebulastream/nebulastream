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

//! Watches that the client sent, and the checks of each reply against the ledger.
//!
//! The checks rely only on what the client sent and what the coordinator replied:
//! a query leaves the catalog or stops only after the client dropped it,
//! and it fails only after the client dropped one of its hosts,
//! because the fake worker never reports an error and the controller retries a start for longer than any trial.

use super::{Ledger, Lifecycle};
use anyhow::Result;
use madsim::rand::seq::IteratorRandom;
use madsim::rand::{Rng, thread_rng};
use model::identifier::QueryId;
use model::query::GetQuery;
use model::query::query_state::QueryState;
use model::request::{Until, Wait, Watched};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::Instant;

/// The longest that the request handler may take past a watch's timeout, and to answer a watch that matches no query.
/// madsim sleeps at least 1 ms, and the release check reads the catalog, which takes no simulated time.
const SLACK: Duration = Duration::from_millis(10);

const MAX_WATCHED: usize = 3;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The condition held before the timeout.
    Released,
    Expired,
    /// The watch matched no query, so it replied at once.
    NoMatch,
}

/// What the ledger needs to check the reply of a sent watch.
pub struct SentWatch {
    ids: Vec<QueryId>,
    wait: Wait,
    at: Instant,
}

impl Ledger {
    /// Watches up to three queries that the client created: half of the time ones that it dropped, otherwise live ones.
    /// Watches an id that no query has when there is no such query, or one time in ten.
    /// Call it right before sending the watch, because the reply's timing is checked against this moment.
    pub fn plan_watch(&self, timeout: Duration) -> (GetQuery, Wait, SentWatch) {
        let mut rng = thread_rng();
        let dropped = rng.gen_bool(0.5);
        let count = rng.gen_range(1..=MAX_WATCHED);
        let mut ids: Vec<QueryId> = self
            .queries
            .iter()
            .filter(|(_, query)| (query.lifecycle != Lifecycle::Created) == dropped)
            .map(|(&id, _)| id)
            .choose_multiple(&mut rng, count);
        if ids.is_empty() || rng.gen_bool(0.1) {
            ids = vec![get_unknown_id()];
        }
        let wait = Wait {
            until: [Until::Running, Until::Terminated, Until::Fresh][rng.gen_range(0..3)],
            timeout: Some(timeout),
        };
        let sent = SentWatch {
            ids: ids.clone(),
            wait,
            at: Instant::now(),
        };
        (GetQuery::all().with_ids(ids), wait, sent)
    }

    /// Panics on a reply that contradicts the ledger.
    pub fn record_watch(&self, sent: SentWatch, reply: Result<Watched>) -> Outcome {
        let SentWatch { ids, wait, at } = sent;
        let elapsed = at.elapsed();
        let timeout = wait.timeout.expect("a watch without a timeout");
        let Watched { rows, expired } =
            reply.unwrap_or_else(|e| panic!("the watch of {ids:?} failed: {e:#}"));
        let context = format!(
            "the {:?} watch of {ids:?} with a timeout of {timeout:?}, replied after {elapsed:?} with expired={expired}",
            wait.until
        );

        if ids == [get_unknown_id()] {
            assert!(
                rows.is_empty() && !expired && elapsed <= SLACK,
                "{context}: a watch that matches no query replies at once with nothing"
            );
            return Outcome::NoMatch;
        }
        assert!(
            elapsed <= timeout + SLACK,
            "{context}: replied later than its timeout"
        );
        assert!(
            !expired || elapsed >= timeout,
            "{context}: expired before its timeout"
        );

        let returned: BTreeSet<QueryId> = rows.iter().map(|row| row.query.id).collect();
        assert!(
            returned.len() == rows.len() && returned.iter().all(|id| ids.contains(id)),
            "{context}: returned {returned:?}, which are not the watched queries"
        );
        for id in ids.iter().filter(|&id| !returned.contains(id)) {
            assert!(
                self.queries[id].lifecycle != Lifecycle::Created,
                "{context}: query {id} is missing, but the client never dropped it"
            );
        }
        for row in &rows {
            let id = row.query.id;
            let query = &self.queries[&id];
            match row.query.state {
                QueryState::Failed => assert!(
                    query
                        .fragments
                        .values()
                        .any(|&host| self.registrations[host].lifecycle != Lifecycle::Created),
                    "{context}: query {id} failed, but the client never dropped one of its hosts"
                ),
                QueryState::Stopped => assert!(
                    query.lifecycle != Lifecycle::Created,
                    "{context}: query {id} stopped, but the client never dropped it"
                ),
                QueryState::Completed => panic!(
                    "{context}: query {id} completed, but the fake worker never completes a fragment"
                ),
                QueryState::Pending | QueryState::Started | QueryState::Running => {}
            }
        }

        let outcome = if expired {
            Outcome::Expired
        } else {
            Outcome::Released
        };
        // The rule of Fresh depends on fragment observations and worker states that the ledger does not know,
        // so the request handler's unit tests cover it.
        let reached: fn(QueryState) -> bool = match wait.until {
            Until::Running => |state| state == QueryState::Running || state.is_terminal(),
            Until::Terminated => |state| state.is_terminal(),
            Until::Fresh => return outcome,
        };
        let met = rows.iter().all(|row| reached(row.query.state));
        assert!(
            expired != met,
            "{context}: the reply does not match its condition, the rows are {:?}",
            rows.iter()
                .map(|row| (row.query.id, row.query.state))
                .collect::<Vec<_>>()
        );
        outcome
    }
}

/// Catalog ids count up from 1, so no trial reaches this one.
fn get_unknown_id() -> QueryId {
    QueryId::new(i64::MAX)
}
