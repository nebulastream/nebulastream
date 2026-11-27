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

//! A typed client for the request handler.
//! A watch replies with rows also when its timeout passes or a query ended early,
//! and this client turns those rows into errors,
//! so every caller reports early ends and timeouts the same way.

use anyhow::{Result, anyhow, bail};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryId;
use model::query::query_state::QueryState;
use model::query::{GetQuery, QueryWithFragments, StopQuery};
use model::request::{Request, Statement, Until, Wait, Watched};
use model::statement::{ParsedStatement, StatementResult};
use std::fmt;
use std::time::Duration;
use thiserror::Error;

/// A query that stopped or failed while the caller waited for it to run or to complete.
#[derive(Error, Debug)]
#[error("query {} ended with state {} before it reached {expected}", .row.query.id, .row.query.state)]
pub struct EarlyTermination {
    pub row: QueryWithFragments,
    pub expected: QueryState,
}

/// The timeout of a wait passed before its condition held.
/// It holds the rows of the last read, so the caller can report their state or stop the queries.
/// It is returned with the code `QueryWaitTimeout`, and displays the queries that have not reached the condition:
/// `query 5 did not reach Running within 15s (state Pending)`, `queries 3, 7 did not terminate within 50ms`.
#[derive(Error, Debug)]
pub struct WaitTimeout {
    pub rows: Vec<QueryWithFragments>,
    pub until: Until,
    pub timeout: Duration,
}

impl fmt::Display for WaitTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (goal, reached): (&str, fn(QueryState) -> bool) = match self.until {
            Until::Running => ("reach Running", |state| {
                state == QueryState::Running || state.is_terminal()
            }),
            Until::Terminated => ("terminate", |state| state.is_terminal()),
            Until::Fresh => ("report a fresh status", |_| false),
        };
        let waiting: Vec<_> = self
            .rows
            .iter()
            .filter(|row| !reached(row.query.state))
            .collect();
        let timeout = self.timeout;
        match waiting.as_slice() {
            [row] => write!(
                f,
                "query {} did not {goal} within {timeout:?} (state {})",
                row.query.id, row.query.state
            ),
            rows => {
                let ids: Vec<_> = rows.iter().map(|row| row.query.id.to_string()).collect();
                write!(
                    f,
                    "queries {} did not {goal} within {timeout:?}",
                    ids.join(", ")
                )
            }
        }
    }
}

/// Sends requests to the request handler and waits for their replies.
#[derive(Clone)]
pub struct Client(async_channel::Sender<Request>);

impl Client {
    pub const fn new(sender: async_channel::Sender<Request>) -> Self {
        Self(sender)
    }

    pub async fn send(&self, statement: Statement) -> Result<StatementResult> {
        let (reply, request) = Request::new(statement);
        let shut_down = || anyhow!("coordinator shut down");
        self.0.send(request).await.map_err(|_| shut_down())?;
        reply.await.map_err(|_| shut_down())?
    }

    /// Waits on the queries that `read` matches when the request is handled, so a query created later is not part of the wait.
    /// A passed timeout is not an error: the reply is marked expired.
    pub async fn watch(&self, read: GetQuery, wait: Wait) -> Result<Watched> {
        match self.send(Statement::Watch(read, wait)).await? {
            StatementResult::Watched(watched) => Ok(watched),
            other => bail!("a watch returned {other:?}"),
        }
    }

    /// Runs SQL that has to create a query.
    /// Fails when the SQL is another statement, which is still applied.
    pub async fn create(&self, sql: String) -> Result<QueryWithFragments> {
        match self.send(Statement::Sql(sql)).await? {
            StatementResult::CreatedQuery(created) => Ok(created),
            other => bail!("the statement did not create a query: {other}"),
        }
    }

    /// Returns the ids of the matched queries before they have stopped.
    pub async fn stop_queries(&self, stop: StopQuery) -> Result<Vec<QueryId>> {
        let stop = Statement::Parsed(ParsedStatement::StopQuery(stop));
        match self.send(stop).await? {
            StatementResult::StoppedQueries(stopped) => {
                Ok(stopped.into_iter().map(|query| query.id).collect())
            }
            other => bail!("a stop returned {other:?}"),
        }
    }

    /// Fails when a query does not exist,
    /// with [`EarlyTermination`] when a query stops or fails first,
    /// and otherwise with [`WaitTimeout`] when the timeout passes.
    pub async fn await_running(
        &self,
        ids: Vec<QueryId>,
        timeout: Option<Duration>,
    ) -> Result<Vec<QueryWithFragments>> {
        self.await_state(ids, Until::Running, timeout, Some(QueryState::Running))
            .await
    }

    /// Fails when a query does not exist,
    /// with [`EarlyTermination`] when a query stops or fails,
    /// and otherwise with [`WaitTimeout`] when the timeout passes.
    pub async fn await_completed(
        &self,
        ids: Vec<QueryId>,
        timeout: Option<Duration>,
    ) -> Result<Vec<QueryWithFragments>> {
        self.await_state(ids, Until::Terminated, timeout, Some(QueryState::Completed))
            .await
    }

    /// A stopped or failed query counts as terminated, for a caller that has stopped or dropped the queries.
    /// A dropped query that was deleted has no row in the result.
    /// Fails with [`WaitTimeout`] when the timeout passes.
    pub async fn await_terminated(
        &self,
        ids: Vec<QueryId>,
        timeout: Option<Duration>,
    ) -> Result<Vec<QueryWithFragments>> {
        self.await_state(ids, Until::Terminated, timeout, None)
            .await
    }

    /// Returns the rows of the last read when the timeout passes.
    pub async fn await_fresh(
        &self,
        read: GetQuery,
        timeout: Option<Duration>,
    ) -> Result<Vec<QueryWithFragments>> {
        let wait = Wait {
            until: Until::Fresh,
            timeout,
        };
        Ok(self.watch(read, wait).await?.rows)
    }

    async fn await_state(
        &self,
        ids: Vec<QueryId>,
        until: Until,
        timeout: Option<Duration>,
        expected: Option<QueryState>,
    ) -> Result<Vec<QueryWithFragments>> {
        let wait = Wait { until, timeout };
        let watched = self
            .watch(GetQuery::all().with_ids(ids.clone()), wait)
            .await?;
        resolve_wait(&ids, wait, expected, watched)
    }
}

/// Returns the rows of a watch on `ids`, or the error of a wait for the state `expected`.
/// Without an expected state, an early end and a missing query are not errors.
fn resolve_wait(
    ids: &[QueryId],
    wait: Wait,
    expected: Option<QueryState>,
    Watched { rows, expired }: Watched,
) -> Result<Vec<QueryWithFragments>> {
    if let Some(expected) = expected {
        // The catalog deletes a dropped query once every fragment has terminated,
        // so a missing row is neither running nor completed.
        if let Some(missing) = ids
            .iter()
            .find(|&&id| rows.iter().all(|row| row.query.id != id))
        {
            bail!("query {missing} does not exist or was dropped");
        }
        if let Some(row) = rows.iter().find(|row| row.query.state.ended_early()) {
            bail!(EarlyTermination {
                row: row.clone(),
                expected,
            });
        }
    }
    match wait.timeout {
        Some(timeout) if expired => Err(anyhow!(WaitTimeout {
            rows,
            until: wait.until,
            timeout,
        })
        .context(CodedError::new(
            ErrorCode::QueryWaitTimeout,
            "the wait timed out",
        ))),
        _ => Ok(rows),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::Iterable;
    use test_strategy::proptest;

    const RUNNING: Wait = Wait {
        until: Until::Running,
        timeout: Some(Duration::from_secs(1)),
    };

    #[proptest]
    fn missing_row(id: QueryId) {
        let nothing = || Watched {
            rows: Vec::new(),
            expired: false,
        };
        let error = resolve_wait(&[id], RUNNING, Some(QueryState::Running), nothing()).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("query {id} does not exist or was dropped")
        );
        assert!(
            resolve_wait(&[id], RUNNING, None, nothing())
                .unwrap()
                .is_empty()
        );
    }

    #[proptest]
    fn early_end(mut row: QueryWithFragments, expired: bool) {
        for state in QueryState::iter() {
            row.query.state = state;
            let ids = [row.query.id];
            let watched = || Watched {
                rows: vec![row.clone()],
                expired,
            };
            let ended = resolve_wait(&ids, RUNNING, Some(QueryState::Running), watched())
                .is_err_and(|error| error.is::<EarlyTermination>());
            assert_eq!(ended, state.ended_early(), "{state}");
            let ended = resolve_wait(&ids, RUNNING, None, watched())
                .is_err_and(|error| error.is::<EarlyTermination>());
            assert!(!ended, "{state}");
        }
    }

    #[proptest]
    fn expired_wait(mut row: QueryWithFragments) {
        row.query.state = QueryState::Running;
        let ids = [row.query.id];
        let watched = |expired| Watched {
            rows: vec![row.clone()],
            expired,
        };
        let error = resolve_wait(&ids, RUNNING, None, watched(true)).unwrap_err();
        let code = error.downcast_ref::<CodedError>().map(|coded| coded.code);
        assert_eq!(code, Some(ErrorCode::QueryWaitTimeout));
        assert!(error.is::<WaitTimeout>());
        assert_eq!(
            resolve_wait(&ids, RUNNING, None, watched(false)).unwrap(),
            [row.clone()]
        );
        let unbounded = Wait {
            timeout: None,
            ..RUNNING
        };
        assert!(resolve_wait(&ids, unbounded, None, watched(true)).is_ok());
    }

    #[proptest]
    fn timeout_message(mut pending: QueryWithFragments, mut running: QueryWithFragments) {
        pending.query.state = QueryState::Pending;
        running.query.state = QueryState::Running;
        let one = WaitTimeout {
            rows: vec![pending.clone(), running.clone()],
            until: Until::Running,
            timeout: Duration::from_secs(15),
        };
        assert_eq!(
            one.to_string(),
            format!(
                "query {} did not reach Running within 15s (state Pending)",
                pending.query.id
            )
        );
        let several = WaitTimeout {
            timeout: Duration::from_millis(50),
            until: Until::Terminated,
            ..one
        };
        assert_eq!(
            several.to_string(),
            format!(
                "queries {}, {} did not terminate within 50ms",
                pending.query.id, running.query.id
            )
        );
    }
}
