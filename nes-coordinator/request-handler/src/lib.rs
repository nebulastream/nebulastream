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

//! The request handler.
//!
//! This file is the loop that owns the side effects: the channels, the catalog reads, and the reply sends.
//! `execute.rs` runs one statement as one catalog transaction.
//! `parking.rs` holds the rules for which replies wait and when they are released.
//! `client.rs` is the typed side of the request channel that frontends use.

mod client;
mod execute;
mod parking;
mod sql_planner;

pub use client::{Client, EarlyTermination, WaitTimeout};
pub use sql_planner::SqlPlanner;

use chrono::Utc;
use model::Execute;
use model::database::Database;
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryId;
use model::query::GetQuery;
use model::request::{Request, Statement, Wait, Watched};
use model::statement::StatementResult;
use model::worker;
use model::worker::WorkerVersion;
use parking::{Condition, Observation, ParkedReplies};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;
use tracing::{debug, info};

/// Whether the workers run in this process or on remote hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerMode {
    Embedded,
    Remote,
}

/// Queries each worker for its version.
pub trait WorkerVersions: Send + Sync + 'static {
    fn versions(
        &self,
        workers: Vec<worker::Model>,
    ) -> impl Future<Output = Vec<WorkerVersion>> + Send;
}

type ReplyTo = oneshot::Sender<anyhow::Result<StatementResult>>;

/// Runs each request against the catalog and replies to it.
///
/// Only a watch's reply can wait (a watch is a read of queries with a wait).
/// It is sent once the catalog satisfies the wait or its deadline passes.
///
/// The loop wakes on a new request, on a notification that the controller wrote to the catalog,
/// or at the closest deadline.
/// On every wake-up it checks every parked reply again and sends the ones that are ready.
pub struct RequestHandler<V> {
    receiver: async_channel::Receiver<Request>,
    db: Database,
    intent_tx: watch::Sender<()>,
    state_rx: watch::Receiver<()>,
    planner: Option<Arc<dyn SqlPlanner>>,
    mode: WorkerMode,
    versions: V,
    parked: ParkedReplies<ReplyTo>,
}

impl<V: WorkerVersions> RequestHandler<V> {
    pub fn new(
        receiver: async_channel::Receiver<Request>,
        db: Database,
        intent_tx: watch::Sender<()>,
        state_rx: watch::Receiver<()>,
        planner: Option<Arc<dyn SqlPlanner>>,
        mode: WorkerMode,
        versions: V,
    ) -> Self {
        Self {
            receiver,
            db,
            intent_tx,
            state_rx,
            planner,
            mode,
            versions,
            parked: ParkedReplies::default(),
        }
    }

    pub async fn run(mut self) {
        info!("starting");
        loop {
            // With nothing due, the timer still fires once an hour,
            // so a catalog change that sends no notification (a worker leaving the active state, say) is picked up eventually.
            let next_deadline = self
                .parked
                .next_deadline()
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(60 * 60));
            tokio::select! {
                recv_result = self.receiver.recv() => {
                    let Ok(request) = recv_result else {
                        info!("all clients have been dropped, shutting down...");
                        return;
                    };
                    self.handle(request).await;
                },
                Ok(()) = self.state_rx.changed() => {
                    self.try_release().await;
                },
                () = tokio::time::sleep_until(next_deadline) => {
                    self.try_release().await;
                },
            }
        }
    }

    async fn handle(&mut self, request: Request) {
        debug!("received: {request:?}");
        let Request {
            statement,
            reply_to,
        } = request;
        let result = match statement {
            Statement::Watch(read, wait) => return self.watch(read, wait, reply_to).await,
            Statement::Sql(sql) => self.execute_sql(sql).await,
            Statement::Parsed(statement) => self.execute(statement).await,
        };
        // A watch writes nothing, so only a statement gives the controller something new to reconcile.
        if result.is_ok() {
            self.intent_tx
                .send(())
                .expect("intent channel should live as long as the request handler");
        }
        let _ = reply_to.send(result);
    }

    async fn watch(&mut self, read: GetQuery, wait: Wait, reply_to: ReplyTo) {
        let submitted_at = Utc::now();
        let read = GetQuery {
            with_fragments: false,
            ..read
        };
        let ids: Vec<QueryId> = match read.execute(&self.db).await {
            Ok(rows) => rows.into_iter().map(|row| row.query.id).collect(),
            Err(e) => {
                let _ = reply_to.send(Err(e));
                return;
            }
        };
        if ids.is_empty() {
            let nothing = Watched {
                rows: Vec::new(),
                expired: false,
            };
            let _ = reply_to.send(Ok(StatementResult::Watched(nothing)));
            return;
        }
        let deadline = wait.timeout.map(|timeout| Instant::now() + timeout);
        let condition = Condition::new(wait.until, submitted_at);
        self.parked.park(ids, condition, deadline, reply_to);
        // The condition can already hold at submission (a watch on terminated queries, say),
        // so the new entry is checked right away.
        self.try_release().await;
    }

    /// Sends the replies whose wait condition holds or whose deadline has passed.
    /// The others stay parked.
    async fn try_release(&mut self) {
        // The controller notifies after every fragment update, so the catalog is only read while a reply is waiting.
        if self.parked.is_empty() {
            return;
        }
        match self.observe(self.parked.query_ids()).await {
            Ok(observation) => {
                for (reply_to, watched) in self.parked.release(&observation) {
                    debug!("released: {watched:?}");
                    let _ = reply_to.send(Ok(StatementResult::Watched(watched)));
                }
            }
            // A reply whose deadline has passed is not held back by a failed read.
            // It gets the read error, and the client can send the watch again.
            Err(e) => {
                debug!("catalog read failed, only expired replies are released: {e:#}");
                let message = format!("{e:#}");
                for reply_to in self.parked.take_expired(Instant::now()) {
                    let unavailable = CodedError::new(ErrorCode::CatalogUnavailable, &message);
                    let _ = reply_to.send(Err(unavailable.into()));
                }
            }
        }
    }

    async fn observe(&self, ids: Vec<QueryId>) -> anyhow::Result<Observation> {
        let rows = GetQuery::all()
            .with_ids(ids)
            .with_fragments()
            .execute(&self.db)
            .await?;
        // Many fragments share a host, and every listed host is one SQL parameter.
        let hosts: HashSet<_> = rows
            .iter()
            .flat_map(|row| &row.fragments)
            .map(|f| f.host_addr.clone())
            .collect();
        let workers = worker::Entity::find()
            .filter(worker::Column::HostAddr.is_in(hosts))
            .all(&self.db)
            .await?;
        Ok(Observation {
            at: Instant::now(),
            queries: rows.into_iter().map(|row| (row.query.id, row)).collect(),
            worker_states: workers
                .into_iter()
                .map(|w| (w.host_addr, w.current_state))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::query::query_fragment;
    use model::query::query_fragment::QueryFragmentState;
    use model::query::query_state::QueryState;
    use model::query::{CreateQuery, CreateQueryWithRefs, DropQuery, QueryWithFragments};
    use model::request::Until;
    use model::statement::ParsedStatement;
    use model::worker::WorkerState;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    use sea_orm::ConnectionTrait;
    use sea_orm::sea_query::Expr;

    /// Long enough for any wait that should be met, so a wait that is never met fails instead of hanging.
    const GENEROUS: Option<Duration> = Some(Duration::from_secs(10));
    const SHORT: Option<Duration> = Some(Duration::from_millis(50));

    struct NoVersions;

    impl WorkerVersions for NoVersions {
        fn versions(
            &self,
            _workers: Vec<worker::Model>,
        ) -> impl Future<Output = Vec<WorkerVersion>> + Send {
            std::future::ready(Vec::new())
        }
    }

    struct TestHandle {
        rt: tokio::runtime::Runtime,
        client: Client,
        db: Database,
        state_tx: watch::Sender<()>,
        /// The handler fails when nothing receives its notifications.
        _intent_rx: watch::Receiver<()>,
    }

    impl TestHandle {
        fn new(req: &mut CreateQueryWithRefs) -> Self {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let (client, db, state_tx, intent_rx) = rt.block_on(async {
                let db = Database::for_test().await;
                model::query::setup_refs(&db, req).await;
                let (sender, receiver) = async_channel::bounded(16);
                let (intent_tx, intent_rx) = watch::channel(());
                let (state_tx, state_rx) = watch::channel(());
                tokio::spawn(
                    RequestHandler::new(
                        receiver,
                        db.clone(),
                        intent_tx,
                        state_rx,
                        None,
                        WorkerMode::Remote,
                        NoVersions,
                    )
                    .run(),
                );
                (Client::new(sender), db, state_tx, intent_rx)
            });
            Self {
                rt,
                client,
                db,
                state_tx,
                _intent_rx: intent_rx,
            }
        }

        fn create(&self, query: CreateQuery) -> QueryId {
            let created = self
                .rt
                .block_on(
                    self.client
                        .send(Statement::Parsed(ParsedStatement::CreateQuery(query))),
                )
                .unwrap();
            match created {
                StatementResult::CreatedQuery(created) => created.query.id,
                other => panic!("expected CreatedQuery, got {other:?}"),
            }
        }

        fn watch(&self, read: GetQuery, until: Until, timeout: Option<Duration>) -> Watched {
            self.rt
                .block_on(self.client.watch(read, Wait { until, timeout }))
                .unwrap()
        }

        /// Runs `wait` while the fragments of `query` (every query without one) move to `target` and the controller notifies.
        /// The outcome does not depend on which comes first, because a new watch is checked right away.
        fn while_moving<T>(
            &self,
            query: Option<QueryId>,
            target: QueryFragmentState,
            wait: impl Future<Output = T>,
        ) -> T {
            self.rt.block_on(async {
                let moving = async {
                    transition_fragments(&self.db, query, target).await;
                    self.state_tx.send(()).expect("state channel closed");
                };
                tokio::join!(wait, moving).0
            })
        }
    }

    /// One generated fixture, for tests whose outcome does not depend on the generated query.
    fn fixture() -> CreateQueryWithRefs {
        let mut fixture = any::<CreateQueryWithRefs>()
            .new_tree(&mut TestRunner::deterministic())
            .unwrap()
            .current();
        fixture
            .workers
            .iter_mut()
            .for_each(|worker| worker.max_operators = None);
        fixture
    }

    /// Moves fragments through the intermediate states that the catalog validation trigger requires before `target`.
    async fn transition_fragments(
        db: &Database,
        query: Option<QueryId>,
        target: QueryFragmentState,
    ) {
        let steps = match target {
            QueryFragmentState::Started => vec![QueryFragmentState::Started],
            QueryFragmentState::Running => {
                vec![QueryFragmentState::Started, QueryFragmentState::Running]
            }
            QueryFragmentState::Completed => vec![
                QueryFragmentState::Started,
                QueryFragmentState::Running,
                QueryFragmentState::Completed,
            ],
            QueryFragmentState::Stopped => {
                vec![QueryFragmentState::Started, QueryFragmentState::Stopped]
            }
            QueryFragmentState::Failed => vec![QueryFragmentState::Failed],
            QueryFragmentState::Pending => vec![],
        };
        for step in steps {
            let mut update = query_fragment::Entity::update_many()
                .col_expr(query_fragment::Column::CurrentState, Expr::value(step))
                .filter(query_fragment::Column::CurrentState.ne(step));
            if let Some(query) = query {
                update = update.filter(query_fragment::Column::QueryId.eq(query));
            }
            update.exec(db).await.unwrap();
        }
    }

    #[test]
    fn a_watch_is_released_with_the_state_that_ended_it() {
        for (target, expected) in [
            (QueryFragmentState::Completed, QueryState::Completed),
            (QueryFragmentState::Stopped, QueryState::Stopped),
            (QueryFragmentState::Failed, QueryState::Failed),
        ] {
            let mut req = fixture();
            let handle = TestHandle::new(&mut req);
            let id = handle.create(req.query);
            let wait = Wait {
                until: Until::Terminated,
                timeout: GENEROUS,
            };
            let watched = handle
                .while_moving(
                    Some(id),
                    target,
                    handle.client.watch(GetQuery::all().with_id(id), wait),
                )
                .unwrap();
            assert!(!watched.expired);
            assert_eq!(watched.rows[0].query.state, expected);
        }
    }

    #[test]
    fn a_watch_whose_condition_holds_replies_without_a_notification() {
        let mut req = fixture();
        let handle = TestHandle::new(&mut req);
        let id = handle.create(req.query);
        handle.rt.block_on(transition_fragments(
            &handle.db,
            Some(id),
            QueryFragmentState::Stopped,
        ));
        let watched = handle.watch(GetQuery::all().with_id(id), Until::Terminated, GENEROUS);
        assert!(!watched.expired);
    }

    #[test]
    fn a_watch_on_several_queries_waits_for_all_of_them() {
        let mut req = fixture();
        let handle = TestHandle::new(&mut req);
        let first = handle.create(req.query.clone());
        handle.create(CreateQuery {
            name: None,
            ..req.query
        });
        handle.rt.block_on(transition_fragments(
            &handle.db,
            Some(first),
            QueryFragmentState::Stopped,
        ));
        let watched = handle.watch(GetQuery::all(), Until::Terminated, SHORT);
        assert!(watched.expired);
        assert_eq!(watched.rows.len(), 2);
    }

    #[test]
    fn a_watch_expires_at_its_deadline_with_the_rows_as_last_read() {
        for timeout in [Some(Duration::ZERO), SHORT] {
            let mut req = fixture();
            let handle = TestHandle::new(&mut req);
            let id = handle.create(req.query);
            let watched = handle.watch(GetQuery::all().with_id(id), Until::Terminated, timeout);
            assert!(watched.expired);
            assert_eq!(watched.rows[0].query.state, QueryState::Pending);
        }
    }

    #[test]
    fn a_watch_that_matches_nothing_replies_at_once() {
        let mut req = fixture();
        let handle = TestHandle::new(&mut req);
        let watched = handle.watch(
            GetQuery::all().with_id(QueryId::new(i64::MAX)),
            Until::Terminated,
            None,
        );
        assert!(watched.rows.is_empty() && !watched.expired);
    }

    #[test]
    fn a_failed_read_releases_only_the_expired_watches_with_a_catalog_error() {
        let mut req = fixture();
        let handle = TestHandle::new(&mut req);
        let id = handle.create(req.query);
        let rename = |from: &str, to: &str| {
            let sql = format!("ALTER TABLE {from} RENAME TO {to}");
            handle
                .rt
                .block_on(handle.db.execute_unprepared(&sql))
                .unwrap();
        };
        let unbounded = handle.rt.spawn({
            let client = handle.client.clone();
            async move {
                let wait = Wait {
                    until: Until::Terminated,
                    timeout: None,
                };
                client.watch(GetQuery::all().with_id(id), wait).await
            }
        });
        // The handler reads the worker table only when it checks parked replies.
        rename("worker", "worker_gone");
        let err = handle
            .rt
            .block_on(handle.client.watch(
                GetQuery::all().with_id(id),
                Wait {
                    until: Until::Terminated,
                    timeout: SHORT,
                },
            ))
            .unwrap_err();
        let code = err.downcast_ref::<CodedError>().map(|coded| coded.code);
        assert_eq!(code, Some(ErrorCode::CatalogUnavailable));

        rename("worker_gone", "worker");
        let watched = handle
            .while_moving(Some(id), QueryFragmentState::Stopped, unbounded)
            .unwrap()
            .unwrap();
        assert!(!watched.expired);
    }

    #[test]
    fn await_running_fails_when_the_query_ends_first() {
        let mut req = fixture();
        let handle = TestHandle::new(&mut req);
        let id = handle.create(req.query);
        let err = handle
            .while_moving(
                Some(id),
                QueryFragmentState::Failed,
                handle.client.await_running(vec![id], GENEROUS),
            )
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<EarlyTermination>(),
            Some(EarlyTermination { row, expected: QueryState::Running }) if row.query.state == QueryState::Failed
        ));
    }

    #[test]
    fn await_completed_times_out_with_its_code_and_the_rows() {
        let mut req = fixture();
        let handle = TestHandle::new(&mut req);
        let id = handle.create(req.query);
        let err = handle
            .rt
            .block_on(handle.client.await_completed(vec![id], SHORT))
            .unwrap_err();
        let code = err.downcast_ref::<CodedError>().map(|coded| coded.code);
        assert_eq!(code, Some(ErrorCode::QueryWaitTimeout));
        assert!(matches!(
            err.downcast_ref::<WaitTimeout>(),
            Some(WaitTimeout { rows, .. }) if rows[0].query.state == QueryState::Pending
        ));
        assert_eq!(
            format!("{err:#}"),
            format!("the wait timed out: query {id} did not terminate within 50ms (state Pending)")
        );
    }

    #[test]
    fn a_dropped_query_that_stops_counts_as_terminated() {
        let mut req = fixture();
        let handle = TestHandle::new(&mut req);
        let id = handle.create(req.query);
        let dropped = handle
            .rt
            .block_on(
                handle
                    .client
                    .drop_queries(DropQuery::all().with_filters(GetQuery::all().with_id(id))),
            )
            .unwrap();
        assert_eq!(dropped, vec![id]);
        let rows = handle
            .while_moving(
                Some(id),
                QueryFragmentState::Stopped,
                handle.client.await_terminated(dropped, GENEROUS),
            )
            .unwrap();
        assert_eq!(rows[0].query.state, QueryState::Stopped);
    }

    #[test]
    fn awaiting_a_query_that_does_not_exist_fails() {
        let mut req = fixture();
        let handle = TestHandle::new(&mut req);
        let err = handle
            .rt
            .block_on(
                handle
                    .client
                    .await_running(vec![QueryId::new(i64::MAX)], None),
            )
            .unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err:#}");
    }

    proptest! {
        // Generated fixtures spread fragments over several hosts, which the catalog read for freshness has to join.
        #[test]
        fn a_fresh_watch_waits_for_an_observation_after_it_was_sent(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);
            let id = handle.create(req.query);
            handle.rt.block_on(async {
                transition_fragments(&handle.db, Some(id), QueryFragmentState::Running).await;
                worker::Entity::update_many()
                    .col_expr(worker::Column::CurrentState, Expr::value(WorkerState::Active))
                    .exec(&handle.db)
                    .await
                    .unwrap();
            });

            let unobserved = handle.watch(GetQuery::all().with_id(id), Until::Fresh, SHORT);
            prop_assert!(unobserved.expired);
            prop_assert!(!unobserved.rows[0].fragments.is_empty());

            let db = handle.db.clone();
            let state_tx = handle.state_tx.clone();
            handle.rt.spawn(async move {
                loop {
                    query_fragment::Entity::update_many()
                        .col_expr(query_fragment::Column::LastObservedAt, Expr::value(Utc::now()))
                        .exec(&db)
                        .await
                        .unwrap();
                    let _ = state_tx.send(());
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            });
            let observed = handle.watch(GetQuery::all().with_id(id), Until::Fresh, GENEROUS);
            prop_assert!(!observed.expired);
            let fragments: Vec<_> = observed.rows.iter().flat_map(|row: &QueryWithFragments| &row.fragments).collect();
            prop_assert!(!fragments.is_empty());
            prop_assert!(fragments.iter().all(|f| f.last_observed_at.is_some()));
        }
    }
}
