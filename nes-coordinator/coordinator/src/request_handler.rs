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

use crate::SqlPlanner;
use chrono::{DateTime, Utc};
use controller::embedded::WorkerFactory;
use model::Execute;
use model::database::{Database, DatabaseTransaction};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryId;
use model::query;
use model::query::GetQuery;
use model::query::query_state::QueryState;
use model::request::{Payload, Request, StatementInput, Wait};
use model::statement::{Statement, StatementResult};
use model::worker::endpoint::NetworkAddr;
use model::worker::{self, WorkerState};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;
use tracing::{debug, info};

/// Returned when a blocking create finds the query in a terminal state
/// that does not match the one the caller asked to wait for. There is
/// nothing left to wait for, and reporting success would hide that the
/// query never reached the requested state. Carries the query's fragments
/// so a caller can read the structured per-fragment error, which the
/// query's aggregated error summary drops the code of.
#[derive(Error, Debug)]
#[error("Query '{}' terminated early with state {}", .0.id, .0.state)]
pub struct EarlyTermination(pub query::Model, pub Vec<query::query_fragment::Model>);

/// Returned when a blocking wait's timeout elapses before its condition is
/// met. The reply cannot report success, since the query never reached the
/// state the caller asked to wait for.
#[derive(Error, Debug)]
#[error("timed out waiting for the blocking condition to be satisfied")]
struct WaitTimeout;

impl WaitTimeout {
    /// Reported with a code, so a caller can tell a wait that gave up from a query that failed.
    fn coded() -> anyhow::Error {
        anyhow::Error::new(CodedError::new(ErrorCode::QueryWaitTimeout, Self))
    }
}

struct PendingCreate {
    block_until: QueryState,
    deadline: Option<Instant>,
    reply_to: oneshot::Sender<anyhow::Result<StatementResult>>,
}

struct PendingDrop {
    query_ids: Vec<QueryId>,
    deadline: Option<Instant>,
    reply_to: oneshot::Sender<anyhow::Result<StatementResult>>,
}

struct PendingWait {
    query_ids: Vec<QueryId>,
    deadline: Option<Instant>,
    reply_to: oneshot::Sender<anyhow::Result<StatementResult>>,
}

struct PendingStatus {
    query: GetQuery,
    submitted_at: DateTime<Utc>,
    deadline: Option<Instant>,
    reply_to: oneshot::Sender<anyhow::Result<StatementResult>>,
}

/// A point-in-time read of query rows and their fragments, keyed by id, that
/// the parked creates, drops, and reads are all resolved against.
type QuerySnapshot = HashMap<QueryId, (query::Model, Vec<query::query_fragment::Model>)>;

/// Owns the request channel and decides when each reply is sent.
///
/// Every statement runs against the catalog right away. The reply,
/// however, can be parked: a create that asks to block until the query
/// reaches a target state, a drop that asks to block until the query
/// terminates, a read that asks to block until every matched query
/// terminates, and a status read that asks to poll for a window all
/// return their reply later, once the controller has moved the catalog
/// far enough for the wait to be satisfied.
///
/// The loop wakes on three signals: a new request, a notification that
/// the controller has written something to the catalog, or the closest
/// poll deadline. On every wake-up it re-checks every parked reply and
/// answers the ones that are now ready.
pub(super) struct RequestHandler {
    receiver: async_channel::Receiver<Request>,
    db: Database,
    intent_tx: watch::Sender<()>,
    state_rx: watch::Receiver<()>,
    planner: Option<Arc<dyn SqlPlanner>>,
    /// Present when the workers run in this process, which is what decides how one is asked for its version.
    factory: Option<Arc<dyn WorkerFactory>>,
    pending_query_creates: HashMap<QueryId, PendingCreate>,
    pending_query_drops: Vec<PendingDrop>,
    pending_query_waits: Vec<PendingWait>,
    pending_polls: Vec<PendingStatus>,
}

impl RequestHandler {
    pub(super) fn new(
        receiver: async_channel::Receiver<Request>,
        db: Database,
        intent_tx: watch::Sender<()>,
        state_rx: watch::Receiver<()>,
        planner: Option<Arc<dyn SqlPlanner>>,
        factory: Option<Arc<dyn WorkerFactory>>,
    ) -> Self {
        Self {
            receiver,
            db,
            intent_tx,
            state_rx,
            planner,
            factory,
            pending_query_creates: HashMap::new(),
            pending_query_drops: Vec::new(),
            pending_query_waits: Vec::new(),
            pending_polls: Vec::new(),
        }
    }

    pub(super) async fn run(mut self) {
        info!("starting");
        loop {
            let next_deadline = self.next_pending_deadline();
            tokio::select! {
                recv_result = self.receiver.recv() => {
                    let Ok(req) = recv_result else {
                        info!("all clients have been dropped, shutting down...");
                        return;
                    };
                    self.handle(req).await;
                },
                Ok(()) = self.state_rx.changed() => {
                    self.resolve_pending_queries().await;
                },
                () = tokio::time::sleep_until(next_deadline) => {
                    self.resolve_pending_queries().await;
                },
            }
        }
    }

    fn next_pending_deadline(&self) -> Instant {
        let creates = self
            .pending_query_creates
            .values()
            .filter_map(|p| p.deadline);
        let drops = self.pending_query_drops.iter().filter_map(|p| p.deadline);
        let waits = self.pending_query_waits.iter().filter_map(|p| p.deadline);
        let polls = self.pending_polls.iter().filter_map(|p| p.deadline);
        creates
            .chain(drops)
            .chain(waits)
            .chain(polls)
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(60 * 60))
    }

    /// Run the statement and return its result together with the read filter
    /// it resolved to, if any. A read planned from SQL resolves to the same
    /// filter as one passed in structured, so a parked poll can re-run it
    /// regardless of how the read arrived.
    async fn execute(
        &self,
        input: StatementInput,
    ) -> anyhow::Result<(StatementResult, Option<GetQuery>)> {
        match input {
            StatementInput::Sql(sql) => {
                let planner = self
                    .planner
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("no SQL planner configured"))?;
                let txn = self
                    .db
                    .begin()
                    .await
                    .map_err(catalog_failure(ErrorCode::CatalogUnavailable))?;
                #[allow(deprecated)]
                let (statement, txn): (Statement, DatabaseTransaction) =
                    tokio::task::spawn_blocking(move || planner.plan(txn, &sql)).await??;
                let filter = read_filter(&statement);
                let rsp = self.execute_statement(statement, &txn).await?;
                txn.commit()
                    .await
                    .map_err(catalog_failure(ErrorCode::CatalogWriteRejected))?;
                Ok((rsp, filter))
            }
            StatementInput::Parsed(statement) => {
                let filter = read_filter(&statement);
                let txn = self
                    .db
                    .begin()
                    .await
                    .map_err(catalog_failure(ErrorCode::CatalogUnavailable))?;
                let rsp = self.execute_statement(statement, &txn).await?;
                txn.commit()
                    .await
                    .map_err(catalog_failure(ErrorCode::CatalogWriteRejected))?;
                Ok((rsp, filter))
            }
        }
    }

    /// Runs one statement against the catalog, except the one the catalog cannot answer.
    /// A worker's version is not written down anywhere, so that request selects the workers here and
    /// then asks them, which is why it does not go through the catalog-only path.
    async fn execute_statement(
        &self,
        statement: Statement,
        txn: &DatabaseTransaction,
    ) -> anyhow::Result<StatementResult> {
        if let Statement::GetWorkerVersion(req) = statement {
            let workers = req.execute(txn).await?;
            return Ok(StatementResult::WorkerVersions(
                controller::worker_versions(workers, self.factory.clone()).await,
            ));
        }
        statement.execute_on(txn).await
    }

    async fn handle(&mut self, req: Request) {
        debug!("received: {req:?}");
        let Request {
            payload: Payload { input, wait },
            reply_to,
        } = req;

        let result = self.execute(input).await;

        if result.is_ok() {
            self.intent_tx.send(()).expect("intent channel closed");
        }

        match result {
            Ok((StatementResult::CreatedQuery(query, fragments), _)) => {
                if let Wait::UntilState { state, timeout } = wait {
                    self.pending_query_creates.insert(
                        query.id,
                        PendingCreate {
                            block_until: state,
                            deadline: timeout.map(|d| Instant::now() + d),
                            reply_to,
                        },
                    );
                } else {
                    let _ = reply_to.send(Ok(StatementResult::CreatedQuery(query, fragments)));
                }
            }
            Ok((StatementResult::DroppedQueries(queries), _)) => {
                if let Wait::UntilTerminated { timeout } = wait {
                    let query_ids: Vec<_> = queries.iter().map(|q| q.id).collect();
                    if query_ids.is_empty() || queries.iter().all(|q| q.state.is_terminal()) {
                        info!("completed: DroppedQueries({} queries)", queries.len());
                        let _ = reply_to.send(Ok(StatementResult::DroppedQueries(queries)));
                    } else {
                        self.pending_query_drops.push(PendingDrop {
                            query_ids,
                            deadline: timeout.map(|d| Instant::now() + d),
                            reply_to,
                        });
                    }
                } else {
                    let _ = reply_to.send(Ok(StatementResult::DroppedQueries(queries)));
                }
            }
            Ok((StatementResult::Queries(queries), read_filter)) => match wait {
                Wait::Poll { timeout } => {
                    if let Some(query) = read_filter {
                        let submitted_at = Utc::now();
                        match self.worker_states_for(&queries).await {
                            Ok(worker_states)
                                if status_satisfied(&queries, &worker_states, submitted_at) =>
                            {
                                info!("completed: Queries({} queries)", queries.len());
                                let _ = reply_to.send(Ok(StatementResult::Queries(queries)));
                            }
                            Ok(_) => {
                                self.pending_polls.push(PendingStatus {
                                    query,
                                    submitted_at,
                                    deadline: timeout.map(|d| Instant::now() + d),
                                    reply_to,
                                });
                            }
                            Err(e) => {
                                let _ = reply_to.send(Err(e));
                            }
                        }
                    } else {
                        let _ = reply_to.send(Ok(StatementResult::Queries(queries)));
                    }
                }
                Wait::UntilTerminated { timeout } => {
                    let query_ids: Vec<_> = queries.iter().map(|(query, _)| query.id).collect();
                    if query_ids.is_empty()
                        || queries.iter().all(|(query, _)| query.state.is_terminal())
                    {
                        info!("completed: awaited {} queries", queries.len());
                        let _ = reply_to.send(Ok(StatementResult::Queries(queries)));
                    } else {
                        self.pending_query_waits.push(PendingWait {
                            query_ids,
                            deadline: timeout.map(|d| Instant::now() + d),
                            reply_to,
                        });
                    }
                }
                _ => {
                    let _ = reply_to.send(Ok(StatementResult::Queries(queries)));
                }
            },
            Ok((resp, _)) => {
                let _ = reply_to.send(Ok(resp));
            }
            Err(e) => {
                let _ = reply_to.send(Err(e));
            }
        }
    }

    async fn resolve_pending_queries(&mut self) {
        if self.pending_query_creates.is_empty()
            && self.pending_query_drops.is_empty()
            && self.pending_query_waits.is_empty()
            && self.pending_polls.is_empty()
        {
            return;
        }

        let now = Instant::now();

        // Creates, drops, and reads all resolve against one catalog read;
        // polls re-run their own filter and so are handled on their own.
        if !self.pending_query_creates.is_empty()
            || !self.pending_query_drops.is_empty()
            || !self.pending_query_waits.is_empty()
        {
            let queries = self.query_snapshot().await;
            self.resolve_creates(&queries, now);
            self.resolve_drops(&queries, now);
            self.resolve_waits(&queries, now);
        }

        self.resolve_polls().await;
    }

    /// Read the current catalog rows for every query a parked create, drop,
    /// or read is waiting on, keyed by id. A read failure yields an empty
    /// snapshot, which leaves the waits parked for the next wake-up.
    async fn query_snapshot(&self) -> QuerySnapshot {
        let mut all_ids: Vec<_> = self.pending_query_creates.keys().copied().collect();
        for pending in &self.pending_query_drops {
            all_ids.extend(&pending.query_ids);
        }
        for pending in &self.pending_query_waits {
            all_ids.extend(&pending.query_ids);
        }

        let result = Statement::GetQuery(GetQuery::all().with_ids(all_ids).with_fragments())
            .execute_with(&self.db)
            .await;
        let Ok(StatementResult::Queries(query_list)) = result else {
            return QuerySnapshot::new();
        };
        query_list
            .into_iter()
            .map(|(query, fragments)| (query.id, (query, fragments)))
            .collect()
    }

    /// Answer parked creates whose query reached the target state, and time
    /// out those whose deadline passed without reaching it. Iterates the
    /// parked creates directly so one whose row is missing can still time out.
    fn resolve_creates(&mut self, queries: &QuerySnapshot, now: Instant) {
        let create_ids: Vec<_> = self.pending_query_creates.keys().copied().collect();
        for id in create_ids {
            let (block_until, deadline) = {
                let pending = &self.pending_query_creates[&id];
                (pending.block_until, pending.deadline)
            };
            let current = queries.get(&id).map(|(query, _)| query.state);
            let resolved = current.is_some_and(|state| wait_resolved(state, block_until));
            let expired = deadline.is_some_and(|deadline| now >= deadline);
            if !resolved && !expired {
                continue;
            }
            let pending = self.pending_query_creates.remove(&id).unwrap();
            if !resolved {
                let _ = pending.reply_to.send(Err(WaitTimeout::coded()));
                continue;
            }
            let (query, fragments) = queries[&id].clone();

            // Only abnormal terminations (Stopped, Failed) count as early.
            // Reaching Completed past a less-advanced wait target is still a
            // successful resolve from the caller's point of view.
            let early_termination = matches!(query.state, QueryState::Stopped | QueryState::Failed)
                && query.state != pending.block_until;
            if early_termination {
                let _ = pending
                    .reply_to
                    .send(Err(EarlyTermination(query, fragments).into()));
            } else {
                info!("completed: CreatedQuery({})", query.id);
                let _ = pending
                    .reply_to
                    .send(Ok(StatementResult::CreatedQuery(query, fragments)));
            }
        }
    }

    /// Answer parked drops once every dropped query has terminated, and time
    /// out those whose deadline passed.
    fn resolve_drops(&mut self, queries: &QuerySnapshot, now: Instant) {
        let pending_drops = std::mem::take(&mut self.pending_query_drops);
        for pending in pending_drops {
            if all_terminal(&pending.query_ids, queries) {
                let dropped: Vec<_> = pending
                    .query_ids
                    .iter()
                    .filter_map(|id| queries.get(id).map(|(query, _)| query.clone()))
                    .collect();
                info!("completed: DroppedQueries({} queries)", dropped.len());
                let _ = pending
                    .reply_to
                    .send(Ok(StatementResult::DroppedQueries(dropped)));
            } else if pending.deadline.is_some_and(|deadline| now >= deadline) {
                let _ = pending.reply_to.send(Err(WaitTimeout::coded()));
            } else {
                self.pending_query_drops.push(pending);
            }
        }
    }

    /// Answer parked reads once every matched query has terminated, and time
    /// out those whose deadline passed.
    fn resolve_waits(&mut self, queries: &QuerySnapshot, now: Instant) {
        let pending_waits = std::mem::take(&mut self.pending_query_waits);
        for pending in pending_waits {
            if all_terminal(&pending.query_ids, queries) {
                let awaited: Vec<_> = pending
                    .query_ids
                    .iter()
                    .filter_map(|id| queries.get(id).cloned())
                    .collect();
                info!("completed: awaited {} queries", awaited.len());
                let _ = pending.reply_to.send(Ok(StatementResult::Queries(awaited)));
            } else if pending.deadline.is_some_and(|deadline| now >= deadline) {
                let _ = pending.reply_to.send(Err(WaitTimeout::coded()));
            } else {
                self.pending_query_waits.push(pending);
            }
        }
    }

    /// Re-run each parked status poll, answering it once its status can no
    /// longer change or its deadline passed.
    async fn resolve_polls(&mut self) {
        let pending_polls = std::mem::take(&mut self.pending_polls);
        for pending in pending_polls {
            let result = Statement::GetQuery(pending.query.clone())
                .execute_with(&self.db)
                .await;
            let now = Instant::now();
            match result {
                Ok(StatementResult::Queries(queries)) => {
                    let worker_states = self.worker_states_for(&queries).await;
                    match worker_states {
                        Ok(worker_states) => {
                            if status_satisfied(&queries, &worker_states, pending.submitted_at)
                                || pending.deadline.is_some_and(|deadline| now >= deadline)
                            {
                                info!("completed: Queries({} queries)", queries.len());
                                let _ =
                                    pending.reply_to.send(Ok(StatementResult::Queries(queries)));
                            } else {
                                self.pending_polls.push(pending);
                            }
                        }
                        Err(e) => {
                            let _ = pending.reply_to.send(Err(e));
                        }
                    }
                }
                Ok(other) => {
                    let _ = pending.reply_to.send(Ok(other));
                }
                Err(e) => {
                    let _ = pending.reply_to.send(Err(e));
                }
            }
        }
    }

    async fn worker_states_for(
        &self,
        queries: &[(query::Model, Vec<query::query_fragment::Model>)],
    ) -> anyhow::Result<HashMap<NetworkAddr, WorkerState>> {
        let hosts: Vec<NetworkAddr> = queries
            .iter()
            .flat_map(|(_, fragments)| fragments.iter().map(|f| f.host_addr.clone()))
            .collect();
        if hosts.is_empty() {
            return Ok(HashMap::new());
        }
        let workers = worker::Entity::find()
            .filter(worker::Column::HostAddr.is_in(hosts))
            .all(&self.db)
            .await?;
        Ok(workers
            .into_iter()
            .map(|w| (w.host_addr, w.current_state))
            .collect())
    }
}

/// Names the code a failure to reach or to write the catalog reports.
/// Only the transaction itself is classified here. What a statement does inside one fails for
/// reasons of its own, which have their own codes.
fn catalog_failure(code: ErrorCode) -> impl Fn(sea_orm::DbErr) -> anyhow::Error {
    move |err| anyhow::Error::new(CodedError::new(code, err))
}

/// The read filter a statement resolves to, if it is a query read. Used to
/// re-run a parked poll on every wake-up.
fn read_filter(statement: &Statement) -> Option<GetQuery> {
    match statement {
        Statement::GetQuery(q) => Some(q.clone()),
        _ => None,
    }
}

/// True once every listed query is present in the snapshot and terminal.
fn all_terminal(query_ids: &[QueryId], queries: &QuerySnapshot) -> bool {
    query_ids.iter().all(|id| {
        queries
            .get(id)
            .is_some_and(|(query, _)| query.state.is_terminal())
    })
}

/// True once the wait can stop parking: `current` has either reached the
/// requested `target` state, or terminated some other way so it cannot make
/// further progress toward it. A terminal state that is not the target still
/// resolves the wait; the caller decides whether that counts as success.
fn wait_resolved(current: QueryState, target: QueryState) -> bool {
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

/// True for a fragment whose status reply the caller can already trust:
/// either it has terminated, its host is not active (so further updates
/// cannot arrive), or it has reached a running state and been observed
/// at least once after the poll was submitted. A fragment that has been
/// observed but is still mid-transition (for example, re-deploying onto a
/// host that just came back) keeps the poll open, so the caller waits for
/// the query to converge instead of returning a snapshot taken mid-flight.
fn status_satisfied(
    queries: &[(query::Model, Vec<query::query_fragment::Model>)],
    worker_states: &HashMap<NetworkAddr, WorkerState>,
    submitted_at: DateTime<Utc>,
) -> bool {
    queries.iter().all(|(_, fragments)| {
        fragments.iter().all(|f| {
            f.current_state.is_terminal()
                || worker_states.get(&f.host_addr) != Some(&WorkerState::Active)
                || (f.current_state == query::query_fragment::QueryFragmentState::Running
                    && f.last_observed_at.is_some_and(|t| t >= submitted_at))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::query::query_fragment;
    use model::query::query_fragment::QueryFragmentState;
    use model::query::query_state::QueryState;
    use model::query::{CreateQueryWithRefs, DropQuery, GetQuery};
    use model::request::Request;
    use model::statement::{Statement, StatementResult};
    use proptest::prelude::*;
    use sea_orm::sea_query::Expr;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    use std::sync::Arc;

    struct TestHandle {
        rt: tokio::runtime::Runtime,
        sender: async_channel::Sender<Request>,
        db: Database,
        intent_rx: watch::Receiver<()>,
        state_tx: Arc<watch::Sender<()>>,
    }

    impl TestHandle {
        fn new(req: &mut CreateQueryWithRefs) -> Self {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let (sender, db, source_ids, sink_id, intent_rx, state_tx) = rt.block_on(async {
                use model::Execute;

                let db = Database::for_test().await;
                for w in &req.workers {
                    w.execute(&db).await.unwrap();
                }
                req.logical_source.execute(&db).await.unwrap();
                let mut source_ids = std::collections::BTreeSet::new();
                for ps in &req.physical_sources {
                    let m = ps.execute(&db).await.unwrap();
                    source_ids.insert(m.id);
                }
                let s = req.sink.execute(&db).await.unwrap();
                let (sender, receiver) = async_channel::bounded(16);
                let (intent_tx, intent_rx) = watch::channel(());
                let (state_tx, state_rx) = watch::channel(());
                let state_tx = Arc::new(state_tx);
                tokio::spawn(
                    RequestHandler::new(receiver, db.clone(), intent_tx, state_rx, None, None)
                        .run(),
                );
                (sender, db, source_ids, s.id, intent_rx, state_tx)
            });
            req.query.source_ids = source_ids;
            req.query.sink_ids = std::collections::BTreeSet::from([sink_id]);
            Self {
                rt,
                sender,
                db,
                intent_rx,
                state_tx,
            }
        }

        fn ask(&self, statement: Statement) -> anyhow::Result<StatementResult> {
            self.ask_with(Payload::parsed(statement))
        }

        fn ask_blocking(
            &self,
            statement: Statement,
            block_until: QueryState,
        ) -> anyhow::Result<StatementResult> {
            self.ask_with(Payload::parsed(statement).wait(Wait::UntilState {
                state: block_until,
                timeout: None,
            }))
        }

        fn ask_with(&self, input: Payload) -> anyhow::Result<StatementResult> {
            let (rx, request) = Request::from(input);
            self.sender
                .send_blocking(request)
                .expect("handler should be running");
            self.rt.block_on(rx)?
        }
    }

    /// Walk a query's fragments through the intermediate states the catalog
    /// validation trigger requires before they can reach `target`.
    async fn transition_fragments(db: &Database, target: QueryFragmentState) {
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
            query_fragment::Entity::update_many()
                .col_expr(query_fragment::Column::CurrentState, Expr::value(step))
                .filter(query_fragment::Column::CurrentState.ne(step))
                .exec(db)
                .await
                .unwrap();
        }
    }

    /// A poll is honored based on the effective statement, so a read resolves
    /// to a filter while a non-read does not. `execute` derives the filter the
    /// same way after planning, so a read polls whether it arrived as SQL or
    /// already structured.
    #[test]
    fn read_filter_matches_reads_only() {
        assert!(read_filter(&Statement::GetQuery(GetQuery::all())).is_some());
        assert!(read_filter(&Statement::DropQuery(DropQuery::all())).is_none());
    }

    proptest! {
        #[test]
        fn non_blocking_create_returns_pending(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);
            let result = handle.ask(Statement::CreateQuery(req.query.clone())).unwrap();
            let query = match result {
                StatementResult::CreatedQuery(q, _) => q,
                other => panic!("expected CreatedQuery, got {other:?}"),
            };
            assert_eq!(query.state, QueryState::Pending);
            assert_eq!(query.sql, req.query.sql);
            assert_eq!(query.name, req.query.name);
        }

        #[test]
        fn blocking_create_resolves_correctly(
            mut req in any::<CreateQueryWithRefs>(),
            block_until in prop_oneof![
                Just(QueryState::Running),
                Just(QueryState::Completed),
            ],
        ) {
            let handle = TestHandle::new(&mut req);

            let mut intent_rx = handle.intent_rx.clone();
            let db = handle.db.clone();
            let state_tx = handle.state_tx.clone();
            handle.rt.spawn(async move {
                intent_rx.changed().await.unwrap();
                transition_fragments(&db, QueryFragmentState::Completed).await;
                state_tx.send(()).expect("state channel closed");
            });

            let result = handle.ask_blocking(Statement::CreateQuery(req.query), block_until).unwrap();
            let query = match result {
                StatementResult::CreatedQuery(q, _) => q,
                other => panic!("expected CreatedQuery, got {other:?}"),
            };
            assert!(wait_resolved(query.state, block_until));
        }

        #[test]
        fn blocking_create_early_termination(
            mut req in any::<CreateQueryWithRefs>(),
            terminal in prop_oneof![
                Just(QueryFragmentState::Stopped),
                Just(QueryFragmentState::Failed),
            ],
        ) {
            let handle = TestHandle::new(&mut req);

            let mut intent_rx = handle.intent_rx.clone();
            let db = handle.db.clone();
            let state_tx = handle.state_tx.clone();
            handle.rt.spawn(async move {
                intent_rx.changed().await.unwrap();
                transition_fragments(&db, terminal).await;
                state_tx.send(()).expect("state channel closed");
            });

            assert!(
                handle.ask_blocking(Statement::CreateQuery(req.query), QueryState::Completed).is_err(),
                "should return EarlyTermination for terminal={terminal:?}"
            );
        }

        #[test]
        fn non_blocking_drop_returns_immediately(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);

            let result = handle.ask(Statement::CreateQuery(req.query)).unwrap();
            let StatementResult::CreatedQuery(created, _) = result else {
                panic!("expected CreatedQuery");
            };

            let drop_req = DropQuery::all()
                .with_filters(GetQuery::all().with_id(created.id));
            let result = handle.ask(Statement::DropQuery(drop_req)).unwrap();
            let StatementResult::DroppedQueries(dropped) = result else {
                panic!("expected DroppedQueries");
            };
            assert_eq!(dropped.len(), 1);
            assert_eq!(dropped[0].id, created.id);
        }

        #[test]
        fn blocking_drop_resolves_when_terminal(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);

            let result = handle.ask(Statement::CreateQuery(req.query)).unwrap();
            let StatementResult::CreatedQuery(created, _) = result else {
                panic!("expected CreatedQuery");
            };

            let mut intent_rx = handle.intent_rx.clone();
            let db = handle.db.clone();
            let state_tx = handle.state_tx.clone();
            let query_id = created.id;
            handle.rt.spawn(async move {
                // Drop emits a second intent after the create; wait for it
                // before flipping fragments to terminal so the handler sees
                // the drop *and* the state change.
                intent_rx.changed().await.unwrap();
                transition_fragments(&db, QueryFragmentState::Stopped).await;
                state_tx.send(()).expect("state channel closed");
            });

            let drop_req = DropQuery::all()
                .with_filters(GetQuery::all().with_id(query_id));
            let result = handle.ask_with(
                Payload::parsed(Statement::DropQuery(drop_req)).until_terminated(None),
            ).unwrap();
            let StatementResult::DroppedQueries(dropped) = result else {
                panic!("expected DroppedQueries");
            };
            assert!(dropped.iter().all(|q| q.state.is_terminal()));
        }
    }
}
