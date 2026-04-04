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
use catalog::crud::{execute_on, execute_statement, notify_intent};
use catalog::database::Database;
use catalog::{QueryStateManager, Reconcilable, WorkerStateManager};
use model::query;
use model::query::query_state::QueryState;
use model::query::{GetQuery, QueryId};
use model::request::{Request, RequestInput, Statement, StatementInput, StatementResponse};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use tracing::{debug, info, instrument};

#[derive(Error, Debug)]
#[error("Query '{}' terminated early with state {}", .0.id, .0.current_state)]
struct EarlyTermination(query::Model);

struct PendingCreate {
    block_until: QueryState,
    reply_to: oneshot::Sender<anyhow::Result<StatementResponse>>,
}

struct PendingDrop {
    query_ids: Vec<QueryId>,
    reply_to: oneshot::Sender<anyhow::Result<StatementResponse>>,
}

pub(super) struct RequestHandler {
    receiver: flume::Receiver<Request>,
    db: Database,
    planner: Option<Arc<dyn SqlPlanner>>,
    query_sm: Arc<QueryStateManager>,
    worker_sm: Arc<WorkerStateManager>,
    pending_query_creates: HashMap<QueryId, PendingCreate>,
    pending_query_drops: Vec<PendingDrop>,
}

impl RequestHandler {
    pub(super) fn new(
        receiver: flume::Receiver<Request>,
        db: Database,
        planner: Option<Arc<dyn SqlPlanner>>,
        query_sm: Arc<QueryStateManager>,
        worker_sm: Arc<WorkerStateManager>,
    ) -> RequestHandler {
        Self {
            receiver,
            db,
            planner,
            query_sm,
            worker_sm,
            pending_query_creates: HashMap::new(),
            pending_query_drops: Vec::new(),
        }
    }

    #[instrument(skip(self))]
    pub(super) async fn run(mut self) {
        let mut query_state_rx = self.query_sm.subscribe_state();

        loop {
            tokio::select! {
                recv_result = self.receiver.recv_async() => match recv_result {
                    Ok(req) => self.handle(req).await,
                    Err(_) => {
                        info!("all clients have been dropped, shutting down...");
                        return;
                    }
                },
                Ok(()) = query_state_rx.changed() => {
                    self.resolve_pending_queries().await;
                },
            }
        }
    }

    async fn execute(
        &self,
        input: StatementInput,
    ) -> anyhow::Result<StatementResponse> {
        match input {
            StatementInput::Sql(sql) => {
                let planner = self
                    .planner
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("no SQL planner configured"))?;
                let txn = self.db.begin().await?;
                let (statement, txn): (Statement, _) =
                    spawn_blocking(move || planner.plan(txn, &sql)).await??;
                let response = execute_on(statement, &txn).await?;
                txn.commit().await?;
                Ok(response)
            }
            StatementInput::Structured(statement) => {
                let txn = self.db.begin().await?;
                let response = execute_on(statement, &txn).await?;
                txn.commit().await?;
                Ok(response)
            }
        }
    }

    #[instrument(skip_all)]
    async fn handle(&mut self, req: Request) {
        debug!("received: {:?}", req);
        let Request { input: RequestInput { input, block_until }, reply_to } = req;

        let should_block_drop = matches!(
            &input,
            StatementInput::Structured(Statement::DropQuery(q)) if q.should_block
        );

        let result = self.execute(input).await;

        if let Ok(ref resp) = result {
            notify_intent(resp, &self.query_sm, &self.worker_sm);
        }

        match result {
            Ok(StatementResponse::CreatedQuery(query))
                if block_until > QueryState::Pending =>
            {
                self.pending_query_creates.insert(
                    query.id,
                    PendingCreate {
                        block_until,
                        reply_to,
                    },
                );
            }
            Ok(StatementResponse::DroppedQueries(queries)) if should_block_drop => {
                let query_ids: Vec<QueryId> = queries.iter().map(|q| q.id).collect();
                if query_ids.is_empty() || queries.iter().all(|q| q.current_state.is_terminal()) {
                    let _ = reply_to.send(Ok(StatementResponse::DroppedQueries(queries)));
                } else {
                    self.pending_query_drops.push(PendingDrop {
                        query_ids,
                        reply_to,
                    });
                }
            }
            Ok(resp) => {
                let _ = reply_to.send(Ok(resp));
            }
            Err(e) => {
                let _ = reply_to.send(Err(e));
            }
        }
    }

    #[instrument(skip(self))]
    async fn resolve_pending_queries(&mut self) {
        if self.pending_query_creates.is_empty() && self.pending_query_drops.is_empty() {
            return;
        }

        let mut all_ids: Vec<QueryId> = self.pending_query_creates.keys().copied().collect();
        for pending in &self.pending_query_drops {
            all_ids.extend(&pending.query_ids);
        }

        let result = execute_statement(
            &self.db,
            Statement::GetQuery(GetQuery::all().with_ids(all_ids)),
        )
        .await;
        let Ok(StatementResponse::Queries(query_list)) = result else {
            return;
        };
        let queries: HashMap<QueryId, query::Model> =
            query_list.into_iter().map(|(m, _)| (m.id, m)).collect();

        let resolved_ids: Vec<QueryId> = queries
            .iter()
            .filter(|(id, query)| {
                self.pending_query_creates
                    .get(id)
                    .is_some_and(|pending| query.current_state >= pending.block_until)
            })
            .map(|(&id, _)| id)
            .collect();

        for id in resolved_ids {
            let pending = self.pending_query_creates.remove(&id).unwrap();
            let query = queries[&id].clone();

            if query.current_state.is_terminal() && query.current_state != pending.block_until {
                let _ = pending.reply_to.send(Err(EarlyTermination(query).into()));
            } else {
                let _ = pending
                    .reply_to
                    .send(Ok(StatementResponse::CreatedQuery(query)));
            }
        }

        let pending_drops = std::mem::take(&mut self.pending_query_drops);
        for pending in pending_drops {
            let all_terminal = pending.query_ids.iter().all(|id| {
                queries
                    .get(id)
                    .is_some_and(|q| q.current_state.is_terminal())
            });
            if all_terminal {
                let dropped: Vec<_> = pending
                    .query_ids
                    .iter()
                    .filter_map(|id| queries.get(id).cloned())
                    .collect();
                let _ = pending
                    .reply_to
                    .send(Ok(StatementResponse::DroppedQueries(dropped)));
            } else {
                self.pending_query_drops.push(pending);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::query::fragment::FragmentState;
    use model::query::query_state::QueryState;
    use model::query::{CreateQueryWithRefs, DropQuery, GetQuery};
    use model::request::{Request, Statement, StatementResponse};
    use proptest::prelude::*;

    struct TestHandle {
        rt: tokio::runtime::Runtime,
        sender: flume::Sender<Request>,
        db: Database,
        query_sm: Arc<QueryStateManager>,
    }

    impl TestHandle {
        fn new(req: &mut CreateQueryWithRefs) -> Self {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let (sender, db, query_sm, _worker_sm, source_ids, sink_id) = rt.block_on(async {
                let db = Database::for_test().await;
                let query_sm = QueryStateManager::from(db.clone());
                let worker_sm = WorkerStateManager::from(db.clone());
                for w in &req.workers {
                    execute_statement(&db, Statement::CreateWorker(w.clone()))
                        .await
                        .unwrap();
                }
                execute_statement(
                    &db,
                    Statement::CreateLogicalSource(req.logical_source.clone()),
                )
                .await
                .unwrap();
                let mut source_ids = Vec::new();
                for ps in &req.physical_sources {
                    let StatementResponse::CreatedPhysicalSource(m) =
                        execute_statement(&db, Statement::CreatePhysicalSource(ps.clone()))
                            .await
                            .unwrap()
                    else {
                        panic!("expected CreatedPhysicalSource")
                    };
                    source_ids.push(m.id);
                }
                let StatementResponse::CreatedSink(s) =
                    execute_statement(&db, Statement::CreateSink(req.sink.clone()))
                        .await
                        .unwrap()
                else {
                    panic!("expected CreatedSink")
                };
                let (sender, receiver) = flume::bounded(16);
                tokio::spawn(
                    RequestHandler::new(
                        receiver,
                        db.clone(),
                        None,
                        query_sm.clone(),
                        worker_sm.clone(),
                    )
                    .run(),
                );
                (sender, db, query_sm, worker_sm, source_ids, s.id)
            });
            req.query.source_ids = source_ids;
            req.query.sink_ids = vec![sink_id];
            Self {
                rt,
                sender,
                db,
                query_sm,
            }
        }

        fn ask(&self, statement: Statement) -> anyhow::Result<StatementResponse> {
            self.ask_with(RequestInput::structured(statement))
        }

        fn ask_blocking(&self, statement: Statement, block_until: QueryState) -> anyhow::Result<StatementResponse> {
            self.ask_with(RequestInput::structured(statement).block_until(block_until))
        }

        fn ask_with(&self, input: RequestInput) -> anyhow::Result<StatementResponse> {
            let (rx, request) = Request::from(input);
            self.sender
                .send(request)
                .expect("handler should be running");
            self.rt.block_on(rx)?
        }
    }

    proptest! {
        #[test]
        fn non_blocking_create_returns_pending(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);
            let result = handle.ask(Statement::CreateQuery(req.query.clone())).unwrap();
            let query = match result {
                StatementResponse::CreatedQuery(q) => q,
                other => panic!("expected CreatedQuery, got {other:?}"),
            };
            assert_eq!(query.current_state, QueryState::Pending);
            assert_eq!(query.statement, req.query.sql);
            assert_eq!(query.name, req.query.name);
        }

        #[test]
        fn blocking_create_resolves_correctly(
            mut req in any::<CreateQueryWithRefs>(),
            block_until in prop_oneof![
                Just(QueryState::Registered),
                Just(QueryState::Running),
                Just(QueryState::Completed),
            ],
        ) {
            let handle = TestHandle::new(&mut req);

            let mut intent_rx = handle.query_sm.subscribe_intent();
            let db = handle.db.clone();
            let query_sm = handle.query_sm.clone();
            handle.rt.spawn(async move {
                intent_rx.changed().await.unwrap();
                let StatementResponse::Queries(queries) = execute_statement(&db, Statement::GetQuery(GetQuery::all())).await.unwrap() else { panic!("unexpected") };
                let (query, _) = queries.into_iter().next().unwrap();
                let fragments = query_sm.get_fragments(query.id).await.unwrap();
                query_sm.transition_to(query.id, &fragments, FragmentState::Completed).await;
            });

            let result = handle.ask_blocking(Statement::CreateQuery(req.query), block_until).unwrap();
            let query = match result {
                StatementResponse::CreatedQuery(q) => q,
                other => panic!("expected CreatedQuery, got {other:?}"),
            };
            assert!(query.current_state >= block_until);
        }

        #[test]
        fn blocking_create_early_termination(
            mut req in any::<CreateQueryWithRefs>(),
            terminal in prop_oneof![
                Just(FragmentState::Stopped),
                Just(FragmentState::Failed),
            ],
        ) {
            let handle = TestHandle::new(&mut req);

            let mut intent_rx = handle.query_sm.subscribe_intent();
            let db = handle.db.clone();
            let query_sm = handle.query_sm.clone();
            handle.rt.spawn(async move {
                intent_rx.changed().await.unwrap();
                let StatementResponse::Queries(queries) = execute_statement(&db, Statement::GetQuery(GetQuery::all())).await.unwrap() else { panic!("unexpected") };
                let (query, _) = queries.into_iter().next().unwrap();
                let fragments = query_sm.get_fragments(query.id).await.unwrap();
                query_sm.transition_to(query.id, &fragments, terminal).await;
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
            let StatementResponse::CreatedQuery(created) = result else {
                panic!("expected CreatedQuery");
            };

            let drop_req = DropQuery::all()
                .with_filters(GetQuery::all().with_id(created.id));
            let result = handle.ask(Statement::DropQuery(drop_req)).unwrap();
            let StatementResponse::DroppedQueries(dropped) = result else {
                panic!("expected DroppedQueries");
            };
            assert_eq!(dropped.len(), 1);
            assert_eq!(dropped[0].id, created.id);
        }

        #[test]
        fn blocking_drop_resolves_when_terminal(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);

            let result = handle.ask(Statement::CreateQuery(req.query)).unwrap();
            let StatementResponse::CreatedQuery(created) = result else {
                panic!("expected CreatedQuery");
            };

            let mut intent_rx = handle.query_sm.subscribe_intent();
            let query_sm = handle.query_sm.clone();
            let query_id = created.id;
            handle.rt.spawn(async move {
                intent_rx.changed().await.unwrap();
                let fragments = query_sm.get_fragments(query_id).await.unwrap();
                query_sm.transition_to(query_id, &fragments, FragmentState::Stopped).await;
            });

            let drop_req = DropQuery::all()
                .with_filters(GetQuery::all().with_id(query_id))
                .should_block();
            let result = handle.ask(Statement::DropQuery(drop_req)).unwrap();
            let StatementResponse::DroppedQueries(dropped) = result else {
                panic!("expected DroppedQueries");
            };
            assert!(dropped.iter().all(|q| q.current_state.is_terminal()));
        }
    }
}
