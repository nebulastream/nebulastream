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

use catalog::Catalog;
use catalog::Reconcilable;
use model::query;
use model::query::query_state::QueryState;
use model::query::{GetQuery, QueryId};
use model::request::{Request, Statement, StatementResponse};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{debug, info, instrument};

#[derive(Error, Debug)]
#[error("Query '{}' terminated early with state {}", .0.id, .0.current_state)]
struct EarlyTermination(query::Model);

struct PendingCreate {
    block_until: QueryState,
    reply_to: oneshot::Sender<anyhow::Result<StatementResponse>>,
}

pub(super) struct RequestHandler {
    receiver: flume::Receiver<Request>,
    catalog: Arc<Catalog>,
    pending_query_creates: HashMap<QueryId, PendingCreate>,
}

impl RequestHandler {
    pub(super) fn new(receiver: flume::Receiver<Request>, catalog: Arc<Catalog>) -> RequestHandler {
        Self {
            receiver,
            catalog,
            pending_query_creates: HashMap::new(),
        }
    }

    #[instrument(skip(self))]
    pub(super) async fn run(mut self) {
        let mut query_state_rx = self.catalog.query.subscribe_state();

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

    #[instrument(skip_all)]
    async fn handle(&mut self, req: Request) {
        debug!("received: {:?}", req);
        let Request {
            statement,
            reply_to,
        } = req;

        // CreateQuery with blocking: defer the reply until the query reaches the target state
        if let Statement::CreateQuery(ref req) = statement {
            if req.is_blocking() {
                let block_until = req.block_until;
                match self.execute(statement).await {
                    Ok(StatementResponse::CreatedQuery(model)) => {
                        self.pending_query_creates.insert(
                            model.id,
                            PendingCreate {
                                block_until,
                                reply_to,
                            },
                        );
                    }
                    other => {
                        let _ = reply_to.send(other);
                    }
                }
                return;
            }
        }

        let result = self.execute(statement).await;
        let _ = reply_to.send(result);
    }

    #[instrument(skip(self))]
    async fn resolve_pending_queries(&mut self) {
        if self.pending_query_creates.is_empty() {
            return;
        }

        let ids: Vec<_> = self.pending_query_creates.keys().copied().collect();

        let Ok(queries) = self
            .catalog
            .query
            .get_query(GetQuery::all().with_ids(ids))
            .await
        else {
            return;
        };
        let queries: HashMap<QueryId, query::Model> =
            queries.into_iter().map(|m| (m.id, m)).collect();

        let resolved_ids: Vec<QueryId> = queries
            .iter()
            .filter(|(id, model)| {
                self.pending_query_creates
                    .get(id)
                    .is_some_and(|pending| model.current_state >= pending.block_until)
            })
            .map(|(&id, _)| id)
            .collect();

        for id in resolved_ids {
            let pending = self.pending_query_creates.remove(&id).unwrap();
            let model = queries[&id].clone();

            if model.current_state.is_terminal() && model.current_state != pending.block_until {
                let _ = pending.reply_to.send(Err(EarlyTermination(model).into()));
            } else {
                let _ = pending
                    .reply_to
                    .send(Ok(StatementResponse::CreatedQuery(model)));
            }
        }
    }

    async fn execute(&self, statement: Statement) -> anyhow::Result<StatementResponse> {
        match statement {
            Statement::CreateWorker(w) => {
                let model = self.catalog.worker.create_worker(w).await?;
                Ok(StatementResponse::CreatedWorker(model))
            }
            Statement::GetWorker(g) => {
                let workers = self.catalog.worker.get_worker(g).await?;
                Ok(StatementResponse::Workers(workers))
            }
            Statement::DropWorker(d) => {
                let worker = self.catalog.worker.drop_worker(d).await?;
                Ok(StatementResponse::DroppedWorker(worker))
            }
            Statement::CreateQuery(q) => {
                let (model, _) = self.catalog.query.create_query(q).await?;
                Ok(StatementResponse::CreatedQuery(model))
            }
            Statement::ExplainQuery(explanation) => {
                Ok(StatementResponse::ExplainedQuery(explanation))
            }
            Statement::GetQuery(g) => {
                if g.with_fragments {
                    let results = self.catalog.query.get_query_with_fragments(g).await?;
                    Ok(StatementResponse::Queries(results))
                } else {
                    let queries = self.catalog.query.get_query(g).await?;
                    let results = queries.into_iter().map(|q| (q, vec![])).collect();
                    Ok(StatementResponse::Queries(results))
                }
            }
            Statement::DropQuery(d) => {
                let queries = self.catalog.query.drop_query(d).await?;
                Ok(StatementResponse::DroppedQueries(queries))
            }
            Statement::CreateLogicalSource(c) => {
                let model = self.catalog.source.create_logical_source(c).await?;
                Ok(StatementResponse::CreatedLogicalSource(model))
            }
            Statement::GetLogicalSource(g) => {
                let model = self.catalog.source.get_logical_source(g).await?;
                Ok(StatementResponse::LogicalSource(model))
            }
            Statement::DropLogicalSource(d) => {
                let model = self.catalog.source.drop_logical_source(d).await?;
                Ok(StatementResponse::DroppedLogicalSources(model))
            }
            Statement::CreatePhysicalSource(c) => {
                let model = self.catalog.source.create_physical_source(c).await?;
                Ok(StatementResponse::CreatedPhysicalSource(model))
            }
            Statement::GetPhysicalSource(g) => {
                let sources = self.catalog.source.get_physical_source(g).await?;
                Ok(StatementResponse::PhysicalSources(sources))
            }
            Statement::DropPhysicalSource(d) => {
                let sources = self.catalog.source.drop_physical_source(d).await?;
                Ok(StatementResponse::DroppedPhysicalSources(sources))
            }
            Statement::CreateSink(c) => {
                let model = self.catalog.sink.create_sink(c).await?;
                Ok(StatementResponse::CreatedSink(model))
            }
            Statement::GetSink(g) => {
                let sinks = self.catalog.sink.get_sink(g).await?;
                Ok(StatementResponse::Sinks(sinks))
            }
            Statement::DropSink(d) => {
                let sinks = self.catalog.sink.drop_sink(d).await?;
                Ok(StatementResponse::DroppedSinks(sinks))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::query::fragment::FragmentState;
    use model::query::query_state::QueryState;
    use model::query::{CreateQueryWithRefs, GetQuery};
    use model::request::{Request, Statement, StatementResponse};
    use proptest::prelude::*;

    struct TestHandle {
        rt: tokio::runtime::Runtime,
        sender: flume::Sender<Request>,
        catalog: Arc<Catalog>,
    }

    impl TestHandle {
        fn new(req: &CreateQueryWithRefs) -> Self {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let (sender, catalog) = rt.block_on(async {
                let catalog = Catalog::for_test().await;
                for w in &req.workers {
                    catalog.worker.create_worker(w.clone()).await.unwrap();
                }
                let (sender, receiver) = flume::bounded(16);
                tokio::spawn(RequestHandler::new(receiver, catalog.clone()).run());
                (sender, catalog)
            });
            Self {
                rt,
                sender,
                catalog,
            }
        }

        fn ask(&self, statement: Statement) -> anyhow::Result<StatementResponse> {
            let (rx, request) = Request::new(statement);
            self.sender
                .send(request)
                .expect("handler should be running");
            self.rt.block_on(rx)?
        }
    }

    proptest! {
        #[test]
        fn non_blocking_create_returns_pending(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&req);
            req.query.block_until = QueryState::Pending;
            let result = handle.ask(Statement::CreateQuery(req.query.clone())).unwrap();
            let model = match result {
                StatementResponse::CreatedQuery(m) => m,
                other => panic!("expected CreatedQuery, got {other:?}"),
            };
            assert_eq!(model.current_state, QueryState::Pending);
            assert_eq!(model.statement, req.query.sql_statement);
            assert_eq!(model.name, req.query.name);
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
            let handle = TestHandle::new(&req);
            req.query.block_until = block_until;

            let mut intent_rx = handle.catalog.query.subscribe_intent();
            let catalog = handle.catalog.clone();
            handle.rt.spawn(async move {
                intent_rx.changed().await.unwrap();
                let query = catalog.query.get_query(GetQuery::all()).await.unwrap()
                    .into_iter().next().unwrap();
                let fragments = catalog.query.get_fragments(query.id).await.unwrap();
                catalog.query.transition_to(query.id, &fragments, FragmentState::Completed).await;
            });

            let result = handle.ask(Statement::CreateQuery(req.query)).unwrap();
            let model = match result {
                StatementResponse::CreatedQuery(m) => m,
                other => panic!("expected CreatedQuery, got {other:?}"),
            };
            assert!(model.current_state >= block_until);
        }

        #[test]
        fn blocking_create_early_termination(
            mut req in any::<CreateQueryWithRefs>(),
            terminal in prop_oneof![
                Just(FragmentState::Stopped),
                Just(FragmentState::Failed),
            ],
        ) {
            let handle = TestHandle::new(&req);
            req.query.block_until = QueryState::Completed;

            let mut intent_rx = handle.catalog.query.subscribe_intent();
            let catalog = handle.catalog.clone();
            handle.rt.spawn(async move {
                intent_rx.changed().await.unwrap();
                let query = catalog.query.get_query(GetQuery::all()).await.unwrap()
                    .into_iter().next().unwrap();
                let fragments = catalog.query.get_fragments(query.id).await.unwrap();
                catalog.query.transition_to(query.id, &fragments, terminal).await;
            });

            assert!(
                handle.ask(Statement::CreateQuery(req.query)).is_err(),
                "should return EarlyTermination for terminal={terminal:?}"
            );
        }
    }
}
