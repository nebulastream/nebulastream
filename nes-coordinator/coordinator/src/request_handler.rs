use catalog::Catalog;
use catalog::Reconcilable;
use model::query;
use model::query::query_state::QueryState;
use model::query::{GetQuery, QueryId};
use model::request::{Request, Statement, StatementResponse};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, instrument};

#[derive(Error, Debug)]
#[error("Query '{query_id}' terminated with state {state} before reaching target state")]
struct EarlyTermination {
    query_id: i64,
    state: QueryState,
    model: query::Model,
}

struct PendingCreate {
    block_until: QueryState,
    reply_to: tokio::sync::oneshot::Sender<anyhow::Result<StatementResponse>>,
}

pub(super) struct RequestHandler {
    receiver: flume::Receiver<Request>,
    catalog: Arc<Catalog>,
    pending_query_creates: HashMap<QueryId, PendingCreate>,
}

impl RequestHandler {
    pub(super) fn new(
        receiver: flume::Receiver<Request>,
        catalog: Arc<Catalog>,
    ) -> RequestHandler {
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
        let Request { statement, reply_to } = req;

        // CreateQuery with blocking: defer the reply until the query reaches the target state
        if let Statement::CreateQuery(ref q) = statement {
            if q.blocking() {
                let block_until = q.block_until;
                match self.execute(statement).await {
                    Ok(StatementResponse::CreatedQuery(model)) => {
                        self.pending_query_creates.insert(
                            model.id,
                            PendingCreate { block_until, reply_to },
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
                let _ = pending.reply_to.send(Err(EarlyTermination {
                    query_id: model.id,
                    state: model.current_state,
                    model,
                }
                .into()));
            } else {
                let _ = pending
                    .reply_to
                    .send(Ok(StatementResponse::CreatedQuery(model)));
            }
        }
    }

    async fn execute(&self, statement: Statement) -> anyhow::Result<StatementResponse> {
        match statement {
            Statement::Sql(sql) => {
                // TODO: call C++ SqlPlanner via FFI, convert result to structured variant
                anyhow::bail!("SqlPlanner not yet integrated: {sql}")
            }
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
                let model = self.catalog.query.create_query(q).await?;
                #[cfg(madsim)]
                {
                    use model::query::fragment::CreateFragment;
                    use model::worker::GetWorker;
                    let workers = self.catalog.worker.get_worker(GetWorker::all()).await?;
                    let fragments = CreateFragment::for_models(&model, &workers);
                    self.catalog.query.create_fragments_for_query(fragments).await?;
                }
                Ok(StatementResponse::CreatedQuery(model))
            }
            Statement::ExplainQuery(_sql) => {
                // TODO: call C++ SqlPlanner in explain mode, return plan explanation
                anyhow::bail!("ExplainQuery not yet implemented")
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
    use catalog::testing::advance_query_to;
    use model::query::query_state::QueryState;
    use model::query::{CreateQuery, GetQuery};
    use model::request::{Request, Statement, StatementResponse};
    use model::worker::CreateWorker;
    use model::worker::endpoint::NetworkAddr;
    use model::Generate;
    use proptest::prelude::*;
    use catalog::test_prop;

    struct TestHandle {
        sender: flume::Sender<Request>,
        catalog: Arc<Catalog>,
    }

    impl TestHandle {
        async fn new() -> Self {
            let catalog = Catalog::for_test().await;
            let worker = CreateWorker::new(
                NetworkAddr::new("test-host".to_string(), 9000),
                NetworkAddr::new("test-host".to_string(), 9001),
                100,
            );
            catalog.worker.create_worker(worker).await.unwrap();
            let (sender, receiver) = flume::bounded(16);
            let handler_catalog = catalog.clone();
            tokio::spawn(RequestHandler::new(receiver, handler_catalog).run());
            Self { sender, catalog }
        }

        async fn send(&self, statement: Statement) -> tokio::sync::oneshot::Receiver<anyhow::Result<StatementResponse>> {
            let (rx, request) = Request::new(statement);
            self.sender
                .send_async(request)
                .await
                .expect("handler should be running");
            rx
        }

        async fn advance_to(&self, query_id: i64, target: QueryState) {
            advance_query_to(&self.catalog, query_id, target).await;
        }
    }

    async fn prop_non_blocking_create_returns_pending(req: CreateQuery) {
        let handle = TestHandle::new().await;
        let statement = req.sql_statement.clone();
        let name = req.name.clone();
        let rx = handle.send(Statement::CreateQuery(req)).await;
        let result = rx.await.unwrap().unwrap();
        let model = match result {
            StatementResponse::CreatedQuery(m) => m,
            other => panic!("expected CreatedQuery, got {other:?}"),
        };
        assert_eq!(model.current_state, QueryState::Pending);
        assert_eq!(model.statement, statement);
        assert_eq!(model.name, name);
    }

    async fn prop_blocking_create_resolves_correctly(
        block_until: QueryState,
        path: Vec<QueryState>,
    ) {
        let handle = TestHandle::new().await;
        let req = CreateQuery::new("SELECT 1".to_string()).block_until(block_until);
        let rx = handle.send(Statement::CreateQuery(req)).await;
        tokio::task::yield_now().await;

        let queries = handle
            .catalog
            .query
            .get_query(GetQuery::all())
            .await
            .unwrap();
        let query = queries.into_iter().next().unwrap();

        for &state in &path {
            handle.advance_to(query.id, state).await;
        }

        let result = rx.await.unwrap();
        let path_contains_target = path.contains(&block_until);

        if path_contains_target {
            let resp = result.expect("expected Ok when path contains block_until");
            let model = match resp {
                StatementResponse::CreatedQuery(m) => m,
                other => panic!("expected CreatedQuery, got {other:?}"),
            };
            assert!(model.current_state >= block_until);
        } else {
            let err = result.expect_err("expected EarlyTermination when path skips block_until");
            let early = err
                .downcast_ref::<EarlyTermination>()
                .expect("error should be EarlyTermination");
            assert!(early.state.is_terminal());
            assert!(early.state >= block_until);
            assert_ne!(early.state, block_until);
        }
    }

    proptest! {
        #[test]
        fn non_blocking_create_returns_pending(req in CreateQuery::generate()) {
            let req = CreateQuery { block_until: QueryState::Pending, ..req };
            test_prop(|| async move {
                prop_non_blocking_create_returns_pending(req).await;
            });
        }

        #[test]
        fn blocking_create_resolves_correctly(
            block_until in prop_oneof![
                Just(QueryState::Registered),
                Just(QueryState::Running),
                Just(QueryState::Completed),
            ],
            path in <Vec<QueryState> as Generate>::generate(),
        ) {
            test_prop(|| async move {
                prop_blocking_create_resolves_correctly(block_until, path).await;
            });
        }
    }
}
