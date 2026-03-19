use crate::coordinator::{CoordinatorRequest, CreateQueryRequest, DropQueryRequest};
use catalog::Catalog;
use catalog::Reconcilable;
use model::request::Request;
use model::query;
use model::query::query_state::QueryState;
use model::query::{GetQuery, QueryId};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, instrument};

macro_rules! dispatch {
    ($self:ident, $req:expr, $field:ident . $method:ident) => {{
        debug!("received: {:?}", $req);
        let Request { payload, reply_to } = $req;
        let _ = reply_to.send($self.catalog.$field.$method(payload).await);
    }};
}

macro_rules! dispatch_blocking {
    ($self:ident, $req:expr, $field:ident . $method:ident, |$result:ident, $request:ident| $store:expr) => {{
        debug!("received: {:?}", $req);
        let Request { payload, reply_to } = $req;
        match $self.catalog.$field.$method(payload.clone()).await {
            Ok($result) => {
                if payload.should_block() {
                    let $request = Request { payload, reply_to };
                    $store
                } else {
                    let _ = reply_to.send(Ok($result));
                }
            }
            Err(e) => {
                let _ = reply_to.send(Err(e));
            }
        }
    }};
}

#[derive(Error, Debug)]
#[error("Query '{query_id}' terminated with state {state} before reaching target state")]
struct EarlyTermination {
    query_id: i64,
    state: QueryState,
    model: query::Model,
}

struct PendingQueryDrop {
    query_ids: Vec<QueryId>,
    request: DropQueryRequest,
}

pub(super) struct RequestHandler {
    receiver: flume::Receiver<CoordinatorRequest>,
    catalog: Arc<Catalog>,
    pending_query_creates: HashMap<QueryId, CreateQueryRequest>,
    pending_query_drops: Vec<PendingQueryDrop>,
}

impl RequestHandler {
    pub(super) fn new(
        receiver: flume::Receiver<CoordinatorRequest>,
        catalog: Arc<Catalog>,
    ) -> RequestHandler {
        Self {
            receiver,
            catalog,
            pending_query_creates: HashMap::new(),
            pending_query_drops: Vec::new(),
        }
    }

    #[instrument(skip(self))]
    pub(super) async fn run(mut self) {
        let mut query_state_rx = self.catalog.query.subscribe_state();

        loop {
            tokio::select! {
                recv_result = self.receiver.recv_async() => match recv_result {
                    Ok(req) => self.handle_recv(req).await,
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

    #[instrument(skip(self))]
    async fn resolve_pending_queries(&mut self) {
        if self.pending_query_creates.is_empty() && self.pending_query_drops.is_empty() {
            return;
        }

        let all_ids: Vec<_> = self
            .pending_query_creates
            .keys()
            .copied()
            .chain(
                self.pending_query_drops
                    .iter()
                    .flat_map(|drop| &drop.query_ids)
                    .copied(),
            )
            .collect();

        let Ok(queries) = self
            .catalog
            .query
            .get_query(GetQuery::all().with_ids(all_ids))
            .await
        else {
            return;
        };
        let queries: HashMap<QueryId, query::Model> =
            queries.into_iter().map(|m| (m.id, m)).collect();

        self.find_remove_created(&queries);
        self.find_remove_dropped(&queries);
    }

    fn find_remove_created(&mut self, queries: &HashMap<QueryId, query::Model>) {
        let resolved_create_ids: Vec<QueryId> = queries
            .iter()
            .filter(|(id, model)| {
                self.pending_query_creates
                    .get(id)
                    .is_some_and(|req| model.current_state >= req.payload.block_until)
            })
            .map(|(&id, _)| id)
            .collect();

        for id in resolved_create_ids {
            let req = self.pending_query_creates.remove(&id).unwrap();
            let model = queries[&id].clone();

            if model.current_state.is_terminal() && model.current_state != req.payload.block_until {
                let _ = req.reply_to.send(Err(EarlyTermination {
                    query_id: model.id,
                    state: model.current_state,
                    model,
                }
                .into()));
            } else {
                let _ = req.reply_to.send(Ok(model));
            }
        }
    }

    fn find_remove_dropped(&mut self, queries: &HashMap<QueryId, query::Model>) {
        let resolved_drop_indices: Vec<usize> = self
            .pending_query_drops
            .iter()
            .enumerate()
            .filter(|(_, drop)| {
                drop.query_ids.iter().all(|id| {
                    queries
                        .get(id)
                        .is_some_and(|m| m.current_state.is_terminal())
                })
            })
            .map(|(i, _)| i)
            .collect();

        for i in resolved_drop_indices.into_iter().rev() {
            let drop = self.pending_query_drops.swap_remove(i);
            let drop_models = drop
                .query_ids
                .iter()
                .map(|id| queries[id].clone())
                .collect();
            let _ = drop.request.reply_to.send(Ok(drop_models));
        }
    }

    #[instrument(skip_all)]
    async fn handle_recv(&mut self, req: CoordinatorRequest) {
        match req {
            CoordinatorRequest::CreateLogicalSource(r) => {
                dispatch!(self, r, source.create_logical_source)
            }
            CoordinatorRequest::CreatePhysicalSource(r) => {
                dispatch!(self, r, source.create_physical_source)
            }
            CoordinatorRequest::CreateSink(r) => {
                dispatch!(self, r, sink.create_sink)
            }
            CoordinatorRequest::CreateWorker(r) => {
                dispatch!(self, r, worker.create_worker)
            }
            CoordinatorRequest::DropLogicalSource(r) => {
                dispatch!(self, r, source.drop_logical_source)
            }
            CoordinatorRequest::DropPhysicalSource(r) => {
                dispatch!(self, r, source.drop_physical_source)
            }
            CoordinatorRequest::DropSink(r) => {
                dispatch!(self, r, sink.drop_sink)
            }
            CoordinatorRequest::DropWorker(r) => {
                dispatch!(self, r, worker.drop_worker)
            }
            CoordinatorRequest::GetLogicalSource(r) => {
                dispatch!(self, r, source.get_logical_source)
            }
            CoordinatorRequest::GetPhysicalSource(r) => {
                dispatch!(self, r, source.get_physical_source)
            }
            CoordinatorRequest::GetSink(r) => {
                dispatch!(self, r, sink.get_sink)
            }
            CoordinatorRequest::GetQuery(r) => {
                let Request { payload, reply_to } = r;
                let result = if payload.with_fragments {
                    self.catalog.query.get_query_with_fragments(payload).await
                } else {
                    self.catalog
                        .query
                        .get_query(payload)
                        .await
                        .map(|qs| qs.into_iter().map(|q| (q, vec![])).collect())
                };
                let _ = reply_to.send(result);
            }
            CoordinatorRequest::GetWorker(r) => {
                dispatch!(self, r, worker.get_worker)
            }
            CoordinatorRequest::CreateQuery(r) => {
                dispatch_blocking!(self, r, query.create_query, |result, request| {
                    self.pending_query_creates.insert(result.id, request);
                })
            }
            CoordinatorRequest::DropQuery(r) => {
                dispatch_blocking!(self, r, query.drop_query, |result, request| {
                    self.pending_query_drops.push(PendingQueryDrop {
                        query_ids: result.iter().map(|q| q.id).collect(),
                        request,
                    });
                })
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use catalog::testing::{advance_query_to, test_prop};
    use model::query::query_state::QueryState;
    use model::query::{CreateQuery, DropQuery, GetQuery, StopMode};
    use model::Generate;
    use model::worker::CreateWorker;
    use model::worker::endpoint::NetworkAddr;
    use proptest::prelude::*;

    struct TestHandle {
        sender: flume::Sender<CoordinatorRequest>,
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

        async fn send<P, R>(&self, payload: P) -> tokio::sync::oneshot::Receiver<R>
        where
            P: std::fmt::Debug,
            Request<P, R>: Into<CoordinatorRequest>,
        {
            let (rx, request) = Request::new(payload);
            self.sender
                .send_async(request.into())
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
        let rx = handle.send(req).await;
        let result: anyhow::Result<model::query::Model> = rx.await.unwrap();
        let model = result.unwrap();
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
        let rx = handle.send(req).await;
        tokio::task::yield_now().await;

        let queries = handle
            .catalog
            .query
            .get_query(GetQuery::all())
            .await
            .unwrap();
        let query = queries.into_iter().next().unwrap();

        for &state in &path[1..] {
            handle.advance_to(query.id, state).await;
        }

        let result: anyhow::Result<model::query::Model> = rx.await.unwrap();
        let path_contains_target = path.contains(&block_until);

        if path_contains_target {
            let model = result.expect("expected Ok when path contains block_until");
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

    async fn prop_blocking_drop_resolves_when_all_terminal(
        n_queries: usize,
        stop_mode: StopMode,
    ) {
        let handle = TestHandle::new().await;

        let mut query_ids = Vec::new();
        for i in 0..n_queries {
            let create = CreateQuery::new(format!("SELECT {i}"));
            let rx = handle.send(create).await;
            let result: anyhow::Result<model::query::Model> = rx.await.unwrap();
            query_ids.push(result.unwrap().id);
        }

        for &id in &query_ids {
            handle.advance_to(id, QueryState::Pending).await;
            handle.advance_to(id, QueryState::Registered).await;
            handle.advance_to(id, QueryState::Running).await;
        }

        let drop = DropQuery::all().stop_mode(stop_mode).blocking();
        let drop_rx = handle.send(drop).await;
        tokio::task::yield_now().await;

        for &id in &query_ids {
            handle.advance_to(id, QueryState::Stopped).await;
        }

        let dropped: anyhow::Result<Vec<model::query::Model>> = drop_rx.await.unwrap();
        let dropped = dropped.unwrap();
        assert_eq!(dropped.len(), n_queries);
        for model in &dropped {
            assert!(model.current_state.is_terminal());
            assert!(query_ids.contains(&model.id));
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

        #[test]
        fn blocking_drop_resolves_when_all_terminal(
            n_queries in 2..=100usize,
            stop_mode in any::<StopMode>(),
        ) {
            test_prop(|| async move {
                prop_blocking_drop_resolves_when_all_terminal(n_queries, stop_mode).await;
            });
        }
    }
}
