use crate::database::Database;
use crate::{QueryStateManager, WorkerStateManager};
use anyhow::Result;
use model::IntoCondition;
use model::query::fragment;
use model::query::query_state::DesiredQueryState;
use model::query::{self, CreateQuery, DropQuery, GetQuery};
use model::request::{Statement, StatementResponse};
use model::sink::{self, CreateSink, DropSink, Entity as SinkEntity, GetSink};
use model::source::logical_source::{
    self, CreateLogicalSource, DropLogicalSource, Entity as LogicalSourceEntity, GetLogicalSource,
};
use model::source::physical_source::{
    self, CreatePhysicalSource, DropPhysicalSource, Entity as PhysicalSourceEntity,
    GetPhysicalSource,
};
use model::worker::{
    self, CreateWorker, DropWorker, Entity as WorkerEntity, GetWorker, network_link,
};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter,
};
use std::future::Future;

pub trait Execute {
    type Response;
    fn execute(self, conn: &impl ConnectionTrait) -> impl Future<Output = Result<Self::Response>>;
}

impl Execute for CreateWorker {
    type Response = worker::Model;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<worker::Model> {
        if self.if_not_exists {
            if let Some(existing) = WorkerEntity::find_by_id(self.host_addr.clone()).one(conn).await? {
                return Ok(existing);
            }
        }
        let host_addr = self.host_addr.clone();
        let peers = self.peers.clone();
        let worker = worker::ActiveModel::from(self).insert(conn).await?;
        if !peers.is_empty() {
            network_link::Entity::insert_many(peers.into_iter().map(|peer| {
                network_link::ActiveModel {
                    source_host_addr: Set(host_addr.clone()),
                    target_host_addr: Set(peer),
                }
            }))
            .exec(conn)
            .await?;
        }
        Ok(worker)
    }
}

impl Execute for GetWorker {
    type Response = Vec<worker::Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Vec<worker::Model>> {
        Ok(WorkerEntity::find()
            .filter(self.into_condition())
            .all(conn)
            .await?)
    }
}

impl Execute for DropWorker {
    type Response = Option<worker::Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Option<worker::Model>> {
        let worker = WorkerEntity::find_by_id(self.host_addr).one(conn).await?;
        let Some(worker) = worker else {
            return Ok(None);
        };
        let mut active: worker::ActiveModel = worker.into();
        active.desired_state = Set(model::worker::DesiredWorkerState::Removed);
        Ok(Some(active.update(conn).await?))
    }
}

impl Execute for CreateQuery {
    type Response = (query::Model, Vec<fragment::Model>);
    async fn execute(
        self,
        conn: &impl ConnectionTrait,
    ) -> Result<(query::Model, Vec<fragment::Model>)> {
        anyhow::ensure!(!self.fragments.is_empty(), "a query must have at least one fragment");
        anyhow::ensure!(!self.source_ids.is_empty(), "a query must reference at least one source");
        anyhow::ensure!(!self.sink_ids.is_empty(), "a query must reference at least one sink");
        let fragments = self.fragments.clone();
        let source_ids = self.source_ids.clone();
        let sink_ids = self.sink_ids.clone();
        let query = query::ActiveModel::from(self).insert(conn).await?;
        fragment::Entity::insert_many(fragments.into_iter().map(|f| f.into_active_model(query.id)))
            .exec(conn)
            .await?;
        query::query_source::Entity::insert_many(source_ids.into_iter().map(|sid| {
            query::query_source::ActiveModel {
                query_id: Set(query.id),
                source_id: Set(sid),
            }
        }))
        .exec(conn)
        .await?;
        query::query_sink::Entity::insert_many(sink_ids.into_iter().map(|sid| {
            query::query_sink::ActiveModel {
                query_id: Set(query.id),
                sink_id: Set(sid),
            }
        }))
        .exec(conn)
        .await?;
        let fragments = fragment::Entity::find()
            .filter(fragment::Column::QueryId.eq(query.id))
            .all(conn)
            .await?;
        Ok((query, fragments))
    }
}

impl Execute for GetQuery {
    type Response = Vec<(query::Model, Vec<fragment::Model>)>;
    async fn execute(
        self,
        conn: &impl ConnectionTrait,
    ) -> Result<Vec<(query::Model, Vec<fragment::Model>)>> {
        if self.with_fragments {
            let queries = query::Entity::find()
                .filter(self.into_condition())
                .all(conn)
                .await?;
            let mut results = Vec::new();
            for q in queries {
                let fragments = fragment::Entity::find()
                    .filter(fragment::Column::QueryId.eq(q.id))
                    .all(conn)
                    .await?;
                results.push((q, fragments));
            }
            Ok(results)
        } else {
            let queries = query::Entity::find()
                .filter(self.into_condition())
                .all(conn)
                .await?;
            Ok(queries.into_iter().map(|q| (q, vec![])).collect())
        }
    }
}

impl Execute for DropQuery {
    type Response = Vec<query::Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Vec<query::Model>> {
        query::Entity::update_many()
            .col_expr(
                query::Column::DesiredState,
                Expr::value(DesiredQueryState::Stopped),
            )
            .col_expr(query::Column::StopMode, Expr::value(self.stop_mode))
            .filter(self.filters.clone().into_condition())
            .exec(conn)
            .await?;
        Ok(query::Entity::find()
            .filter(self.filters.into_condition())
            .all(conn)
            .await?)
    }
}

impl Execute for CreateLogicalSource {
    type Response = logical_source::Model;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<logical_source::Model> {
        if self.if_not_exists {
            if let Some(existing) = LogicalSourceEntity::find_by_id(&self.name).one(conn).await? {
                return Ok(existing);
            }
        }
        Ok(logical_source::ActiveModel::from(self).insert(conn).await?)
    }
}

impl Execute for GetLogicalSource {
    type Response = Vec<logical_source::Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Vec<logical_source::Model>> {
        Ok(LogicalSourceEntity::find()
            .filter(self.into_condition())
            .all(conn)
            .await?)
    }
}

impl Execute for DropLogicalSource {
    type Response = Vec<logical_source::Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Vec<logical_source::Model>> {
        let condition = self.into_condition();
        let sources = LogicalSourceEntity::find()
            .filter(condition.clone())
            .all(conn)
            .await?;
        LogicalSourceEntity::delete_many()
            .filter(condition)
            .exec(conn)
            .await?;
        Ok(sources)
    }
}

impl Execute for CreatePhysicalSource {
    type Response = physical_source::Model;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<physical_source::Model> {
        if self.if_not_exists {
            let existing = PhysicalSourceEntity::find()
                .filter(physical_source::Column::LogicalSource.eq(&self.logical_source))
                .filter(physical_source::Column::HostAddr.eq(self.host_addr.to_string()))
                .filter(physical_source::Column::SourceType.eq(self.source_type.to_string()))
                .one(conn)
                .await?;
            if let Some(existing) = existing {
                return Ok(existing);
            }
        }
        Ok(physical_source::ActiveModel::from(self)
            .insert(conn)
            .await?)
    }
}

impl Execute for physical_source::CreateInlineSource {
    type Response = physical_source::Model;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<physical_source::Model> {
        Ok(physical_source::ActiveModel::from(self)
            .insert(conn)
            .await?)
    }
}

impl Execute for GetPhysicalSource {
    type Response = Vec<physical_source::Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Vec<physical_source::Model>> {
        Ok(PhysicalSourceEntity::find()
            .filter(self.into_condition())
            .all(conn)
            .await?)
    }
}

impl Execute for DropPhysicalSource {
    type Response = Vec<physical_source::Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Vec<physical_source::Model>> {
        let condition = self.into_condition();
        let sources = PhysicalSourceEntity::find()
            .filter(condition.clone())
            .all(conn)
            .await?;
        PhysicalSourceEntity::delete_many()
            .filter(condition)
            .exec(conn)
            .await?;
        Ok(sources)
    }
}

impl Execute for CreateSink {
    type Response = sink::Model;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<sink::Model> {
        if self.if_not_exists {
            let existing = SinkEntity::find()
                .filter(sink::Column::Name.eq(&self.name))
                .one(conn)
                .await?;
            if let Some(existing) = existing {
                return Ok(existing);
            }
        }
        Ok(sink::ActiveModel::from(self).insert(conn).await?)
    }
}

impl Execute for sink::CreateInlineSink {
    type Response = sink::Model;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<sink::Model> {
        Ok(sink::ActiveModel::from(self).insert(conn).await?)
    }
}

impl Execute for GetSink {
    type Response = Vec<sink::Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Vec<sink::Model>> {
        Ok(SinkEntity::find()
            .filter(self.into_condition())
            .all(conn)
            .await?)
    }
}

impl Execute for DropSink {
    type Response = Vec<sink::Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Vec<sink::Model>> {
        let condition = self.into_condition();
        let sinks = SinkEntity::find()
            .filter(condition.clone())
            .all(conn)
            .await?;
        SinkEntity::delete_many()
            .filter(condition)
            .exec(conn)
            .await?;
        Ok(sinks)
    }
}

pub async fn execute_on(
    statement: Statement,
    conn: &impl ConnectionTrait,
) -> Result<StatementResponse> {
    match statement {
        Statement::CreateWorker(req) => {
            Ok(StatementResponse::CreatedWorker(req.execute(conn).await?))
        }
        Statement::GetWorker(req) => Ok(StatementResponse::Workers(req.execute(conn).await?)),
        Statement::DropWorker(req) => {
            Ok(StatementResponse::DroppedWorker(req.execute(conn).await?))
        }
        Statement::CreateQuery(req) => {
            let (query, _) = req.execute(conn).await?;
            Ok(StatementResponse::CreatedQuery(query))
        }
        Statement::ExplainQuery(explanation) => Ok(StatementResponse::ExplainedQuery(explanation)),
        Statement::GetQuery(req) => Ok(StatementResponse::Queries(req.execute(conn).await?)),
        Statement::DropQuery(req) => {
            Ok(StatementResponse::DroppedQueries(req.execute(conn).await?))
        }
        Statement::CreateLogicalSource(req) => Ok(StatementResponse::CreatedLogicalSource(
            req.execute(conn).await?,
        )),
        Statement::GetLogicalSource(req) => {
            Ok(StatementResponse::LogicalSource(req.execute(conn).await?))
        }
        Statement::DropLogicalSource(req) => Ok(StatementResponse::DroppedLogicalSources(
            req.execute(conn).await?,
        )),
        Statement::CreatePhysicalSource(req) => Ok(StatementResponse::CreatedPhysicalSource(
            req.execute(conn).await?,
        )),
        Statement::GetPhysicalSource(req) => {
            Ok(StatementResponse::PhysicalSources(req.execute(conn).await?))
        }
        Statement::DropPhysicalSource(req) => Ok(StatementResponse::DroppedPhysicalSources(
            req.execute(conn).await?,
        )),
        Statement::CreateSink(req) => Ok(StatementResponse::CreatedSink(req.execute(conn).await?)),
        Statement::GetSink(req) => Ok(StatementResponse::Sinks(req.execute(conn).await?)),
        Statement::DropSink(req) => Ok(StatementResponse::DroppedSinks(req.execute(conn).await?)),
    }
}

pub fn notify_intent(
    response: &StatementResponse,
    query_sm: &QueryStateManager,
    worker_sm: &WorkerStateManager,
) {
    match response {
        StatementResponse::CreatedQuery(_) | StatementResponse::DroppedQueries(_) => {
            query_sm.notify_intent();
        }
        StatementResponse::CreatedWorker(_) | StatementResponse::DroppedWorker(Some(_)) => {
            worker_sm.notify_intent();
        }
        _ => {}
    }
}

pub async fn execute_statement(db: &Database, statement: Statement) -> Result<StatementResponse> {
    let txn = db.begin().await?;
    let response = execute_on(statement, &txn).await?;
    txn.commit().await?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use model::query::CreateQueryWithRefs;
    use model::request::StatementResponse;
    use model::sink::{DropSink, GetSink, SinkWithRefs};
    use model::source::logical_source::{CreateLogicalSource, DropLogicalSource, GetLogicalSource};
    use model::source::physical_source::{
        DropPhysicalSource, GetPhysicalSource, PhysicalSourceWithRefs,
    };
    use model::worker::{
        CreateWorker, DesiredWorkerState, DropWorker, GetWorker, WorkerState, n_unique_workers,
    };
    use test_strategy::proptest;

    async fn exec(db: &Database, stmt: Statement) -> StatementResponse {
        execute_statement(db, stmt).await.unwrap()
    }

    async fn exec_err(db: &Database, stmt: Statement) {
        assert!(execute_statement(db, stmt).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn get_logical_source(create_source: CreateLogicalSource) {
        let db = Database::for_test().await;
        let resp = exec(&db, Statement::CreateLogicalSource(create_source.clone())).await;
        let StatementResponse::CreatedLogicalSource(source) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(source.name, create_source.name);
        assert_eq!(source.schema, create_source.schema);

        let resp = exec(
            &db,
            Statement::GetLogicalSource(
                GetLogicalSource::all().with_name(create_source.name.clone()),
            ),
        )
        .await;
        let StatementResponse::LogicalSource(sources) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(sources.first().unwrap().name, create_source.name);
    }

    #[proptest(async = "tokio")]
    async fn capacity_constraints(mut worker: CreateWorker) {
        let db = Database::for_test().await;

        let mut negative = worker.clone();
        negative.max_operators = Some(-worker.max_operators.unwrap_or(1).max(1));
        exec_err(&db, Statement::CreateWorker(negative)).await;

        worker.max_operators = Some(worker.max_operators.unwrap_or(0).max(0));
        exec(&db, Statement::CreateWorker(worker)).await;
    }

    #[proptest(async = "tokio")]
    async fn get_physical_source(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateLogicalSource(req.logical.clone())).await;
        exec(&db, Statement::CreateWorker(req.worker.clone())).await;
        exec(&db, Statement::CreatePhysicalSource(req.physical.clone())).await;

        let resp = exec(
            &db,
            Statement::GetPhysicalSource(
                GetPhysicalSource::all().with_logical_source(req.logical.name.clone()),
            ),
        )
        .await;
        let StatementResponse::PhysicalSources(sources) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].logical_source, Some(req.logical.name));
        assert_eq!(sources[0].host_addr, Some(req.worker.host_addr));
        assert_eq!(sources[0].source_type, req.physical.source_type);
    }

    #[proptest(async = "tokio")]
    async fn logical_name_unique(create_source: CreateLogicalSource) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateLogicalSource(create_source.clone())).await;
        exec_err(&db, Statement::CreateLogicalSource(create_source)).await;
    }

    #[proptest(async = "tokio")]
    async fn physical_source_refs_exist(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        exec_err(&db, Statement::CreatePhysicalSource(req.physical.clone())).await;

        exec(&db, Statement::CreateLogicalSource(req.logical)).await;
        exec_err(&db, Statement::CreatePhysicalSource(req.physical.clone())).await;

        exec(&db, Statement::CreateWorker(req.worker)).await;
        exec(&db, Statement::CreatePhysicalSource(req.physical)).await;
    }

    #[proptest(async = "tokio")]
    async fn physical_source_drop_with_refs(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateLogicalSource(req.logical.clone())).await;
        exec(&db, Statement::CreateWorker(req.worker)).await;
        exec(&db, Statement::CreatePhysicalSource(req.physical)).await;

        let drop_req = DropLogicalSource {
            with_name: Some(req.logical.name.clone()),
        };
        exec_err(&db, Statement::DropLogicalSource(drop_req)).await;
    }

    #[proptest(async = "tokio")]
    async fn drop_physical_by_id(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateLogicalSource(req.logical)).await;
        exec(&db, Statement::CreateWorker(req.worker)).await;
        let resp = exec(&db, Statement::CreatePhysicalSource(req.physical)).await;
        let StatementResponse::CreatedPhysicalSource(created) = resp else {
            panic!("unexpected response");
        };

        let drop_req =
            DropPhysicalSource::all().with_filters(GetPhysicalSource::all().with_id(created.id));
        let resp = exec(&db, Statement::DropPhysicalSource(drop_req)).await;
        let StatementResponse::DroppedPhysicalSources(dropped) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].id, created.id);

        let resp = exec(
            &db,
            Statement::GetPhysicalSource(GetPhysicalSource::all().with_id(created.id)),
        )
        .await;
        let StatementResponse::PhysicalSources(remaining) = resp else {
            panic!("unexpected response");
        };
        assert!(remaining.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn drop_physical_by_logical_source(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateLogicalSource(req.logical.clone())).await;
        exec(&db, Statement::CreateWorker(req.worker)).await;
        exec(&db, Statement::CreatePhysicalSource(req.physical)).await;

        let drop_req = DropPhysicalSource::all()
            .with_filters(GetPhysicalSource::all().with_logical_source(req.logical.name.clone()));
        let resp = exec(&db, Statement::DropPhysicalSource(drop_req)).await;
        let StatementResponse::DroppedPhysicalSources(dropped) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(dropped.len(), 1);

        let resp = exec(
            &db,
            Statement::GetPhysicalSource(
                GetPhysicalSource::all().with_logical_source(req.logical.name),
            ),
        )
        .await;
        let StatementResponse::PhysicalSources(remaining) = resp else {
            panic!("unexpected response");
        };
        assert!(remaining.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn drop_physical_no_match_noop(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateLogicalSource(req.logical)).await;
        exec(&db, Statement::CreateWorker(req.worker.clone())).await;
        exec(&db, Statement::CreatePhysicalSource(req.physical)).await;

        let drop_req =
            DropPhysicalSource::all().with_filters(GetPhysicalSource::all().with_id(999999));
        let resp = exec(&db, Statement::DropPhysicalSource(drop_req)).await;
        let StatementResponse::DroppedPhysicalSources(dropped) = resp else {
            panic!("unexpected response");
        };
        assert!(dropped.is_empty());

        let resp = exec(
            &db,
            Statement::GetPhysicalSource(
                GetPhysicalSource::all().with_host_addr(req.worker.host_addr),
            ),
        )
        .await;
        let StatementResponse::PhysicalSources(remaining) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(remaining.len(), 1);
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create_physical(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateLogicalSource(req.logical)).await;
        exec(&db, Statement::CreateWorker(req.worker)).await;
        let resp = exec(&db, Statement::CreatePhysicalSource(req.physical.clone())).await;
        let StatementResponse::CreatedPhysicalSource(created) = resp else {
            panic!("unexpected response");
        };

        let drop_req =
            DropPhysicalSource::all().with_filters(GetPhysicalSource::all().with_id(created.id));
        exec(&db, Statement::DropPhysicalSource(drop_req)).await;
        exec(&db, Statement::CreatePhysicalSource(req.physical)).await;
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create_logical(create_source: CreateLogicalSource) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateLogicalSource(create_source.clone())).await;

        let drop_req = DropLogicalSource {
            with_name: Some(create_source.name.clone()),
        };
        exec(&db, Statement::DropLogicalSource(drop_req)).await;
        exec(&db, Statement::CreateLogicalSource(create_source)).await;
    }

    #[proptest(async = "tokio")]
    async fn create_and_get_sink(req: SinkWithRefs) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateWorker(req.worker)).await;

        let resp = exec(&db, Statement::CreateSink(req.sink.clone())).await;
        let StatementResponse::CreatedSink(created) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(created.name, Some(req.sink.name.clone()));
        assert_eq!(created.sink_type, req.sink.sink_type);

        let resp = exec(
            &db,
            Statement::GetSink(GetSink::all().with_name(req.sink.name.clone())),
        )
        .await;
        let StatementResponse::Sinks(sinks) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].name, Some(req.sink.name));
    }

    #[proptest(async = "tokio")]
    async fn drop_sink(req: SinkWithRefs) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateWorker(req.worker)).await;
        exec(&db, Statement::CreateSink(req.sink.clone())).await;

        let resp = exec(
            &db,
            Statement::DropSink(DropSink {
                name: Some(req.sink.name.clone()),
                id: None,
            }),
        )
        .await;
        let StatementResponse::DroppedSinks(dropped) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].name, Some(req.sink.name.clone()));

        let resp = exec(
            &db,
            Statement::GetSink(GetSink::all().with_name(req.sink.name)),
        )
        .await;
        let StatementResponse::Sinks(sinks) = resp else {
            panic!("unexpected response");
        };
        assert!(sinks.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn sink_name_unique(req: SinkWithRefs) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateWorker(req.worker)).await;
        exec(&db, Statement::CreateSink(req.sink.clone())).await;
        exec_err(&db, Statement::CreateSink(req.sink)).await;
    }

    #[proptest(async = "tokio")]
    async fn sink_worker_ref_required(req: SinkWithRefs) {
        let db = Database::for_test().await;
        exec_err(&db, Statement::CreateSink(req.sink.clone())).await;

        exec(&db, Statement::CreateWorker(req.worker)).await;
        exec(&db, Statement::CreateSink(req.sink)).await;
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create_sink(req: SinkWithRefs) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateWorker(req.worker)).await;
        exec(&db, Statement::CreateSink(req.sink.clone())).await;

        exec(
            &db,
            Statement::DropSink(DropSink {
                name: Some(req.sink.name.clone()),
                id: None,
            }),
        )
        .await;
        exec(&db, Statement::CreateSink(req.sink)).await;
    }

    #[proptest(async = "tokio")]
    async fn create_and_get_worker(req: CreateWorker) {
        let db = Database::for_test().await;

        let resp = exec(&db, Statement::CreateWorker(req.clone())).await;
        let StatementResponse::CreatedWorker(created) = resp else {
            panic!("unexpected response");
        };

        assert_eq!(created.host_addr, req.host_addr);
        assert_eq!(created.data_addr, req.data_addr);
        assert_eq!(created.max_operators, req.max_operators);
        assert_eq!(created.current_state, WorkerState::Pending);
        assert_eq!(created.desired_state, DesiredWorkerState::Active);

        let resp = exec(
            &db,
            Statement::GetWorker(GetWorker::all().with_host_addr(req.host_addr.clone())),
        )
        .await;
        let StatementResponse::Workers(workers) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(workers.len(), 1);
        assert_eq!(workers.first().unwrap().host_addr, req.host_addr);
    }

    #[proptest(async = "tokio")]
    async fn drop_and_remove_worker(req: CreateWorker) {
        let db = Database::for_test().await;
        let resp = exec(&db, Statement::CreateWorker(req.clone())).await;
        let StatementResponse::CreatedWorker(created) = resp else {
            panic!("unexpected response");
        };

        let resp = exec(
            &db,
            Statement::DropWorker(DropWorker::new(req.host_addr.clone())),
        )
        .await;
        let StatementResponse::DroppedWorker(Some(updated)) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(updated.desired_state, DesiredWorkerState::Removed);

        let wsm = crate::WorkerStateManager::from(db.clone());
        let removed = wsm
            .mark_worker(created.into(), WorkerState::Removed)
            .await
            .expect("remove should succeed");
        assert_eq!(removed.current_state, WorkerState::Removed);

        let resp = exec(
            &db,
            Statement::GetWorker(GetWorker::all().with_host_addr(req.host_addr)),
        )
        .await;
        let StatementResponse::Workers(workers) = resp else {
            panic!("unexpected response");
        };
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].current_state, WorkerState::Removed);
    }

    #[proptest(async = "tokio")]
    async fn host_addr_data_addr_must_differ(mut req: CreateWorker) {
        let db = Database::for_test().await;
        req.data_addr = req.host_addr.clone();
        exec_err(&db, Statement::CreateWorker(req)).await;
    }

    #[proptest(async = "tokio")]
    async fn data_addr_unique(#[strategy(n_unique_workers(2))] mut workers: Vec<CreateWorker>) {
        let dup_addr = workers[0].data_addr.clone();
        workers[1].data_addr = dup_addr;

        let db = Database::for_test().await;
        exec(&db, Statement::CreateWorker(workers[0].clone())).await;
        exec_err(&db, Statement::CreateWorker(workers[1].clone())).await;
    }

    #[proptest(async = "tokio")]
    async fn worker_drop_blocked_by_active_fragments(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        for w in &req.workers {
            exec(&db, Statement::CreateWorker(w.clone())).await;
        }
        exec(&db, Statement::CreateLogicalSource(req.logical_source.clone())).await;
        let mut source_ids = Vec::new();
        for ps in &req.physical_sources {
            let StatementResponse::CreatedPhysicalSource(m) =
                exec(&db, Statement::CreatePhysicalSource(ps.clone())).await
            else {
                panic!("expected CreatedPhysicalSource")
            };
            source_ids.push(m.id);
        }
        let StatementResponse::CreatedSink(s) =
            exec(&db, Statement::CreateSink(req.sink.clone())).await
        else {
            panic!("expected CreatedSink")
        };
        req.query.source_ids = source_ids;
        req.query.sink_ids = vec![s.id];
        exec(&db, Statement::CreateQuery(req.query.clone())).await;

        let fragment_host = &req.query.fragments[0].host_addr;
        exec_err(
            &db,
            Statement::DropWorker(DropWorker::new(fragment_host.clone())),
        )
        .await;
    }

    #[proptest(async = "tokio")]
    async fn worker_self_peer_rejected(mut req: CreateWorker) {
        let db = Database::for_test().await;
        req.peers = vec![req.host_addr.clone()];
        exec_err(&db, Statement::CreateWorker(req)).await;
    }

    #[proptest(async = "tokio")]
    async fn duplicate_peer_rejected(
        #[strategy(n_unique_workers(2))] mut workers: Vec<CreateWorker>,
    ) {
        let peer_addr = workers[1].host_addr.clone();
        workers[0].peers = vec![peer_addr.clone(), peer_addr];

        let db = Database::for_test().await;
        exec(&db, Statement::CreateWorker(workers[1].clone())).await;
        exec_err(&db, Statement::CreateWorker(workers[0].clone())).await;
    }

    #[proptest(async = "tokio")]
    async fn worker_host_addr_unique(req: CreateWorker) {
        let db = Database::for_test().await;
        exec(&db, Statement::CreateWorker(req.clone())).await;
        exec_err(&db, Statement::CreateWorker(req)).await;
    }
}
