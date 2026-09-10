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

//! The worker entity, its state machine, and the requests that manage it.

mod create;
mod drop;
pub mod endpoint;
mod get;
pub mod network_link;
mod status;
mod version;

pub use create::CreateWorker;
pub use drop::DropWorker;
pub use get::GetWorker;
pub use status::GetWorkerStatus;
pub use version::{GetWorkerVersion, WorkerVersion};

use crate::query::query_fragment::{self, QueryFragmentState};
use crate::source::physical;
use crate::{query, sink};
use endpoint::NetworkAddr;
use sea_orm::QuerySelect;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter};

/// Observed worker state, written by the controller.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Display, EnumIter, Serialize, Deserialize, DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum WorkerState {
    /// Assigned at insert time and when a removed address is registered again.
    Pending,
    Active,
    Unreachable,
    Removed,
}

/// Target state.
/// The controller moves the observed state toward it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Display, EnumIter, Serialize, Deserialize, DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum DesiredWorkerState {
    Active,
    Removed,
}

/// A registered worker, identified by its network address.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "worker")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub host_addr: NetworkAddr,
    #[sea_orm(unique)]
    pub data_addr: NetworkAddr,
    /// How many operators this worker can host in total, `None` for unbounded.
    pub max_operators: Option<i32>,
    #[sea_orm(column_type = "JsonBinary")]
    pub config: serde_json::Value,
    pub current_state: WorkerState,
    pub desired_state: DesiredWorkerState,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "crate::source::physical::Entity")]
    PhysicalSource,
    #[sea_orm(has_many = "crate::sink::Entity")]
    Sink,
    #[sea_orm(has_many = "crate::query::query_fragment::Entity")]
    QueryFragment,
}

impl Related<physical::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PhysicalSource.def()
    }
}

impl Related<sink::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Sink.def()
    }
}

impl Related<query::query_fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::QueryFragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

/// The observed state to move a worker to.
/// There is no variant for the initial state, since only registration assigns it.
#[derive(Debug, Clone, Copy)]
pub enum WorkerTransition {
    Active,
    Unreachable,
    Removed,
}

impl WorkerTransition {
    pub const fn get_state(self) -> WorkerState {
        match self {
            Self::Active => WorkerState::Active,
            Self::Unreachable => WorkerState::Unreachable,
            Self::Removed => WorkerState::Removed,
        }
    }
}

impl ActiveModel {
    /// Records the worker's observed state, as reported by the controller.
    /// Unlike a fragment's state, no trigger checks the transition, because every state is reachable:
    /// a worker can recover from unreachable, and registering a removed address again resets it to Pending.
    pub fn apply_transition(&mut self, transition: WorkerTransition) {
        self.current_state = sea_orm::Set(transition.get_state());
    }
}

impl Model {
    /// The configured capacity minus the operators of the worker's non-terminal fragments, `None` when unbounded.
    /// It is negative when the fragments need more operators than the capacity.
    pub async fn free_capacity(&self, conn: &impl ConnectionTrait) -> Result<Option<i64>, DbErr> {
        let Some(capacity) = self.max_operators else {
            return Ok(None);
        };
        let used: Option<i64> = query_fragment::Entity::find()
            .select_only()
            .column_as(query_fragment::Column::NumOperators.sum(), "used")
            .filter(query_fragment::Column::HostAddr.eq(self.host_addr.clone()))
            .filter(query_fragment::Column::CurrentState.is_not_in(QueryFragmentState::TERMINAL))
            .into_tuple()
            .one(conn)
            .await?
            .flatten();
        Ok(Some(i64::from(capacity) - used.unwrap_or(0)))
    }
}

impl Entity {
    /// The workers that the controller still has to reconcile:
    /// every worker targeted as `Active`,
    /// and every worker with an observed state that has not yet converged on its target.
    pub async fn get_managed(conn: &impl ConnectionTrait) -> Result<Vec<Model>, DbErr> {
        Entity::find()
            .filter(
                Column::DesiredState
                    .eq(DesiredWorkerState::Active)
                    .or(Expr::col(Column::CurrentState).ne(Expr::col(Column::DesiredState))),
            )
            .all(conn)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::error::{ErrorCode, code_of};
    use crate::query::query_fragment::QueryFragmentError;
    use crate::query::{
        self, CreateQuery, CreateQueryWithRefs, free_capacities, query_fragment,
        query_fragment::QueryFragmentState, setup, walk_all,
    };
    use crate::statement::ParsedStatement;
    use crate::worker::endpoint::NetworkAddr;
    use crate::worker::{
        self, CreateWorker, DesiredWorkerState, DropWorker, GetWorker, GetWorkerStatus,
        WorkerState, WorkerTransition, network_link,
    };
    use sea_orm::{ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
    use std::collections::{HashMap, HashSet};
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get(req: CreateWorker) {
        let db = Database::for_test().await;
        let created = req.execute(&db).await.unwrap();
        assert_eq!(created.host_addr, req.host_addr);
        assert_eq!(created.data_addr, req.data_addr);
        assert_eq!(created.max_operators, req.max_operators);
        assert_eq!(created.current_state, WorkerState::Pending);
        assert_eq!(created.desired_state, DesiredWorkerState::Active);

        let workers = GetWorker::all()
            .with_host_addr(req.host_addr.clone())
            .execute(&db)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0], created);

        let unknown = DropWorker::new(req.data_addr.clone());
        assert_eq!(unknown.execute(&db).await.unwrap(), None);
        let updated = DropWorker::new(req.host_addr)
            .execute(&db)
            .await
            .unwrap()
            .expect("worker should exist");
        assert_eq!(updated.desired_state, DesiredWorkerState::Removed);
        assert_eq!(GetWorker::all().execute(&db).await.unwrap().len(), 1);
    }

    #[proptest(async = "tokio")]
    async fn host_addr_data_addr_must_differ(mut req: CreateWorker) {
        let db = Database::for_test().await;
        req.data_addr = req.host_addr.clone();
        let rejected = code_of(req.execute(&db).await);
        assert_eq!(rejected, Some(ErrorCode::CatalogWriteRejected));
    }

    #[proptest(async = "tokio")]
    async fn data_addr_unique(
        #[strategy(CreateWorker::topology_dag(2))] mut workers: Vec<CreateWorker>,
    ) {
        workers[1].data_addr = workers[0].data_addr.clone();
        let db = Database::for_test().await;
        workers[0].execute(&db).await.unwrap();
        let taken = code_of(workers[1].execute(&db).await);
        assert_eq!(taken, Some(ErrorCode::WorkerAlreadyExists));
    }

    #[proptest(async = "tokio")]
    async fn negative_capacity_rejected(
        mut worker: CreateWorker,
        #[strategy(i32::MIN..0)] capacity: i32,
    ) {
        let db = Database::for_test().await;
        worker.max_operators = Some(capacity);
        let rejected = code_of(worker.execute(&db).await);
        assert_eq!(rejected, Some(ErrorCode::CatalogWriteRejected));
    }

    #[proptest(async = "tokio")]
    async fn self_peer_rejected(mut req: CreateWorker) {
        let db = Database::for_test().await;
        req.peers = vec![req.host_addr.clone()];
        let statement = ParsedStatement::CreateWorker(req);
        assert!(statement.execute_with(&db).await.is_err());
        assert!(GetWorker::all().execute(&db).await.unwrap().is_empty());
    }

    #[proptest(async = "tokio")]
    async fn duplicate_peer_rejected(mut req: CreateWorker) {
        let db = Database::for_test().await;
        req.peers = vec![addr("peer", 1); 2];
        assert!(req.execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn managed_workers(
        #[strategy(CreateWorker::topology_dag(2..=32))] workers: Vec<CreateWorker>,
    ) {
        let db = Database::for_test().await;
        for w in &workers {
            w.execute(&db).await.unwrap();
        }
        let managed = || async { worker::Entity::get_managed(&db).await.unwrap() };
        assert_eq!(managed().await.len(), workers.len());

        let target = workers[0].host_addr.clone();
        let removing = DropWorker::new(target.clone())
            .execute(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(managed().await.contains(&removing));
        let found = GetWorker::all().with_host_addr(target.clone());
        assert_eq!(found.execute(&db).await.unwrap(), [removing]);

        finish_removal(&db, target.clone()).await;
        assert!(managed().await.iter().all(|w| w.host_addr != target));
    }

    #[proptest(async = "tokio")]
    async fn if_not_exists_returns_existing(req: CreateWorker) {
        let db = Database::for_test().await;
        let original = req.execute(&db).await.unwrap();
        let taken = code_of(req.execute(&db).await);
        assert_eq!(taken, Some(ErrorCode::WorkerAlreadyExists));

        let retry = CreateWorker {
            host_addr: req.host_addr.clone(),
            data_addr: NetworkAddr::new("worker-alt-host", 9999).unwrap(),
            max_operators: Some(999),
            peers: vec![],
            config: serde_json::json!({"different": "config"}),
            if_not_exists: true,
        };
        let returned = retry.execute(&db).await.unwrap();
        assert_eq!(returned, original);

        let all = GetWorker::all().execute(&db).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    fn addr(host: &str, port: u16) -> NetworkAddr {
        NetworkAddr::new(host, port).unwrap()
    }

    fn registration(
        peers: Vec<NetworkAddr>,
        max_operators: i32,
        if_not_exists: bool,
    ) -> CreateWorker {
        CreateWorker {
            host_addr: addr("worker", 8080),
            data_addr: addr("worker", 9000 + max_operators as u16),
            max_operators: Some(max_operators),
            peers,
            config: serde_json::json!({ "max_operators": max_operators }),
            if_not_exists,
        }
    }

    async fn finish_removal(db: &Database, host_addr: NetworkAddr) {
        let row = DropWorker::new(host_addr)
            .execute(db)
            .await
            .unwrap()
            .unwrap();
        let mut removed: worker::ActiveModel = row.into();
        removed.apply_transition(WorkerTransition::Removed);
        removed.update(db).await.unwrap();
    }

    async fn peers_of(db: &Database, host_addr: &NetworkAddr) -> HashSet<NetworkAddr> {
        network_link::Entity::find()
            .filter(network_link::Column::SourceHostAddr.eq(host_addr.clone()))
            .all(db)
            .await
            .unwrap()
            .into_iter()
            .map(|link| link.target_host_addr)
            .collect()
    }

    #[tokio::test]
    async fn a_removed_address_can_be_registered_again() {
        for if_not_exists in [false, true] {
            let db = Database::for_test().await;
            let first = registration(vec![addr("old-peer", 1)], 1, false);
            let other = CreateWorker {
                host_addr: addr("other", 8080),
                data_addr: addr("other", 9000),
                max_operators: None,
                peers: vec![first.host_addr.clone()],
                config: serde_json::Value::Null,
                if_not_exists: false,
            };
            first.execute(&db).await.unwrap();
            other.execute(&db).await.unwrap();
            finish_removal(&db, first.host_addr.clone()).await;

            let second = registration(vec![addr("new-peer", 2)], 2, if_not_exists);
            let row = second.execute(&db).await.unwrap();

            assert_eq!(row.current_state, WorkerState::Pending);
            assert_eq!(row.desired_state, DesiredWorkerState::Active);
            assert_eq!(row.data_addr, second.data_addr);
            assert_eq!(row.max_operators, second.max_operators);
            assert_eq!(row.config, second.config);
            assert_eq!(
                peers_of(&db, &second.host_addr).await,
                HashSet::from([addr("new-peer", 2)])
            );
            assert!(
                worker::Entity::get_managed(&db)
                    .await
                    .unwrap()
                    .contains(&row)
            );
            assert_eq!(
                peers_of(&db, &other.host_addr).await,
                HashSet::from([second.host_addr])
            );
        }
    }

    #[tokio::test]
    async fn a_malformed_address_fails_the_read() {
        let db = Database::for_test().await;
        let insert = "INSERT INTO worker (host_addr, data_addr) VALUES ('no-port', 'host:1')";
        db.execute_unprepared(insert).await.unwrap();
        assert!(GetWorker::all().execute(&db).await.is_err());
    }

    #[tokio::test]
    async fn registering_during_a_removal_fails_and_changes_nothing() {
        for if_not_exists in [false, true] {
            let db = Database::for_test().await;
            let first = registration(vec![], 1, false);
            first.execute(&db).await.unwrap();
            let removing = DropWorker::new(first.host_addr.clone())
                .execute(&db)
                .await
                .unwrap()
                .unwrap();

            let again = registration(vec![], 2, if_not_exists);
            let rejected = code_of(again.execute(&db).await);
            assert_eq!(rejected, Some(ErrorCode::WorkerAlreadyExists));
            assert_eq!(GetWorker::all().execute(&db).await.unwrap(), vec![removing]);
        }
    }

    #[proptest(async = "tokio")]
    async fn capacity_lifecycle_per_worker(
        mut req: CreateQueryWithRefs,
        #[strategy(proptest::sample::select(vec![
            QueryFragmentState::Completed,
            QueryFragmentState::Stopped,
            QueryFragmentState::Failed,
        ]))]
        terminal_state: QueryFragmentState,
    ) {
        let configured: HashMap<_, _> = req
            .workers
            .iter()
            .map(|w| (w.host_addr.clone(), w.max_operators.map(i64::from)))
            .collect();

        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        walk_all(&fragments, terminal_state, &db).await;
        assert_eq!(free_capacities(&db).await, configured);

        // The generated query fits the configured capacities, so it fits again once its first run has terminated.
        // The first run still holds the name, so the second run is unnamed.
        let statement = ParsedStatement::CreateQuery(CreateQuery {
            name: None,
            ..req.query
        });
        statement.execute_with(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn drop_cascades_active_fragments_to_failed(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (query, fragments) = setup(&db, &mut req).await;
        let host = fragments[0].host_addr.clone();

        DropWorker::new(host.clone())
            .execute(&db)
            .await
            .unwrap()
            .expect("worker should exist");

        let after = query_fragment::Entity::find()
            .filter(query_fragment::Column::HostAddr.eq(host.clone()))
            .all(&db)
            .await
            .unwrap();
        assert!(!after.is_empty());
        let (worker, hosted) = GetWorkerStatus::new(host.clone())
            .execute(&db)
            .await
            .unwrap();
        let requested = req.workers.iter().find(|w| w.host_addr == host).unwrap();
        assert_eq!(worker.max_operators, requested.max_operators);
        assert_eq!(hosted.len(), after.len());
        assert!(hosted.iter().all(|f| after.contains(f)));
        let unknown = GetWorkerStatus::new(addr("no-such-worker", 1));
        let rejected = code_of(unknown.execute(&db).await);
        assert_eq!(rejected, Some(ErrorCode::UnknownWorker));
        for f in &after {
            assert_eq!(f.current_state, QueryFragmentState::Failed);
            // The worker-removal trigger must set a stop timestamp and an error,
            // like the application does when it fails a fragment.
            // Without the error, the query's error below is not derived from these fragments.
            assert!(
                f.stop_timestamp.is_some(),
                "cascade-failed fragment missing stop_timestamp"
            );
            assert!(f.error.is_some(), "cascade-failed fragment missing error");
        }

        let query = query::Entity::find_by_id(query.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let err = query.error.expect("query.error must be set when failed");
        assert_eq!(
            err.host_addr, host,
            "query.error must name the removed host"
        );
        assert!(
            matches!(err.error, QueryFragmentError::Transport { .. }),
            "a removed worker is a transport failure, got: {err}"
        );
    }
}
