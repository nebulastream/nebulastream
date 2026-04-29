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

mod create;
mod drop;
pub mod endpoint;
mod get;
pub mod network_link;
mod status;

pub use create::CreateWorker;
pub(crate) use create::n_unique_workers;
pub use drop::DropWorker;
pub use get::GetWorker;
pub use status::GetWorkerStatus;

use crate::source::physical;
use crate::{query, sink};
use endpoint::NetworkAddr;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter};

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Display,
    EnumIter,
    Serialize,
    Deserialize,
    DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum WorkerState {
    #[default]
    Pending,
    Active,
    Unreachable,
    Removed,
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Display,
    EnumIter,
    Serialize,
    Deserialize,
    DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum DesiredWorkerState {
    #[default]
    Active,
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "worker")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub host_addr: NetworkAddr,
    #[sea_orm(unique)]
    pub data_addr: NetworkAddr,
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
    #[sea_orm(has_many = "crate::query::fragment::Entity")]
    Fragment,
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

impl Related<query::fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Debug, Clone, Copy)]
pub enum WorkerTransition {
    Pending,
    Active,
    Unreachable,
    Removed,
}

impl WorkerTransition {
    pub fn state(&self) -> WorkerState {
        match self {
            Self::Pending => WorkerState::Pending,
            Self::Active => WorkerState::Active,
            Self::Unreachable => WorkerState::Unreachable,
            Self::Removed => WorkerState::Removed,
        }
    }
}

impl ActiveModel {
    pub fn apply_transition(&mut self, transition: WorkerTransition) {
        self.current_state = sea_orm::Set(transition.state());
    }
}

impl Entity {
    pub async fn actionable(conn: &impl ConnectionTrait) -> Result<Vec<Model>, DbErr> {
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
    use crate::query::fragment::FragmentState;
    use crate::query::{CreateQueryWithRefs, setup, walk_all};
    use crate::worker::endpoint::NetworkAddr;
    use crate::worker::{
        self, Column, CreateWorker, DesiredWorkerState, DropWorker, GetWorker, WorkerState,
        WorkerTransition, n_unique_workers, network_link,
    };
    use sea_orm::prelude::Expr;
    use sea_orm::{ActiveModelTrait, ActiveValue::Set, ColumnTrait, EntityTrait, QueryFilter};
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
    }

    #[proptest(async = "tokio")]
    async fn drop_worker(req: CreateWorker) {
        let db = Database::for_test().await;
        req.execute(&db).await.unwrap();

        let updated = DropWorker::new(req.host_addr)
            .execute(&db)
            .await
            .unwrap()
            .expect("worker should exist");
        assert_eq!(updated.desired_state, DesiredWorkerState::Removed);
        assert_eq!(GetWorker::all().execute(&db).await.unwrap().len(), 1);
    }

    #[proptest(async = "tokio")]
    async fn host_addr_unique(req: CreateWorker) {
        let db = Database::for_test().await;
        req.execute(&db).await.unwrap();
        assert!(req.execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn host_addr_data_addr_must_differ(mut req: CreateWorker) {
        let db = Database::for_test().await;
        req.data_addr = req.host_addr.clone();
        assert!(req.execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn data_addr_unique(#[strategy(n_unique_workers(2))] mut workers: Vec<CreateWorker>) {
        workers[1].data_addr = workers[0].data_addr.clone();
        let db = Database::for_test().await;
        workers[0].execute(&db).await.unwrap();
        assert!(workers[1].execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn capacity_constraints(mut worker: CreateWorker) {
        let db = Database::for_test().await;
        let mut negative = worker.clone();
        negative.max_operators = Some(-worker.max_operators.unwrap_or(1).max(1));
        assert!(negative.execute(&db).await.is_err());

        worker.max_operators = Some(worker.max_operators.unwrap_or(0).max(0));
        worker.execute(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn self_peer_rejected(mut req: CreateWorker) {
        let db = Database::for_test().await;
        req.peers = vec![req.host_addr.clone()];
        assert!(req.execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn duplicate_peer_rejected(
        #[strategy(n_unique_workers(2))] mut workers: Vec<CreateWorker>,
    ) {
        let peer_addr = workers[1].host_addr.clone();
        workers[0].peers = vec![peer_addr.clone(), peer_addr];
        let db = Database::for_test().await;
        workers[1].execute(&db).await.unwrap();
        assert!(workers[0].execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn mismatch_correctness(
        #[strategy(CreateWorker::topology_dag(2, 32))] workers: Vec<CreateWorker>,
    ) {
        let db = Database::for_test().await;
        for w in &workers {
            w.execute(&db).await.unwrap();
        }

        // Mark every other worker as Active (matching desired_state)
        let all = worker::Entity::find().all(&db).await.unwrap();
        for (i, w) in all.into_iter().enumerate() {
            if i % 2 == 0 {
                let mut active: worker::ActiveModel = w.into();
                active.apply_transition(WorkerTransition::Active);
                active.update(&db).await.unwrap();
            }
        }

        let mismatched: HashSet<_> = worker::Entity::find()
            .filter(Expr::col(Column::CurrentState).ne(Expr::col(Column::DesiredState)))
            .all(&db)
            .await
            .unwrap()
            .into_iter()
            .map(|w| w.host_addr)
            .collect();

        let all = worker::Entity::find().all(&db).await.unwrap();
        for w in &all {
            let expect_mismatch = w.current_state != WorkerState::Active;
            assert_eq!(
                mismatched.contains(&w.host_addr),
                expect_mismatch,
                "Worker '{}': current={:?}, expected mismatch={}",
                w.host_addr,
                w.current_state,
                expect_mismatch
            );
        }
    }

    #[proptest(async = "tokio")]
    async fn if_not_exists_returns_existing(req: CreateWorker) {
        let db = Database::for_test().await;
        let original = req.execute(&db).await.unwrap();

        let retry = CreateWorker {
            host_addr: req.host_addr.clone(),
            data_addr: NetworkAddr::new("worker-alt-host", 9999),
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

    #[proptest(async = "tokio")]
    async fn link_self_loop_rejected(req: CreateWorker) {
        let db = Database::for_test().await;
        req.execute(&db).await.unwrap();

        let link = network_link::ActiveModel {
            source_host_addr: Set(req.host_addr.clone()),
            target_host_addr: Set(req.host_addr.clone()),
        };
        assert!(link.insert(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn capacity_lifecycle_per_worker(
        mut req: CreateQueryWithRefs,
        #[strategy(proptest::prop_oneof![
            proptest::strategy::Just(FragmentState::Completed),
            proptest::strategy::Just(FragmentState::Stopped),
            proptest::strategy::Just(FragmentState::Failed),
        ])]
        terminal_state: FragmentState,
    ) {
        let initial: HashMap<_, _> = req
            .workers
            .iter()
            .map(|w| (w.host_addr.clone(), w.max_operators))
            .collect();

        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        walk_all(&fragments, terminal_state, &db).await;

        for w in GetWorker::all().execute(&db).await.unwrap() {
            assert_eq!(w.max_operators, initial[&w.host_addr]);
        }
    }

    #[proptest(async = "tokio")]
    async fn drop_cascades_active_fragments_to_failed(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let host = fragments[0].host_addr.clone();

        DropWorker::new(host.clone())
            .execute(&db)
            .await
            .unwrap()
            .expect("worker should exist");

        let after = crate::query::fragment::Entity::find()
            .filter(crate::query::fragment::Column::HostAddr.eq(host))
            .all(db.connection())
            .await
            .unwrap();
        assert!(!after.is_empty());
        for f in after {
            assert_eq!(f.current_state, FragmentState::Failed);
        }
    }

    #[proptest(async = "tokio")]
    async fn drop_preserves_terminal_fragments(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        walk_all(&fragments, FragmentState::Completed, &db).await;
        let host = fragments[0].host_addr.clone();

        DropWorker::new(host.clone())
            .execute(&db)
            .await
            .unwrap()
            .expect("worker should exist");

        let after = crate::query::fragment::Entity::find()
            .filter(crate::query::fragment::Column::HostAddr.eq(host))
            .all(db.connection())
            .await
            .unwrap();
        assert!(!after.is_empty());
        for f in after {
            assert_eq!(f.current_state, FragmentState::Completed);
        }
    }
}
