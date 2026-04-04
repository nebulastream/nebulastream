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
    use crate::worker::{
        self, CreateWorker, DesiredWorkerState, DropWorker, GetWorker, WorkerState,
        n_unique_workers,
    };
    use sea_orm::prelude::Expr;
    use sea_orm::{ActiveModelTrait, ActiveValue::Set, EntityTrait, QueryFilter};
    use std::collections::HashSet;
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get(req: CreateWorker) {
        let db = Database::for_test().await;
        let created = req.clone().execute(&db.conn).await.unwrap();
        assert_eq!(created.host_addr, req.host_addr);
        assert_eq!(created.data_addr, req.data_addr);
        assert_eq!(created.max_operators, req.max_operators);
        assert_eq!(created.current_state, WorkerState::Pending);
        assert_eq!(created.desired_state, DesiredWorkerState::Active);

        let workers = GetWorker::all()
            .with_host_addr(req.host_addr.clone())
            .execute(&db.conn)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].host_addr, req.host_addr);
    }

    #[proptest(async = "tokio")]
    async fn drop_and_remove(req: CreateWorker) {
        let db = Database::for_test().await;
        req.clone().execute(&db.conn).await.unwrap();

        let updated = DropWorker::new(req.host_addr)
            .execute(&db.conn)
            .await
            .unwrap()
            .expect("worker should exist");
        assert_eq!(updated.desired_state, DesiredWorkerState::Removed);
    }

    #[proptest(async = "tokio")]
    async fn host_addr_unique(req: CreateWorker) {
        let db = Database::for_test().await;
        req.clone().execute(&db.conn).await.unwrap();
        assert!(req.execute(&db.conn).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn host_addr_data_addr_must_differ(mut req: CreateWorker) {
        let db = Database::for_test().await;
        req.data_addr = req.host_addr.clone();
        assert!(req.execute(&db.conn).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn data_addr_unique(#[strategy(n_unique_workers(2))] mut workers: Vec<CreateWorker>) {
        workers[1].data_addr = workers[0].data_addr.clone();
        let db = Database::for_test().await;
        workers[0].clone().execute(&db.conn).await.unwrap();
        assert!(workers[1].clone().execute(&db.conn).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn capacity_constraints(mut worker: CreateWorker) {
        let db = Database::for_test().await;
        let mut negative = worker.clone();
        negative.max_operators = Some(-worker.max_operators.unwrap_or(1).max(1));
        assert!(negative.execute(&db.conn).await.is_err());

        worker.max_operators = Some(worker.max_operators.unwrap_or(0).max(0));
        worker.execute(&db.conn).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn self_peer_rejected(mut req: CreateWorker) {
        let db = Database::for_test().await;
        req.peers = vec![req.host_addr.clone()];
        assert!(req.execute(&db.conn).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn duplicate_peer_rejected(
        #[strategy(n_unique_workers(2))] mut workers: Vec<CreateWorker>,
    ) {
        let peer_addr = workers[1].host_addr.clone();
        workers[0].peers = vec![peer_addr.clone(), peer_addr];
        let db = Database::for_test().await;
        workers[1].clone().execute(&db.conn).await.unwrap();
        assert!(workers[0].clone().execute(&db.conn).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn get_mismatch_correctness(
        #[strategy(CreateWorker::topology_dag(2, 32))] workers: Vec<CreateWorker>,
    ) {
        let db = Database::for_test().await;
        for w in &workers {
            w.clone().execute(&db.conn).await.unwrap();
        }

        // Mark every other worker as Active (matching desired_state)
        let all = worker::Entity::find().all(&db.conn).await.unwrap();
        for (i, w) in all.into_iter().enumerate() {
            if i % 2 == 0 {
                let mut active: worker::ActiveModel = w.into();
                active.current_state = Set(WorkerState::Active);
                active.update(&db.conn).await.unwrap();
            }
        }

        let mismatched: HashSet<_> = worker::Entity::find()
            .filter(Expr::cust("current_state <> desired_state"))
            .all(&db.conn)
            .await
            .unwrap()
            .into_iter()
            .map(|w| w.host_addr)
            .collect();

        let all = worker::Entity::find().all(&db.conn).await.unwrap();
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
}
