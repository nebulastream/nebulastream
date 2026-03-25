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

use crate::worker::endpoint::NetworkAddr;
use crate::worker::CreateWorker;
use proptest::arbitrary::Arbitrary;
use proptest::collection::vec;
use proptest::strategy::BoxedStrategy;
use sea_orm::entity::prelude::*;
use sea_orm::{FromJsonQueryResult, NotSet, Set};
use serde::{Deserialize, Serialize};
use strum::Display;
use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub enum FragmentError {
    #[error("Internal worker error; code: {code}, msg: {msg}, stacktrace: {trace}")]
    WorkerInternal {
        code: u64,
        msg: String,
        trace: String,
    },
    #[error("Worker communication error: {msg}")]
    WorkerCommunication { msg: String },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "fragment")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: i64,
    pub query_id: i64,
    pub host_addr: NetworkAddr,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    pub plan: Vec<u8>,
    pub used_capacity: i32,
    pub has_source: bool,
    pub current_state: FragmentState,
    pub start_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stop_timestamp: Option<chrono::DateTime<chrono::Local>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<FragmentError>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "crate::query::Entity",
        from = "Column::QueryId",
        to = "crate::query::Column::Id",
        on_update = "Restrict",
        on_delete = "Cascade"
    )]
    Query,
    #[sea_orm(
        belongs_to = "crate::worker::Entity",
        from = "Column::HostAddr",
        to = "crate::worker::Column::HostAddr",
        on_update = "Restrict",
        on_delete = "Restrict"
    )]
    Worker,
}

impl Related<crate::query::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Query.def()
    }
}

impl Related<crate::worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl Model {
    pub fn with_state(&self, state: FragmentState) -> ActiveModel {
        let mut am: ActiveModel = self.clone().into();
        am.current_state = Set(state);
        am
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CreateFragment {
    pub host_addr: NetworkAddr,
    pub plan: Vec<u8>,
    pub used_capacity: i32,
    pub has_source: bool,
}

impl CreateFragment {
    pub fn into_active_model(self, query_id: i64) -> ActiveModel {
        ActiveModel {
            id: NotSet,
            query_id: Set(query_id),
            host_addr: Set(self.host_addr),
            plan: Set(self.plan),
            used_capacity: Set(self.used_capacity),
            has_source: Set(self.has_source),
            current_state: NotSet,
            start_timestamp: NotSet,
            stop_timestamp: NotSet,
            error: NotSet,
        }
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum FragmentState {
    #[default]
    Pending,
    Registered,
    Started,
    Running,
    Completed,
    Stopped,
    Failed,
}

impl FragmentState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Stopped | Self::Failed)
    }

    pub fn next(self) -> Option<Self> {
        match self {
            Self::Pending => Some(Self::Registered),
            Self::Registered => Some(Self::Started),
            Self::Started => Some(Self::Running),
            Self::Running => Some(Self::Completed),
            Self::Completed | Self::Stopped | Self::Failed => None,
        }
    }
}

impl TryFrom<i32> for FragmentState {
    type Error = i32;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FragmentState::Registered),
            1 => Ok(FragmentState::Started),
            2 => Ok(FragmentState::Running),
            3 => Ok(FragmentState::Stopped),
            4 => Ok(FragmentState::Failed),
            other => Err(other),
        }
    }
}

#[derive(Debug, Clone)]
struct FragmentCfg {
    worker_idx: usize,
    used_capacity: i32,
    has_source: bool,
}

#[derive(Debug, Clone)]
pub struct FragmentsWithRefs {
    pub workers: Vec<CreateWorker>,
    fragment_specs: Vec<FragmentCfg>,
}

impl FragmentsWithRefs {
    pub fn create_fragments(&self) -> Vec<CreateFragment> {
        self.fragment_specs
            .iter()
            .map(|cfg| {
                let worker = &self.workers[cfg.worker_idx];
                CreateFragment {
                    host_addr: worker.host_addr.clone(),
                    plan: vec![],
                    used_capacity: cfg.used_capacity,
                    has_source: cfg.has_source,
                }
            })
            .collect()
    }
}

impl Arbitrary for FragmentsWithRefs {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;

        const MAX_WORKERS: u8 = 32;
        (
            CreateWorker::topology_dag(2, MAX_WORKERS),
            vec((0..=16i32, any::<bool>()), 1..=20usize),
        )
            .prop_map(|(workers, fragment_params)| {
                let mut remaining_capacity: Vec<i32> = workers
                    .iter()
                    .map(|w| w.capacity.unwrap_or(i32::MAX))
                    .collect();
                let num_workers = workers.len();

                let mut cfgs = Vec::new();
                for (used_capacity, has_source) in fragment_params {
                    let placed = (0..num_workers).find(|offset| {
                        let idx = (cfgs.len() + offset) % num_workers;
                        if used_capacity <= remaining_capacity[idx] {
                            remaining_capacity[idx] -= used_capacity;
                            true
                        } else {
                            false
                        }
                    });

                    if let Some(offset) = placed {
                        let idx = (cfgs.len() + offset) % num_workers;
                        cfgs.push(FragmentCfg {
                            worker_idx: idx,
                            used_capacity,
                            has_source,
                        });
                    }
                }

                if cfgs.is_empty() {
                    cfgs.push(FragmentCfg {
                        worker_idx: 0,
                        used_capacity: 0,
                        has_source: false,
                    });
                }

                FragmentsWithRefs {
                    workers,
                    fragment_specs: cfgs,
                }
            })
            .boxed()
    }
}
