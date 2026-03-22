use crate::worker::endpoint::{GrpcAddr, HostAddr};
use sea_orm::entity::prelude::*;
use sea_orm::{FromJsonQueryResult, NotSet, Set};
use serde::{Deserialize, Serialize};
use strum::Display;
use thiserror::Error;

pub type FragmentId = i64;

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
    pub id: FragmentId,
    pub query_id: i64,
    pub host_addr: HostAddr,
    pub grpc_addr: GrpcAddr,
    pub plan: serde_json::Value,
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

#[derive(Clone, Debug)]
pub struct CreateFragment {
    pub query_id: i64,
    pub host_addr: HostAddr,
    pub grpc_addr: GrpcAddr,
    pub plan: serde_json::Value,
    pub used_capacity: i32,
    pub has_source: bool,
}

impl From<CreateFragment> for ActiveModel {
    fn from(req: CreateFragment) -> Self {
        Self {
            id: NotSet,
            query_id: Set(req.query_id),
            host_addr: Set(req.host_addr),
            grpc_addr: Set(req.grpc_addr),
            plan: Set(req.plan),
            used_capacity: Set(req.used_capacity),
            has_source: Set(req.has_source),
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

#[cfg(feature = "testing")]
impl CreateFragment {
    pub fn for_models(
        query: &super::Model,
        workers: &[crate::worker::Model],
    ) -> Vec<CreateFragment> {
        use crate::worker::WorkerState;

        workers
            .iter()
            .filter(|w| w.current_state == WorkerState::Active)
            .map(|w| CreateFragment {
                query_id: query.id,
                host_addr: w.host_addr.clone(),
                grpc_addr: w.grpc_addr.clone(),
                plan: serde_json::json!({}),
                used_capacity: 0,
                has_source: false,
            })
            .collect()
    }
}

#[cfg(feature = "testing")]
#[derive(Debug, Clone)]
struct FragmentConfig {
    worker_idx: usize,
    used_capacity: i32,
    has_source: bool,
}

#[cfg(feature = "testing")]
#[derive(Debug, Clone)]
pub struct ValidFragments {
    pub workers: Vec<crate::worker::CreateWorker>,
    fragment_specs: Vec<FragmentConfig>,
}

#[cfg(feature = "testing")]
impl ValidFragments {
    pub fn create_fragments(&self, query_id: i64) -> Vec<CreateFragment> {
        self.fragment_specs
            .iter()
            .map(|spec| {
                let worker = &self.workers[spec.worker_idx];
                CreateFragment {
                    query_id,
                    host_addr: worker.host_addr.clone(),
                    grpc_addr: worker.grpc_addr.clone(),
                    plan: serde_json::json!({}),
                    used_capacity: spec.used_capacity,
                    has_source: spec.has_source,
                }
            })
            .collect()
    }
}

#[cfg(feature = "testing")]
impl crate::Generate for ValidFragments {
    fn generate() -> proptest::strategy::BoxedStrategy<Self> {
        use proptest::prelude::*;

        const MAX_WORKERS: usize = 5;
        (
            crate::worker::CreateWorker::dag_topology(MAX_WORKERS),
            prop::collection::vec((0..=16i32, any::<bool>()), 1..=20usize),
        )
            .prop_map(|(workers, fragment_params)| {
                let mut remaining_capacity: Vec<i32> =
                    workers.iter().map(|w| w.capacity).collect();
                let num_workers = workers.len();

                let mut fragment_specs = Vec::new();
                for (used_capacity, has_source) in fragment_params {
                    let placed = (0..num_workers).find(|offset| {
                        let idx = (fragment_specs.len() + offset) % num_workers;
                        if used_capacity <= remaining_capacity[idx] {
                            remaining_capacity[idx] -= used_capacity;
                            true
                        } else {
                            false
                        }
                    });

                    if let Some(offset) = placed {
                        let idx = (fragment_specs.len() + offset) % num_workers;
                        fragment_specs.push(FragmentConfig {
                            worker_idx: idx,
                            used_capacity,
                            has_source,
                        });
                    }
                }

                if fragment_specs.is_empty() {
                    fragment_specs.push(FragmentConfig {
                        worker_idx: 0,
                        used_capacity: 0,
                        has_source: false,
                    });
                }

                ValidFragments {
                    workers,
                    fragment_specs,
                }
            })
            .boxed()
    }
}
