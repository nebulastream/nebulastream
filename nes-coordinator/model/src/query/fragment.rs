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
    pub num_operators: i32,
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
        let mut fragment: ActiveModel = self.clone().into();
        fragment.current_state = Set(state);
        fragment
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CreateFragment {
    pub host_addr: NetworkAddr,
    pub plan: Vec<u8>,
    pub num_operators: i32,
    pub has_source: bool,
}

impl CreateFragment {
    pub fn into_active_model(self, query_id: i64) -> ActiveModel {
        ActiveModel {
            id: NotSet,
            query_id: Set(query_id),
            host_addr: Set(self.host_addr),
            plan: Set(self.plan),
            num_operators: Set(self.num_operators),
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

impl From<i32> for FragmentState {
    fn from(value: i32) -> Self {
        match value {
            0 => FragmentState::Registered,
            1 | 2 => FragmentState::Running,
            3 => FragmentState::Stopped,
            4 => FragmentState::Failed,
            other => panic!("unknown QueryState: {other}"),
        }
    }
}

impl From<FragmentState> for i32 {
    fn from(state: FragmentState) -> Self {
        match state {
            FragmentState::Registered => 0,
            FragmentState::Started => 1,
            FragmentState::Running => 2,
            FragmentState::Stopped | FragmentState::Completed => 3,
            FragmentState::Failed => 4,
            FragmentState::Pending => panic!("Pending has no proto representation"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FragmentsWithRefs {
    pub workers: Vec<CreateWorker>,
    pub fragments: Vec<CreateFragment>,
}

/// Generate fragments placed on the given workers, respecting their capacity.
fn place_fragments(workers: &[CreateWorker], placements: Vec<(usize, bool)>) -> Vec<CreateFragment> {
    let mut remaining: Vec<i32> = workers
        .iter()
        .map(|w| w.max_operators.unwrap_or(i32::MAX))
        .collect();

    placements
        .into_iter()
        .map(|(idx, has_source)| {
            let cap = remaining[idx].min(12);
            let ops = (cap / 2).clamp(0, cap);
            remaining[idx] -= ops;
            CreateFragment {
                host_addr: workers[idx].host_addr.clone(),
                plan: vec![],
                num_operators: ops,
                has_source,
            }
        })
        .collect()
}

impl FragmentsWithRefs {
    /// Strategy that generates fragment sets for `num_queries` queries placed on `workers`,
    /// guaranteeing the total never exceeds any worker's capacity.
    pub fn for_queries(workers: Vec<CreateWorker>, num_queries: usize) -> BoxedStrategy<Vec<Vec<CreateFragment>>> {
        use proptest::prelude::*;
        let n = workers.len().max(1);
        // For each query, generate 1..=n fragments, each assigned to a random worker
        vec(vec((0..n, any::<bool>()), 1..=n), num_queries)
            .prop_map(move |queries| {
                let mut remaining: Vec<i32> = workers
                    .iter()
                    .map(|w| w.max_operators.unwrap_or(i32::MAX))
                    .collect();
                // Budget per fragment: total capacity / total fragments across all queries
                let total_capacity: i64 = remaining.iter().map(|&c| c as i64).sum();
                let total_fragments: i64 = queries.iter().map(|q| q.len() as i64).sum();
                let budget = ((total_capacity / total_fragments.max(1)).min(12).max(1)) as i32;

                queries
                    .into_iter()
                    .map(|placements| {
                        placements
                            .into_iter()
                            .map(|(idx, has_source)| {
                                let ops = budget.min(remaining[idx]).max(0);
                                remaining[idx] -= ops;
                                CreateFragment {
                                    host_addr: workers[idx].host_addr.clone(),
                                    plan: vec![],
                                    num_operators: ops,
                                    has_source,
                                }
                            })
                            .collect()
                    })
                    .collect()
            })
            .boxed()
    }
}

impl Arbitrary for FragmentsWithRefs {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;

        const MAX_WORKERS: u8 = 32;
        (CreateWorker::topology_dag(1, MAX_WORKERS), 1..=10usize)
            .prop_flat_map(|(workers, num)| {
                let n = workers.len();
                (Just(workers), vec((0..n, any::<bool>()), num))
            })
            .prop_map(|(workers, placements)| {
                let fragments = place_fragments(&workers, placements);
                FragmentsWithRefs { workers, fragments }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
