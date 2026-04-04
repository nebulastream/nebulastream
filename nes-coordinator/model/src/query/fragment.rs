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

use crate::identifier::FragmentId;
use crate::identifier::QueryId;
use crate::sink::{CreateSink, SinkType};
use crate::source::logical::CreateLogicalSource;
use crate::source::physical::{CreatePhysicalSource, SourceType};
use crate::worker::CreateWorker;
use crate::worker::endpoint::NetworkAddr;
use proptest::arbitrary::Arbitrary;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use proptest_derive::Arbitrary as DeriveArbitrary;
use sea_orm::entity::prelude::*;
use sea_orm::{FromJsonQueryResult, NotSet, Set};
use serde::{Deserialize, Serialize};
use strum::Display;
use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub enum FragmentError {
    #[error("Internal worker error; code: {code}, msg: {msg}, stacktrace: {trace}")]
    Internal {
        code: u16,
        msg: String,
        trace: String,
    },
    #[error("Worker communication error: {msg}")]
    Transport { msg: String },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "fragment")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: FragmentId,
    pub query_id: QueryId,
    pub host_addr: NetworkAddr,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    #[serde(skip)]
    pub plan: Vec<u8>,
    pub num_operators: i32,
    pub has_source: bool,
    pub current_state: FragmentState,
    pub desired_state: DesiredFragmentState,
    pub stop_mode: Option<StopMode>,
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
    pub(crate) fn with_state(&self, state: FragmentState) -> ActiveModel {
        let mut fragment: ActiveModel = self.clone().into();
        fragment.current_state = Set(state);
        fragment
    }
}

impl Entity {
    pub async fn actionable(
        conn: &impl ConnectionTrait,
        host: &NetworkAddr,
    ) -> Result<Vec<Model>, DbErr> {
        Entity::find()
            .filter(Column::HostAddr.eq(host.clone()))
            .filter(Expr::col(Column::CurrentState).ne(Expr::col(Column::DesiredState)))
            .filter(Column::CurrentState.is_not_in([
                FragmentState::Completed,
                FragmentState::Stopped,
                FragmentState::Failed,
            ]))
            .all(conn)
            .await
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
    pub(crate) fn into_active_model(self, query_id: QueryId) -> ActiveModel {
        ActiveModel {
            id: NotSet,
            query_id: Set(query_id),
            host_addr: Set(self.host_addr),
            plan: Set(self.plan),
            num_operators: Set(self.num_operators),
            has_source: Set(self.has_source),
            current_state: NotSet,
            desired_state: NotSet,
            stop_mode: NotSet,
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
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
/// Ordered by progression: Pending < Registered < Running < terminal.
pub enum FragmentState {
    #[default]
    Pending,
    Registered,
    Running,
    Completed,
    Stopped,
    Failed,
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
pub enum DesiredFragmentState {
    #[default]
    Completed,
    Stopped,
}

#[derive(
    DeriveArbitrary,
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum StopMode {
    #[default]
    Graceful,
    Forceful,
}

impl From<StopMode> for i32 {
    fn from(value: StopMode) -> Self {
        match value {
            StopMode::Graceful => 0,
            StopMode::Forceful => 1,
        }
    }
}

impl FragmentState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Stopped | Self::Failed)
    }

    pub fn next(self) -> Option<Self> {
        match self {
            Self::Pending => Some(Self::Registered),
            Self::Registered => Some(Self::Running),
            Self::Running => Some(Self::Completed),
            Self::Completed | Self::Stopped | Self::Failed => None,
        }
    }
}

impl From<i32> for FragmentState {
    /// Maps the worker's proto `QueryState` enum to coordinator `FragmentState`.
    /// Proto: Registered=0, Started=1, Running=2, Stopped=3, Failed=4.
    /// Started is an internal worker detail, so both Started(1) and Running(2) map to Running.
    fn from(value: i32) -> Self {
        match value {
            0 => FragmentState::Registered,
            1 | 2 => FragmentState::Running,
            3 => FragmentState::Completed,
            4 => FragmentState::Failed,
            other => panic!("unknown QueryState: {other}"),
        }
    }
}

impl From<FragmentState> for i32 {
    fn from(state: FragmentState) -> Self {
        match state {
            FragmentState::Registered => 0,
            FragmentState::Running => 2,
            FragmentState::Stopped | FragmentState::Completed => 3,
            FragmentState::Failed => 4,
            FragmentState::Pending => panic!("Pending has no proto representation"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FragmentsWithRefs {
    pub(crate) workers: Vec<CreateWorker>,
    pub(crate) fragments: Vec<CreateFragment>,
}

/// Generate fragments placed on the given workers, respecting their capacity.
fn place_fragments(
    workers: &[CreateWorker],
    placements: Vec<(usize, bool)>,
) -> Vec<CreateFragment> {
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
    pub(crate) fn for_queries(
        workers: Vec<CreateWorker>,
        num_queries: usize,
    ) -> BoxedStrategy<Vec<Vec<CreateFragment>>> {
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
                let budget = (total_capacity / total_fragments.max(1)).min(12).max(1) as i32;

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

/// Per-query references: fragments, physical sources on source-fragments, and a sink.
#[derive(Debug, Clone)]
pub(crate) struct QueryRefs {
    pub(crate) fragments: Vec<CreateFragment>,
    pub(crate) physical_sources: Vec<CreatePhysicalSource>,
    pub(crate) sink: CreateSink,
}

/// Everything needed to create a batch of queries: logical sources and per-query refs.
#[derive(Debug, Clone)]
pub(crate) struct QuerySetWithRefs {
    pub(crate) logical_sources: Vec<CreateLogicalSource>,
    pub(crate) queries: Vec<QueryRefs>,
}

impl QuerySetWithRefs {
    /// Strategy that generates `num_queries` queries placed on `workers`.
    ///
    /// Generates 1..=num_queries logical sources. Each query gets physical sources
    /// on its `has_source` fragments (referencing a random logical source) and a
    /// sink on a random fragment.
    pub(crate) fn on_workers(
        workers: Vec<CreateWorker>,
        num_queries: usize,
    ) -> BoxedStrategy<Self> {
        let n = workers.len().max(1);
        let num_sources = 1..=num_queries.max(1);
        (
            vec(any::<CreateLogicalSource>(), num_sources),
            any::<SourceType>(),
            any::<SinkType>(),
            vec(
                (vec((0..n, any::<bool>()), 1..=n), "[a-zA-Z]{1,10}"),
                num_queries,
            ),
        )
            .prop_map(
                move |(logical_sources, source_type, sink_type, query_specs)| {
                    let mut remaining: Vec<i32> = workers
                        .iter()
                        .map(|w| w.max_operators.unwrap_or(i32::MAX))
                        .collect();
                    let total_capacity: i64 = remaining.iter().map(|&c| c as i64).sum();
                    let total_fragments: i64 =
                        query_specs.iter().map(|(p, _)| p.len() as i64).sum();
                    let budget = (total_capacity / total_fragments.max(1)).min(12).max(1) as i32;

                    let num_logical = logical_sources.len();
                    let queries = query_specs
                        .into_iter()
                        .enumerate()
                        .map(|(qi, (placements, sink_prefix))| {
                            let fragments: Vec<CreateFragment> = placements
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
                                .collect();

                            let logical = &logical_sources[qi % num_logical];
                            let physical_sources: Vec<CreatePhysicalSource> = fragments
                                .iter()
                                .filter(|f| f.has_source)
                                .map(|f| CreatePhysicalSource {
                                    logical_source: logical.name.clone(),
                                    host_addr: f.host_addr.clone(),
                                    source_type,
                                    source_config: serde_json::json!({}),
                                    parser_config: serde_json::json!({}),
                                    if_not_exists: true,
                                })
                                .collect();

                            let sink = CreateSink {
                                name: format!("{sink_prefix}_{qi}"),
                                host_addr: fragments[0].host_addr.clone(),
                                sink_type,
                                schema: serde_json::json!({}),
                                config: serde_json::json!({}),
                                if_not_exists: false,
                            };

                            QueryRefs {
                                fragments,
                                physical_sources,
                                sink,
                            }
                        })
                        .collect();

                    QuerySetWithRefs {
                        logical_sources,
                        queries,
                    }
                },
            )
            .boxed()
    }
}
