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

pub mod fragment;
pub mod query_sink;
pub mod query_source;
pub mod query_state;

use crate::IntoCondition;
use crate::query::fragment::CreateFragment;
use crate::sink::{CreateSink, SinkType};
use crate::source::logical_source::CreateLogicalSource;
use crate::source::physical_source::{CreatePhysicalSource, SourceType};
use crate::worker::CreateWorker;
use proptest::arbitrary::Arbitrary;
use proptest::collection::vec;
use proptest::strategy::BoxedStrategy;
use proptest_derive::Arbitrary;
use query_state::{DesiredQueryState, QueryState};
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::Condition;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use strum::Display;

pub type QueryId = i64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "query")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: QueryId,
    pub name: Option<String>,
    pub statement: String,
    pub current_state: QueryState,
    pub desired_state: DesiredQueryState,
    pub start_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stop_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stop_mode: Option<StopMode>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<serde_json::Value>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "fragment::Entity")]
    Fragment,
}

impl Related<fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(
    Arbitrary,
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

#[derive(Clone, Debug, Deserialize)]
pub struct CreateQuery {
    #[serde(default)]
    pub name: Option<String>,
    pub sql: String,
    #[serde(default)]
    pub fragments: Vec<CreateFragment>,
    #[serde(default)]
    pub source_ids: Vec<i64>,
    #[serde(default)]
    pub sink_ids: Vec<i64>,
}

impl CreateQuery {
    pub fn new(sql: String) -> Self {
        Self {
            name: None,
            sql,
            fragments: Vec::new(),
            source_ids: Vec::new(),
            sink_ids: Vec::new(),
        }
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_fragments(mut self, fragments: Vec<CreateFragment>) -> Self {
        self.fragments = fragments;
        self
    }
}

impl From<CreateQuery> for ActiveModel {
    fn from(req: CreateQuery) -> Self {
        Self {
            id: NotSet,
            name: Set(req.name),
            statement: Set(req.sql),
            current_state: NotSet,
            desired_state: NotSet,
            start_timestamp: NotSet,
            stop_timestamp: NotSet,
            stop_mode: NotSet,
            error: NotSet,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DropQuery {
    pub stop_mode: StopMode,
    pub filters: GetQuery,
    #[serde(default)]
    pub should_block: bool,
}

impl DropQuery {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_filters(mut self, filters: GetQuery) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_stop_mode(mut self, stop_mode: StopMode) -> Self {
        self.stop_mode = stop_mode;
        self
    }

    pub fn should_block(mut self) -> Self {
        self.should_block = true;
        self
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct GetQuery {
    pub ids: Option<Vec<QueryId>>,
    pub name: Option<String>,
    pub with_fragments: bool,
}

impl GetQuery {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: QueryId) -> Self {
        self.ids = Some(vec![id]);
        self
    }

    pub fn with_ids(mut self, ids: Vec<QueryId>) -> Self {
        self.ids = Some(ids);
        self
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_fragments(mut self) -> Self {
        self.with_fragments = true;
        self
    }
}

impl IntoCondition for GetQuery {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.ids.map(|ids| Column::Id.is_in(ids)))
            .add_option(self.name.map(|v| Column::Name.eq(v)))
    }
}

#[derive(Debug, Clone)]
pub struct CreateQueryWithRefs {
    pub workers: Vec<CreateWorker>,
    pub logical_source: CreateLogicalSource,
    pub physical_sources: Vec<CreatePhysicalSource>,
    pub sink: CreateSink,
    pub query: CreateQuery,
}

impl Arbitrary for CreateQueryWithRefs {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use fragment::FragmentsWithRefs;
        use proptest::prelude::*;
        use proptest::char::range;

        let name = (
            range('a', 'z'),
            vec(range('a', 'z'), 2..=10),
        )
            .prop_map(|(first, rest)| std::iter::once(first).chain(rest).collect::<String>());
        let statement = (
            vec(range('a', 'z'), 1..=10),
            vec(range('a', 'z'), 1..=10),
            vec(range('a', 'z'), 1..=10),
        )
            .prop_map(|(field, source, sink)| {
                format!(
                    "SELECT {} FROM {} INTO {};",
                    field.into_iter().collect::<String>(),
                    source.into_iter().collect::<String>(),
                    sink.into_iter().collect::<String>(),
                )
            });
        let sink_name = (
            range('a', 'z'),
            vec(range('a', 'z'), 2..=10),
        )
            .prop_map(|(first, rest)| std::iter::once(first).chain(rest).collect::<String>());
        any::<FragmentsWithRefs>()
            .prop_flat_map(move |fragments| {
                let n = fragments.fragments.len();
                (
                    Just(fragments),
                    name.clone(),
                    statement.clone(),
                    any::<CreateLogicalSource>(),
                    any::<SourceType>(),
                    sink_name.clone(),
                    any::<SinkType>(),
                    0..n,
                )
            })
            .prop_map(|(fragments, name, statement, logical_source, source_type, sink_name, sink_type, sink_frag_idx)| {
                let mut physical_sources: Vec<CreatePhysicalSource> = fragments.fragments.iter()
                    .filter(|f| f.has_source)
                    .map(|f| CreatePhysicalSource {
                        logical_source: logical_source.name.clone(),
                        host_addr: f.host_addr.clone(),
                        source_type,
                        source_config: serde_json::json!({}),
                        parser_config: serde_json::json!({}),
                        if_not_exists: false,
                    })
                    .collect();
                if physical_sources.is_empty() {
                    physical_sources.push(CreatePhysicalSource {
                        logical_source: logical_source.name.clone(),
                        host_addr: fragments.fragments[0].host_addr.clone(),
                        source_type,
                        source_config: serde_json::json!({}),
                        parser_config: serde_json::json!({}),
                        if_not_exists: false,
                    });
                }
                let sink = CreateSink {
                    name: sink_name,
                    host_addr: fragments.fragments[sink_frag_idx].host_addr.clone(),
                    sink_type,
                    schema: serde_json::json!({}),
                    config: serde_json::json!({}),
                    if_not_exists: false,
                };
                let query = CreateQuery::new(statement)
                    .name(name)
                    .with_fragments(fragments.fragments);
                CreateQueryWithRefs {
                    workers: fragments.workers,
                    logical_source,
                    physical_sources,
                    sink,
                    query,
                }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
