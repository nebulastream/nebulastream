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
pub mod query_state;

use crate::IntoCondition;
use crate::query::fragment::CreateFragment;
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
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
    pub sql_statement: String,
    #[serde(default)]
    pub block_until: QueryState,
    #[serde(default)]
    pub fragments: Vec<CreateFragment>,
}

impl CreateQuery {
    pub fn new(stmt: String) -> Self {
        Self {
            name: None,
            sql_statement: stmt,
            block_until: QueryState::default(),
            fragments: Vec::new(),
        }
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn block_until(mut self, state: QueryState) -> Self {
        assert!(
            state != QueryState::Failed && state != QueryState::Stopped,
            "Invalid target state: {:?}",
            state
        );
        self.block_until = state;
        self
    }

    pub fn is_blocking(&self) -> bool {
        self.block_until != QueryState::Pending
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
            statement: Set(req.sql_statement),
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
    pub query: CreateQuery,
}

impl Arbitrary for CreateQueryWithRefs {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use fragment::FragmentsWithRefs;
        use proptest::prelude::*;

        let name = (
            proptest::char::range('a', 'z'),
            vec(proptest::char::range('a', 'z'), 2..=10),
        )
            .prop_map(|(first, rest)| std::iter::once(first).chain(rest).collect::<String>());
        let statement = (
            vec(proptest::char::range('a', 'z'), 1..=10),
            vec(proptest::char::range('a', 'z'), 1..=10),
        )
            .prop_map(|(col, table)| {
                format!(
                    "SELECT {} FROM {}",
                    col.into_iter().collect::<String>(),
                    table.into_iter().collect::<String>()
                )
            });
        (
            name,
            statement,
            prop_oneof![
                Just(QueryState::Pending),
                Just(QueryState::Registered),
                Just(QueryState::Running),
                Just(QueryState::Completed),
            ],
            any::<FragmentsWithRefs>(),
        )
            .prop_map(|(name, statement, block_until, fragments)| {
                let query = CreateQuery::new(statement)
                    .name(name)
                    .block_until(block_until)
                    .with_fragments(fragments.create_fragments());
                CreateQueryWithRefs {
                    workers: fragments.workers,
                    query,
                }
            })
            .boxed()
    }
}
