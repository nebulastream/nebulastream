pub mod fragment;
pub mod query_state;

use crate::IntoCondition;
#[cfg(feature = "testing")]
use proptest_derive::Arbitrary;
use query_state::{DesiredQueryState, QueryState};
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::Condition;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use strum::Display;
use uuid::Uuid;

pub type QueryName = String;
pub type QueryId = i64;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "query")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub name: QueryName,
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

#[cfg_attr(feature = "testing", derive(Arbitrary))]
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

#[derive(Clone, Debug)]
pub struct CreateQuery {
    pub name: QueryName,
    pub sql_statement: String,
    pub block_until: QueryState,
}

impl CreateQuery {
    pub fn new(statement: String) -> Self {
        Self {
            name: Uuid::new_v4().to_string(),
            sql_statement: statement,
            block_until: QueryState::default(),
        }
    }

    pub fn name(mut self, name: QueryName) -> Self {
        self.name = name;
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

    pub fn blocking(&self) -> bool {
        self.block_until != QueryState::Pending
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

#[derive(Clone, Debug, Default)]
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

    pub fn stop_mode(mut self, stop_mode: StopMode) -> Self {
        self.stop_mode = stop_mode;
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct GetQuery {
    pub ids: Option<Vec<QueryId>>,
    pub name: Option<QueryName>,
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

    pub fn with_name(mut self, name: QueryName) -> Self {
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

#[cfg(feature = "testing")]
impl crate::Generate for CreateQuery {
    fn generate() -> proptest::strategy::BoxedStrategy<Self> {
        use proptest::prelude::*;
        let name = (
            proptest::char::range('a', 'z'),
            proptest::collection::vec(proptest::char::range('a', 'z'), 2..=10),
        )
            .prop_map(|(first, rest)| std::iter::once(first).chain(rest).collect::<String>());
        let statement = (
            proptest::collection::vec(proptest::char::range('a', 'z'), 1..=10),
            proptest::collection::vec(proptest::char::range('a', 'z'), 1..=10),
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
        )
            .prop_map(|(name, statement, block_until)| {
                CreateQuery::new(statement)
                    .name(name)
                    .block_until(block_until)
            })
            .boxed()
    }
}
