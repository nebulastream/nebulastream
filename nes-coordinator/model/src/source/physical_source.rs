use super::logical_source::LogicalSourceName;
use crate::worker::endpoint::HostAddr;
#[cfg(feature = "testing")]
use proptest_derive::Arbitrary;
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::Condition;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use strum::Display;

pub type PhysicalSourceId = i64;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "physical_source")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: PhysicalSourceId,
    pub logical_source: LogicalSourceName,
    pub host_addr: HostAddr,
    pub source_type: SourceType,
    #[sea_orm(column_type = "Json")]
    pub source_config: Json,
    #[sea_orm(column_type = "Json")]
    pub parser_config: Json,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::logical_source::Entity",
        from = "Column::LogicalSource",
        to = "super::logical_source::Column::Name",
        on_update = "Restrict",
        on_delete = "Restrict"
    )]
    LogicalSource,
    #[sea_orm(
        belongs_to = "crate::worker::Entity",
        from = "Column::HostAddr",
        to = "crate::worker::Column::HostAddr",
        on_update = "Restrict",
        on_delete = "Restrict"
    )]
    Worker,
}

impl Related<super::logical_source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::LogicalSource.def()
    }
}

impl Related<crate::worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Clone, Debug)]
pub struct CreatePhysicalSource {
    pub logical_source: LogicalSourceName,
    pub host_addr: HostAddr,
    pub source_type: SourceType,
    pub source_config: serde_json::Value,
    pub parser_config: serde_json::Value,
}

impl From<CreatePhysicalSource> for ActiveModel {
    fn from(req: CreatePhysicalSource) -> Self {
        Self {
            id: NotSet,
            logical_source: Set(req.logical_source),
            host_addr: Set(req.host_addr),
            source_type: Set(req.source_type),
            source_config: Set(req.source_config),
            parser_config: Set(req.parser_config),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct GetPhysicalSource {
    pub id: Option<PhysicalSourceId>,
    pub host_addr: Option<HostAddr>,
    pub logical_source: Option<LogicalSourceName>,
}

impl GetPhysicalSource {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: PhysicalSourceId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_host_addr(mut self, host_addr: HostAddr) -> Self {
        self.host_addr = Some(host_addr);
        self
    }

    pub fn with_logical_source(mut self, logical_source: LogicalSourceName) -> Self {
        self.logical_source = Some(logical_source);
        self
    }
}

impl crate::IntoCondition for GetPhysicalSource {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.id.map(|v| Column::Id.eq(v)))
            .add_option(self.host_addr.map(|v| Column::HostAddr.eq(v)))
            .add_option(self.logical_source.map(|v| Column::LogicalSource.eq(v)))
    }
}

#[derive(Clone, Debug, Default)]
pub struct DropPhysicalSource {
    pub filters: GetPhysicalSource,
}

impl DropPhysicalSource {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_filters(mut self, filters: GetPhysicalSource) -> Self {
        self.filters = filters;
        self
    }
}

impl crate::IntoCondition for DropPhysicalSource {
    fn into_condition(self) -> Condition {
        self.filters.into_condition()
    }
}

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Display, EnumIter, DeriveActiveEnum, Serialize, Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum SourceType {
    File,
    Tcp,
}

#[cfg(feature = "testing")]
#[derive(Debug, Clone)]
pub struct PhysicalSourceWithRefs {
    pub logical: super::logical_source::CreateLogicalSource,
    pub worker: crate::worker::CreateWorker,
    pub physical: CreatePhysicalSource,
}

#[cfg(feature = "testing")]
impl crate::Generate for PhysicalSourceWithRefs {
    fn generate() -> proptest::strategy::BoxedStrategy<Self> {
        use proptest::prelude::*;
        (
            any::<super::logical_source::CreateLogicalSource>(),
            crate::worker::CreateWorker::generate(),
            any::<SourceType>(),
        )
            .prop_map(|(logical, worker, source_type)| {
                let physical = CreatePhysicalSource {
                    logical_source: logical.name.clone(),
                    host_addr: worker.host_addr.clone(),
                    source_type,
                    source_config: serde_json::json!({}),
                    parser_config: serde_json::json!({}),
                };
                PhysicalSourceWithRefs {
                    logical,
                    worker,
                    physical,
                }
            })
            .boxed()
    }
}
