use crate::source::schema::Schema;
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;

pub type LogicalSourceName = String;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "logical_source")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub name: LogicalSourceName,
    #[sea_orm(column_type = "JsonBinary")]
    pub schema: Schema,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::physical_source::Entity")]
    PhysicalSource,
}

impl Related<super::physical_source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PhysicalSource.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[cfg_attr(feature = "testing", derive(proptest_derive::Arbitrary))]
#[derive(Clone, Debug)]
pub struct CreateLogicalSource {
    #[cfg_attr(
        feature = "testing",
        proptest(regex = "[a-z][a-z0-9_]{2,29}")
    )]
    pub name: LogicalSourceName,
    pub schema: Schema,
}

#[derive(Debug)]
pub struct GetLogicalSource {
    pub with_name: LogicalSourceName,
}

#[derive(Debug)]
pub struct DropLogicalSource {
    pub with_name: LogicalSourceName,
}

impl From<CreateLogicalSource> for ActiveModel {
    fn from(req: CreateLogicalSource) -> Self {
        Self {
            name: Set(req.name),
            schema: Set(req.schema),
        }
    }
}
