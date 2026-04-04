use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "query_sink")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub query_id: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub sink_id: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::Entity",
        from = "Column::QueryId",
        to = "super::Column::Id",
        on_delete = "Cascade",
        on_update = "Restrict"
    )]
    Query,
    #[sea_orm(
        belongs_to = "crate::sink::Entity",
        from = "Column::SinkId",
        to = "crate::sink::Column::Id",
        on_delete = "Cascade",
        on_update = "Restrict"
    )]
    Sink,
}

impl Related<super::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Query.def()
    }
}

impl Related<crate::sink::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Sink.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
