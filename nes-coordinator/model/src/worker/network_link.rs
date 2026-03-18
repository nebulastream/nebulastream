use crate::worker::endpoint::HostAddr;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "network_link")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_host_addr: HostAddr,
    #[sea_orm(primary_key, auto_increment = false)]
    pub target_host_addr: HostAddr,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::Entity",
        from = "Column::SourceHostAddr",
        to = "super::Column::HostAddr"
    )]
    SourceWorker,
    #[sea_orm(
        belongs_to = "super::Entity",
        from = "Column::TargetHostAddr",
        to = "super::Column::HostAddr"
    )]
    TargetWorker,
}

impl ActiveModelBehavior for ActiveModel {}
