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

use crate::identifier::{QueryId, SinkId};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "query_sink")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub query_id: QueryId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub sink_id: SinkId,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "crate::query::Entity",
        from = "Column::QueryId",
        to = "crate::query::Column::Id",
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
