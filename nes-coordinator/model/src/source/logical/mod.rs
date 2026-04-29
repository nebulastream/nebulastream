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

mod create;
mod drop;
mod get;

pub use create::CreateLogicalSource;
pub use drop::DropLogicalSource;
pub use get::GetLogicalSource;

use crate::source::physical;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "logical_source")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub name: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub schema: Json,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "crate::source::physical::Entity")]
    PhysicalSource,
}

impl Related<physical::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PhysicalSource.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::source::logical::{CreateLogicalSource, DropLogicalSource, GetLogicalSource};
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get(req: CreateLogicalSource) {
        let db = Database::for_test().await;
        let created = req.execute(&db).await.unwrap();
        assert_eq!(created.name, req.name);
        assert_eq!(created.schema, req.schema);

        let sources = GetLogicalSource::all()
            .with_name(req.name.clone())
            .execute(&db)
            .await
            .unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].name, req.name);
    }

    #[proptest(async = "tokio")]
    async fn name_unique(req: CreateLogicalSource) {
        let db = Database::for_test().await;
        req.execute(&db).await.unwrap();
        assert!(req.execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create(req: CreateLogicalSource) {
        let db = Database::for_test().await;
        req.execute(&db).await.unwrap();
        DropLogicalSource { with_name: Some(req.name.clone()) }
            .execute(&db)
            .await
            .unwrap();
        req.execute(&db).await.unwrap();
    }
}
