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

//! The logical-source entity and the requests that manage it.

mod create;
mod drop;
mod get;

pub use create::CreateLogicalSource;
pub use drop::DropLogicalSource;
pub use get::GetLogicalSource;

use crate::source::physical;
use sea_orm::entity::prelude::*;

/// A user-declared source identified by name.
/// Its schema is shared by every physical source bound to it.
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
    use crate::error::{ErrorCode, code_of};
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
    async fn filters_select_one_of_several(req: CreateLogicalSource) {
        let db = Database::for_test().await;
        let other = CreateLogicalSource {
            name: format!("{}_", req.name),
            ..req.clone()
        };
        let first = req.execute(&db).await.unwrap();
        let second = other.execute(&db).await.unwrap();
        assert_eq!(GetLogicalSource::all().execute(&db).await.unwrap().len(), 2);

        let named = GetLogicalSource::all().with_name(req.name.clone());
        assert_eq!(named.execute(&db).await.unwrap(), [first.clone()]);
        let named = DropLogicalSource::all().with_name(req.name);
        assert_eq!(named.execute(&db).await.unwrap(), [first]);
        assert_eq!(
            DropLogicalSource::all().execute(&db).await.unwrap(),
            [second]
        );
        let remaining = GetLogicalSource::all().execute(&db).await.unwrap();
        assert!(remaining.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn name_unique_until_dropped(req: CreateLogicalSource) {
        let db = Database::for_test().await;
        let created = req.execute(&db).await.unwrap();
        let taken = code_of(req.execute(&db).await);
        assert_eq!(taken, Some(ErrorCode::SourceAlreadyExists));
        let retry = CreateLogicalSource {
            schema: serde_json::json!({"different": "schema"}),
            if_not_exists: true,
            ..req.clone()
        };
        assert_eq!(retry.execute(&db).await.unwrap(), created);
        DropLogicalSource::all()
            .with_name(req.name.clone())
            .execute(&db)
            .await
            .unwrap();
        req.execute(&db).await.unwrap();
    }
}
