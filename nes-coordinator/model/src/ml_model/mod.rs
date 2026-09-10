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

//! The ML-model entity: a registered inference model.

mod create;
mod drop;
mod get;

pub use create::CreateMlModel;
pub use drop::DropMlModel;
pub use get::GetMlModel;

use sea_orm::entity::prelude::*;

/// A registered ML inference model.
///
/// The MLIR body is copied into each query plan that uses the model,
/// so dropping a model row does not affect running queries.
/// Unlike sources and sinks, `ml_model` has no query relation and no drop guard.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "ml_model")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub name: String,
    /// The original file on the coordinator's filesystem,
    /// kept for `SHOW` output and for re-registration after a path change.
    pub path: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub input_schema: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub output_schema: Json,
    /// The MLIR body produced at registration time; the file is not read again after import.
    #[serde(skip)]
    #[sea_orm(column_type = "JsonBinary")]
    pub imported: Json,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::error::{ErrorCode, code_of};
    use crate::ml_model::{CreateMlModel, DropMlModel, GetMlModel};
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get(req: CreateMlModel) {
        let db = Database::for_test().await;
        let created = req.execute(&db).await.unwrap();
        assert_eq!(created.name, req.name);
        assert_eq!(created.path, req.path);
        assert_eq!(created.input_schema, req.input_schema);
        assert_eq!(created.output_schema, req.output_schema);
        assert_eq!(created.imported, req.imported);

        let models = GetMlModel::all()
            .with_name(req.name.clone())
            .execute(&db)
            .await
            .unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0], created);
    }

    #[proptest(async = "tokio")]
    async fn filters_select_one_of_several(req: CreateMlModel) {
        let db = Database::for_test().await;
        let other = CreateMlModel {
            name: format!("{}_", req.name),
            ..req.clone()
        };
        let first = req.execute(&db).await.unwrap();
        let second = other.execute(&db).await.unwrap();
        assert_eq!(GetMlModel::all().execute(&db).await.unwrap().len(), 2);

        let named = GetMlModel::all().with_name(req.name.clone());
        assert_eq!(named.execute(&db).await.unwrap(), [first.clone()]);
        let named = DropMlModel::all().with_name(req.name);
        assert_eq!(named.execute(&db).await.unwrap(), [first]);
        assert_eq!(DropMlModel::all().execute(&db).await.unwrap(), [second]);
        assert!(GetMlModel::all().execute(&db).await.unwrap().is_empty());
    }

    #[proptest(async = "tokio")]
    async fn name_unique_until_dropped(req: CreateMlModel) {
        let db = Database::for_test().await;
        let created = req.execute(&db).await.unwrap();
        let taken = code_of(req.execute(&db).await);
        // A name that is taken is the caller's error,
        // so it reports itself rather than reporting that some write did not go through.
        assert_eq!(taken, Some(ErrorCode::ModelAlreadyExists));
        let retry = CreateMlModel {
            input_schema: serde_json::json!({"different": "schema"}),
            if_not_exists: true,
            ..req.clone()
        };
        assert_eq!(retry.execute(&db).await.unwrap(), created);
        DropMlModel::all()
            .with_name(req.name.clone())
            .execute(&db)
            .await
            .unwrap();
        req.execute(&db).await.unwrap();
    }
}
