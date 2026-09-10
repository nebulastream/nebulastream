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

//! The ML-model entity: a registered inference model, holding the original file
//! path and the MLIR body imported at registration time.

mod create;
mod drop;
mod get;

pub use create::CreateMlModel;
pub use drop::DropMlModel;
pub use get::GetMlModel;

use sea_orm::entity::prelude::*;

/// Registered ML inference model. `path` is the original source on the
/// coordinator's filesystem (kept for `SHOW` output and re-registration
/// after a path change). `imported` is the MLIR body produced at
/// registration time and carried inline inside every `infer_model`
/// operator; the coordinator does not re-read the file after import.
///
/// The MLIR is embedded in plans at registration, so dropping a model row
/// does not affect running queries. Unlike sources and sinks, `ml_model` has
/// no query relation and no drop-guard.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "ml_model")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub name: String,
    pub path: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub input_schema: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub output_schema: Json,
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
    use crate::error::{CodedError, ErrorCode};
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
    async fn name_unique(req: CreateMlModel) {
        let db = Database::for_test().await;
        req.execute(&db).await.unwrap();
        let error = req.execute(&db).await.expect_err("the name is taken");
        // A name that is taken is the caller's error, so it reports itself rather than reporting
        // that some write did not go through.
        assert_eq!(
            error.downcast_ref::<CodedError>().map(|coded| coded.code),
            Some(ErrorCode::ModelAlreadyExists)
        );
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create(req: CreateMlModel) {
        let db = Database::for_test().await;
        req.execute(&db).await.unwrap();
        DropMlModel::all()
            .with_name(req.name.clone())
            .execute(&db)
            .await
            .unwrap();
        req.execute(&db).await.unwrap();
    }
}
