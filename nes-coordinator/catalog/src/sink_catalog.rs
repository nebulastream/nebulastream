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

use crate::database::Database;
use anyhow::Result;
use model::IntoCondition;
use model::sink;
use model::sink::{CreateSink, DropSink, Entity as SinkEntity, GetSink};
use sea_orm::{ActiveModelTrait, EntityTrait, QueryFilter};
use std::sync::Arc;

pub struct SinkCatalog {
    db: Database,
}

impl SinkCatalog {
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self { db })
    }

    pub async fn create_sink(&self, req: CreateSink) -> Result<sink::Model> {
        Ok(sink::ActiveModel::from(req).insert(&self.db.conn).await?)
    }

    pub async fn get_sink(&self, req: GetSink) -> Result<Vec<sink::Model>> {
        Ok(SinkEntity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await?)
    }

    pub async fn drop_sink(&self, req: DropSink) -> Result<Vec<sink::Model>> {
        let condition = req.into_condition();
        let models = SinkEntity::find()
            .filter(condition.clone())
            .all(&self.db.conn)
            .await?;
        SinkEntity::delete_many()
            .filter(condition)
            .exec(&self.db.conn)
            .await?;
        Ok(models)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Catalog;
    use model::sink::SinkWithRefs;
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get_sink(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(req.worker).await.expect("worker creation should succeed");

        let created = catalog.sink.create_sink(req.sink.clone()).await.expect("sink creation should succeed");
        assert_eq!(created.name, req.sink.name);
        assert_eq!(created.sink_type, req.sink.sink_type);

        let sinks = catalog.sink.get_sink(GetSink::all().with_name(req.sink.name.clone())).await.unwrap();
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].name, req.sink.name);
    }

    #[proptest(async = "tokio")]
    async fn drop_sink(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(req.worker).await.unwrap();
        catalog.sink.create_sink(req.sink.clone()).await.unwrap();

        let dropped = catalog.sink.drop_sink(DropSink { name: req.sink.name.clone() }).await.unwrap();
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].name, req.sink.name);

        let sinks = catalog.sink.get_sink(GetSink::all().with_name(req.sink.name)).await.unwrap();
        assert!(sinks.is_empty(), "Sink should be deleted");
    }

    #[proptest(async = "tokio")]
    async fn sink_name_unique(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(req.worker).await.unwrap();
        catalog.sink.create_sink(req.sink.clone()).await.unwrap();

        assert!(
            catalog.sink.create_sink(req.sink.clone()).await.is_err(),
            "Duplicate sink name '{}' should be rejected", req.sink.name
        );
    }

    #[proptest(async = "tokio")]
    async fn sink_worker_ref_required(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;
        assert!(catalog.sink.create_sink(req.sink.clone()).await.is_err(), "Sink without worker should fail");

        catalog.worker.create_worker(req.worker).await.unwrap();
        catalog.sink.create_sink(req.sink).await.expect("sink with valid worker ref should succeed");
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create_sink(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(req.worker).await.unwrap();
        catalog.sink.create_sink(req.sink.clone()).await.unwrap();

        catalog.sink.drop_sink(DropSink { name: req.sink.name.clone() }).await.unwrap();
        catalog.sink.create_sink(req.sink).await.expect("re-creation after drop should succeed");
    }
}
