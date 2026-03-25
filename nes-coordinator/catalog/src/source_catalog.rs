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
use model::source::logical_source::{
    self, CreateLogicalSource, DropLogicalSource, Entity as LogicalSourceEntity, GetLogicalSource,
};
use model::source::physical_source::{
    self, CreatePhysicalSource, DropPhysicalSource, Entity as PhysicalSourceEntity,
    GetPhysicalSource,
};
use sea_orm::{ActiveModelTrait, EntityTrait, QueryFilter};
use std::sync::Arc;

pub struct SourceCatalog {
    db: Database,
}

impl SourceCatalog {
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self { db })
    }

    pub async fn create_logical_source(
        &self,
        req: CreateLogicalSource,
    ) -> Result<logical_source::Model> {
        Ok(logical_source::ActiveModel::from(req)
            .insert(&self.db.conn)
            .await?)
    }

    pub async fn get_logical_source(
        &self,
        req: GetLogicalSource,
    ) -> Result<Vec<logical_source::Model>> {
        Ok(LogicalSourceEntity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await?)
    }

    pub async fn drop_logical_source(
        &self,
        req: DropLogicalSource,
    ) -> Result<Vec<logical_source::Model>> {
        let condition = req.into_condition();
        let logical_sources = LogicalSourceEntity::find()
            .filter(condition.clone())
            .all(&self.db.conn)
            .await?;
        LogicalSourceEntity::delete_many()
            .filter(condition)
            .exec(&self.db.conn)
            .await?;
        Ok(logical_sources)
    }

    pub async fn create_physical_source(
        &self,
        req: CreatePhysicalSource,
    ) -> Result<physical_source::Model> {
        Ok(physical_source::ActiveModel::from(req)
            .insert(&self.db.conn)
            .await?)
    }

    pub async fn get_physical_source(
        &self,
        req: GetPhysicalSource,
    ) -> Result<Vec<physical_source::Model>> {
        Ok(PhysicalSourceEntity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await?)
    }

    pub async fn drop_physical_source(
        &self,
        req: DropPhysicalSource,
    ) -> Result<Vec<physical_source::Model>> {
        let physical_sources = PhysicalSourceEntity::find()
            .filter(req.clone().into_condition())
            .all(&self.db.conn)
            .await?;
        PhysicalSourceEntity::delete_many()
            .filter(req.into_condition())
            .exec(&self.db.conn)
            .await?;
        Ok(physical_sources)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Catalog;
    use model::source::logical_source::CreateLogicalSource;
    use model::source::physical_source::{DropPhysicalSource, PhysicalSourceWithRefs};
    use model::worker::CreateWorker;
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn get_logical_source(create_source: CreateLogicalSource) {
        let catalog = Catalog::for_test().await;
        let model = catalog.source.create_logical_source(create_source.clone()).await.unwrap();

        assert_eq!(model.name, create_source.name);
        assert_eq!(model.schema, create_source.schema);

        let fetched = catalog.source
            .get_logical_source(GetLogicalSource::all().with_name(create_source.name.clone()))
            .await.unwrap();
        assert_eq!(fetched.first().unwrap().name, create_source.name);
    }

    #[proptest(async = "tokio")]
    async fn capacity_constraints(mut worker: CreateWorker) {
        let catalog = Catalog::for_test().await;

        let mut negative = worker.clone();
        negative.capacity = Some(-(worker.capacity.unwrap_or(1).max(1)));

        assert!(catalog.worker.create_worker(negative).await.is_err(), "Negative capacity should fail");

        worker.capacity = Some(worker.capacity.unwrap_or(0).max(0));
        assert!(catalog.worker.create_worker(worker).await.is_ok(), "Non-negative capacity should succeed");
    }

    #[proptest(async = "tokio")]
    async fn get_physical_source(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog.source.create_logical_source(req.logical.clone()).await.unwrap();
        catalog.worker.create_worker(req.worker.clone()).await.unwrap();
        catalog.source.create_physical_source(req.physical.clone()).await.unwrap();

        let rsp = catalog.source
            .get_physical_source(GetPhysicalSource::all().with_logical_source(req.logical.name.clone()))
            .await.unwrap();

        assert_eq!(rsp.len(), 1);
        assert_eq!(rsp[0].logical_source, req.logical.name);
        assert_eq!(rsp[0].host_addr, req.worker.host_addr);
        assert_eq!(rsp[0].source_type, req.physical.source_type);
    }

    #[proptest(async = "tokio")]
    async fn logical_name_unique(create_source: CreateLogicalSource) {
        let catalog = Catalog::for_test().await;
        catalog.source.create_logical_source(create_source.clone()).await.unwrap();

        assert!(
            catalog.source.create_logical_source(create_source).await.is_err(),
            "Duplicate logical source should be rejected"
        );
    }

    #[proptest(async = "tokio")]
    async fn physical_source_refs_exist(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        assert!(catalog.source.create_physical_source(req.physical.clone()).await.is_err());

        catalog.source.create_logical_source(req.logical).await.unwrap();
        assert!(catalog.source.create_physical_source(req.physical.clone()).await.is_err());

        catalog.worker.create_worker(req.worker).await.unwrap();
        catalog.source.create_physical_source(req.physical).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn physical_source_drop_with_refs(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog.source.create_logical_source(req.logical.clone()).await.unwrap();
        catalog.worker.create_worker(req.worker).await.unwrap();
        catalog.source.create_physical_source(req.physical).await.unwrap();

        let drop_req = DropLogicalSource { with_name: Some(req.logical.name.clone()) };
        assert!(
            catalog.source.drop_logical_source(drop_req).await.is_err(),
            "Cannot drop logical source '{}' while physical sources reference it", req.logical.name
        );
    }

    #[proptest(async = "tokio")]
    async fn drop_physical_by_id(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog.source.create_logical_source(req.logical).await.unwrap();
        catalog.worker.create_worker(req.worker).await.unwrap();
        let created = catalog.source.create_physical_source(req.physical).await.unwrap();

        let drop_req = DropPhysicalSource::all().with_filters(GetPhysicalSource::all().with_id(created.id));
        let dropped = catalog.source.drop_physical_source(drop_req).await.unwrap();
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].id, created.id);

        let remaining = catalog.source.get_physical_source(GetPhysicalSource::all().with_id(created.id)).await.unwrap();
        assert!(remaining.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn drop_physical_by_logical_source(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog.source.create_logical_source(req.logical.clone()).await.unwrap();
        catalog.worker.create_worker(req.worker).await.unwrap();
        catalog.source.create_physical_source(req.physical).await.unwrap();

        let drop_req = DropPhysicalSource::all()
            .with_filters(GetPhysicalSource::all().with_logical_source(req.logical.name.clone()));
        let dropped = catalog.source.drop_physical_source(drop_req).await.unwrap();
        assert_eq!(dropped.len(), 1);

        let remaining = catalog.source
            .get_physical_source(GetPhysicalSource::all().with_logical_source(req.logical.name))
            .await.unwrap();
        assert!(remaining.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn drop_physical_no_match_noop(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog.source.create_logical_source(req.logical).await.unwrap();
        catalog.worker.create_worker(req.worker.clone()).await.unwrap();
        catalog.source.create_physical_source(req.physical).await.unwrap();

        let drop_req = DropPhysicalSource::all().with_filters(GetPhysicalSource::all().with_id(999999));
        assert!(catalog.source.drop_physical_source(drop_req).await.unwrap().is_empty());

        let remaining = catalog.source
            .get_physical_source(GetPhysicalSource::all().with_host_addr(req.worker.host_addr))
            .await.unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create_physical(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog.source.create_logical_source(req.logical).await.unwrap();
        catalog.worker.create_worker(req.worker).await.unwrap();
        let first = catalog.source.create_physical_source(req.physical.clone()).await.unwrap();

        let drop_req = DropPhysicalSource::all().with_filters(GetPhysicalSource::all().with_id(first.id));
        catalog.source.drop_physical_source(drop_req).await.unwrap();

        catalog.source.create_physical_source(req.physical).await.expect("re-creation should succeed");
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create_logical(create_source: CreateLogicalSource) {
        let catalog = Catalog::for_test().await;
        catalog.source.create_logical_source(create_source.clone()).await.unwrap();

        let drop_req = DropLogicalSource { with_name: Some(create_source.name.clone()) };
        catalog.source.drop_logical_source(drop_req).await.unwrap();

        catalog.source.create_logical_source(create_source).await.expect("re-creation should succeed");
    }
}
