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
    ) -> Result<Option<logical_source::Model>> {
        Ok(LogicalSourceEntity::find_by_id(req.with_name)
            .one(&self.db.conn)
            .await?)
    }

    pub async fn drop_logical_source(
        &self,
        req: DropLogicalSource,
    ) -> Result<Option<logical_source::Model>> {
        let model = LogicalSourceEntity::find_by_id(req.with_name.clone())
            .one(&self.db.conn)
            .await?;
        if model.is_some() {
            LogicalSourceEntity::delete_by_id(req.with_name)
                .exec(&self.db.conn)
                .await?;
        }
        Ok(model)
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
        let models = PhysicalSourceEntity::find()
            .filter(req.clone().into_condition())
            .all(&self.db.conn)
            .await?;
        PhysicalSourceEntity::delete_many()
            .filter(req.into_condition())
            .exec(&self.db.conn)
            .await?;
        Ok(models)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Catalog;
    use crate::testing::test_prop;
    use model::source::physical_source::DropPhysicalSource;
    use model::source::logical_source::CreateLogicalSource;
    use model::Generate;
    use model::source::physical_source::PhysicalSourceWithRefs;
    use model::worker::CreateWorker;
    use proptest::prelude::*;
    use proptest::proptest;

    async fn prop_get_logical_source(create_source: CreateLogicalSource) {
        let catalog = Catalog::for_test().await;

        let model = catalog
            .source
            .create_logical_source(create_source.clone())
            .await
            .expect("logical source creation should succeed");

        assert_eq!(model.name, create_source.name);
        assert_eq!(model.schema, create_source.schema);

        let fetched = catalog
            .source
            .get_logical_source(GetLogicalSource {
                with_name: create_source.name.clone(),
            })
            .await
            .expect("get should not fail");

        let fetched = fetched.expect("get request should return a value");
        assert_eq!(fetched.name, create_source.name);
        assert_eq!(fetched.schema, create_source.schema);
    }

    async fn prop_capacity_constraints(worker: CreateWorker) {
        let catalog = Catalog::for_test().await;

        let mut negative = worker.clone();
        negative.capacity = -(worker.capacity.max(1));

        assert!(
            catalog.worker.create_worker(negative).await.is_err(),
            "Cannot create worker with negative capacity"
        );
        assert!(
            catalog.worker.create_worker(worker).await.is_ok(),
            "Worker with non-negative capacity is allowed"
        );
    }

    async fn prop_get_physical_source(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;

        catalog
            .source
            .create_logical_source(req.logical.clone())
            .await
            .expect("logical source creation should succeed");
        catalog
            .worker
            .create_worker(req.worker.clone())
            .await
            .expect("worker creation should succeed");
        catalog
            .source
            .create_physical_source(req.physical.clone())
            .await
            .expect("physical source creation should succeed");

        let rsp = catalog
            .source
            .get_physical_source(
                GetPhysicalSource::all().with_logical_source(req.logical.name.clone()),
            )
            .await
            .unwrap();

        assert_eq!(rsp.len(), 1);
        assert_eq!(rsp[0].logical_source, req.logical.name);
        assert_eq!(rsp[0].host_addr, req.worker.host_addr);
        assert_eq!(rsp[0].source_type, req.physical.source_type);
    }

    async fn prop_logical_name_unique(create_source: CreateLogicalSource) {
        let catalog = Catalog::for_test().await;
        let model = catalog
            .source
            .create_logical_source(create_source.clone())
            .await
            .expect("first logical source creation should succeed");

        assert_eq!(model.name, create_source.name);
        assert_eq!(model.schema, create_source.schema);

        assert!(
            catalog
                .source
                .create_logical_source(create_source)
                .await
                .is_err(),
            "Duplicate logical source with name '{}' should be rejected",
            model.name
        );
    }

    async fn prop_physical_refs_exist(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        assert!(
            catalog
                .source
                .create_physical_source(req.physical.clone())
                .await
                .is_err(),
            "Physical source with missing refs should be rejected",
        );

        catalog
            .source
            .create_logical_source(req.logical)
            .await
            .expect("logical source creation should succeed");

        assert!(
            catalog
                .source
                .create_physical_source(req.physical.clone())
                .await
                .is_err(),
            "Physical source with missing worker ref should be rejected",
        );

        catalog
            .worker
            .create_worker(req.worker)
            .await
            .expect("worker creation should succeed");

        catalog
            .source
            .create_physical_source(req.physical)
            .await
            .expect("physical source with valid refs should succeed");
    }

    async fn prop_drop_with_refs_fails(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog
            .source
            .create_logical_source(req.logical.clone())
            .await
            .expect("logical source creation should succeed");
        catalog
            .worker
            .create_worker(req.worker)
            .await
            .expect("worker creation should succeed");
        catalog
            .source
            .create_physical_source(req.physical)
            .await
            .expect("physical source creation should succeed");

        let drop_request = DropLogicalSource {
            with_name: req.logical.name.clone(),
        };

        assert!(
            catalog
                .source
                .drop_logical_source(drop_request)
                .await
                .is_err(),
            "Should not be able to drop logical source '{}' while physical sources reference it",
            req.logical.name
        );
    }

    async fn prop_drop_physical_by_id(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog
            .source
            .create_logical_source(req.logical)
            .await
            .unwrap();
        catalog
            .worker
            .create_worker(req.worker)
            .await
            .unwrap();
        let created = catalog
            .source
            .create_physical_source(req.physical)
            .await
            .unwrap();

        let drop_req = DropPhysicalSource::all().with_filters(GetPhysicalSource::all().with_id(created.id));
        let dropped = catalog
            .source
            .drop_physical_source(drop_req)
            .await
            .unwrap();

        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].id, created.id);
        assert_eq!(dropped[0].logical_source, created.logical_source);
        assert_eq!(dropped[0].host_addr, created.host_addr);

        let remaining = catalog
            .source
            .get_physical_source(GetPhysicalSource::all().with_id(created.id))
            .await
            .unwrap();
        assert!(
            remaining.is_empty(),
            "Physical source should be removed after drop"
        );
    }

    async fn prop_drop_physical_by_logical_source(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog
            .source
            .create_logical_source(req.logical.clone())
            .await
            .unwrap();
        catalog
            .worker
            .create_worker(req.worker)
            .await
            .unwrap();
        catalog
            .source
            .create_physical_source(req.physical)
            .await
            .unwrap();

        let drop_req = DropPhysicalSource::all()
            .with_filters(GetPhysicalSource::all().with_logical_source(req.logical.name.clone()));
        let dropped = catalog
            .source
            .drop_physical_source(drop_req)
            .await
            .unwrap();

        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].logical_source, req.logical.name);

        let remaining = catalog
            .source
            .get_physical_source(
                GetPhysicalSource::all().with_logical_source(req.logical.name.clone()),
            )
            .await
            .unwrap();
        assert!(
            remaining.is_empty(),
            "All physical sources for '{}' should be removed",
            req.logical.name
        );
    }

    async fn prop_drop_physical_no_match_noop(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog
            .source
            .create_logical_source(req.logical)
            .await
            .unwrap();
        catalog
            .worker
            .create_worker(req.worker.clone())
            .await
            .unwrap();
        catalog
            .source
            .create_physical_source(req.physical)
            .await
            .unwrap();

        let drop_req = DropPhysicalSource::all().with_filters(GetPhysicalSource::all().with_id(999999));
        let dropped = catalog
            .source
            .drop_physical_source(drop_req)
            .await
            .unwrap();
        assert!(dropped.is_empty());

        let remaining = catalog
            .source
            .get_physical_source(GetPhysicalSource::all().with_host_addr(req.worker.host_addr))
            .await
            .unwrap();
        assert_eq!(
            remaining.len(),
            1,
            "Existing physical source should not be affected"
        );
    }

    async fn prop_create_drop_create_physical(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog
            .source
            .create_logical_source(req.logical)
            .await
            .unwrap();
        catalog
            .worker
            .create_worker(req.worker)
            .await
            .unwrap();
        let first = catalog
            .source
            .create_physical_source(req.physical.clone())
            .await
            .unwrap();

        let drop_req = DropPhysicalSource::all().with_filters(GetPhysicalSource::all().with_id(first.id));
        catalog
            .source
            .drop_physical_source(drop_req)
            .await
            .unwrap();

        catalog
            .source
            .create_physical_source(req.physical)
            .await
            .expect("second creation after drop should succeed");
    }

    async fn prop_create_drop_create_logical(create_source: CreateLogicalSource) {
        let catalog = Catalog::for_test().await;
        catalog
            .source
            .create_logical_source(create_source.clone())
            .await
            .expect("first create should succeed");

        let drop_request = DropLogicalSource {
            with_name: create_source.name.clone(),
        };

        catalog
            .source
            .drop_logical_source(drop_request)
            .await
            .expect("drop should succeed");

        catalog
            .source
            .create_logical_source(create_source)
            .await
            .expect("second create after drop should succeed");
    }

    proptest! {
        #[test]
        fn get_logical_source(create_source in any::<CreateLogicalSource>()) {
            test_prop(|| async move {
                prop_get_logical_source(create_source).await;
            });
        }

        #[test]
        fn capacity_constraints(worker in CreateWorker::generate()) {
            test_prop(|| async move {
                prop_capacity_constraints(worker).await;
            });
        }

        #[test]
        fn get_physical_source(req in PhysicalSourceWithRefs::generate()) {
            test_prop(|| async move {
                prop_get_physical_source(req).await;
            });
        }

        #[test]
        fn logical_name_unique(create_source: CreateLogicalSource) {
            test_prop(|| async move {
                prop_logical_name_unique(create_source).await;
            });
        }

        #[test]
        fn physical_source_refs_exist(req in PhysicalSourceWithRefs::generate()) {
            test_prop(|| async move {
                prop_physical_refs_exist(req).await;
            })
        }

        #[test]
        fn physical_source_drop_with_refs(req in PhysicalSourceWithRefs::generate()) {
            test_prop(|| async move {
                prop_drop_with_refs_fails(req).await;
            })
        }

        #[test]
        fn create_drop_create_logical_succeeds(create_source: CreateLogicalSource) {
            test_prop(|| async move {
                prop_create_drop_create_logical(create_source).await;
            })
        }

        #[test]
        fn drop_physical_by_id(req in PhysicalSourceWithRefs::generate()) {
            test_prop(|| async move {
                prop_drop_physical_by_id(req).await;
            })
        }

        #[test]
        fn drop_physical_by_logical_source(req in PhysicalSourceWithRefs::generate()) {
            test_prop(|| async move {
                prop_drop_physical_by_logical_source(req).await;
            })
        }

        #[test]
        fn drop_physical_no_match_noop(req in PhysicalSourceWithRefs::generate()) {
            test_prop(|| async move {
                prop_drop_physical_no_match_noop(req).await;
            })
        }

        #[test]
        fn create_drop_create_physical_succeeds(req in PhysicalSourceWithRefs::generate()) {
            test_prop(|| async move {
                prop_create_drop_create_physical(req).await;
            })
        }
    }
}
