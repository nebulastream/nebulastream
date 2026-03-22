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
    use crate::{test_prop, Catalog};
    use model::Generate;
    use model::sink::SinkWithRefs;
    use proptest::proptest;

    async fn prop_create_and_get_sink(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;

        catalog
            .worker
            .create_worker(req.worker)
            .await
            .expect("worker creation should succeed");

        let created = catalog
            .sink
            .create_sink(req.sink.clone())
            .await
            .expect("sink creation should succeed");

        assert_eq!(created.name, req.sink.name);
        assert_eq!(created.sink_type, req.sink.sink_type);

        let sinks = catalog
            .sink
            .get_sink(GetSink::all().with_name(req.sink.name.clone()))
            .await
            .expect("get sink should succeed");

        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].name, req.sink.name);
        assert_eq!(sinks[0].sink_type, req.sink.sink_type);
    }

    async fn prop_drop_sink(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;

        catalog.worker.create_worker(req.worker).await.unwrap();
        catalog.sink.create_sink(req.sink.clone()).await.unwrap();

        let dropped = catalog
            .sink
            .drop_sink(DropSink { name: req.sink.name.clone() })
            .await
            .expect("drop should succeed");

        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].name, req.sink.name);

        let sinks = catalog
            .sink
            .get_sink(GetSink::all().with_name(req.sink.name))
            .await
            .unwrap();
        assert!(sinks.is_empty(), "Sink should be deleted");
    }

    async fn prop_sink_name_unique(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;

        catalog
            .worker
            .create_worker(req.worker)
            .await
            .expect("worker creation should succeed");

        catalog
            .sink
            .create_sink(req.sink.clone())
            .await
            .expect("first sink creation should succeed");

        assert!(
            catalog.sink.create_sink(req.sink.clone()).await.is_err(),
            "Duplicate sink name '{}' should be rejected",
            req.sink.name
        );
    }

    async fn prop_sink_worker_ref_required(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;

        assert!(
            catalog.sink.create_sink(req.sink.clone()).await.is_err(),
            "Sink creation without worker should be rejected"
        );

        catalog
            .worker
            .create_worker(req.worker)
            .await
            .expect("worker creation should succeed");

        catalog
            .sink
            .create_sink(req.sink)
            .await
            .expect("sink creation with valid worker ref should succeed");
    }

    async fn prop_create_drop_create_sink(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;

        catalog
            .worker
            .create_worker(req.worker)
            .await
            .expect("worker creation should succeed");

        catalog
            .sink
            .create_sink(req.sink.clone())
            .await
            .expect("first sink creation should succeed");

        let drop_req = DropSink { name: req.sink.name.clone() };
        catalog
            .sink
            .drop_sink(drop_req)
            .await
            .expect("drop should succeed");

        catalog
            .sink
            .create_sink(req.sink)
            .await
            .expect("second creation after drop should succeed");
    }

    proptest! {
        #[test]
        fn create_and_get_sink(req in SinkWithRefs::generate()) {
            test_prop(|| async move {
                prop_create_and_get_sink(req).await;
            });
        }

        #[test]
        fn drop_sink(req in SinkWithRefs::generate()) {
            test_prop(|| async move {
                prop_drop_sink(req).await;
            });
        }

        #[test]
        fn sink_name_unique(req in SinkWithRefs::generate()) {
            test_prop(|| async move {
                prop_sink_name_unique(req).await;
            });
        }

        #[test]
        fn sink_worker_ref_required(req in SinkWithRefs::generate()) {
            test_prop(|| async move {
                prop_sink_worker_ref_required(req).await;
            });
        }

        #[test]
        fn create_drop_create_sink_succeeds(req in SinkWithRefs::generate()) {
            test_prop(|| async move {
                prop_create_drop_create_sink(req).await;
            });
        }
    }
}
