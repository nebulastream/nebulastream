pub mod database;
pub mod error;
pub mod notification;
pub mod query_catalog;
pub mod sink_catalog;
pub mod source_catalog;
pub mod worker_catalog;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use notification::Reconcilable;

use database::Database;
use query_catalog::QueryCatalog;
use sink_catalog::SinkCatalog;
use source_catalog::SourceCatalog;
use std::sync::Arc;
use worker_catalog::WorkerCatalog;

#[derive(Clone)]
pub struct Catalog {
    pub source: Arc<SourceCatalog>,
    pub sink: Arc<SinkCatalog>,
    pub worker: Arc<WorkerCatalog>,
    pub query: Arc<QueryCatalog>,
}

impl Catalog {
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self {
            source: SourceCatalog::from(db.clone()),
            sink: SinkCatalog::from(db.clone()),
            worker: WorkerCatalog::from(db.clone()),
            query: QueryCatalog::from(db),
        })
    }

    #[cfg(any(test, feature = "testing"))]
    pub async fn for_test() -> Arc<Self> {
        Self::from(Database::for_test().await)
    }
}
