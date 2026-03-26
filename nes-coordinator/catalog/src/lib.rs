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

pub mod database;
pub mod error;
pub mod notification;
pub mod query_catalog;
pub mod sink_catalog;
pub mod source_catalog;
pub mod worker_catalog;

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

    pub async fn for_test() -> Arc<Self> {
        Self::from(Database::for_test().await)
    }
}

