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

pub(crate) mod create;
mod drop;
pub(crate) mod get;

pub use create::{CreateInlineSource, CreatePhysicalSource};
pub use drop::DropPhysicalSource;
pub use get::GetPhysicalSource;

use crate::identifier::SourceId;
use crate::source::logical;
#[cfg(test)]
use crate::source::logical::CreateLogicalSource;
#[cfg(test)]
use crate::worker::CreateWorker;
use crate::worker::endpoint::NetworkAddr;
use crate::{ConnectorKind, worker};
#[cfg(test)]
use proptest::arbitrary::Arbitrary;
#[cfg(test)]
use proptest::strategy::BoxedStrategy;
use proptest_derive::Arbitrary;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "physical_source")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: SourceId,
    // Nullable for query-owned (inline/internal) sources, which have
    // no shared logical binding.
    pub logical_source: Option<String>,
    pub host_addr: NetworkAddr,
    pub source_type: SourceType,
    #[sea_orm(column_type = "Json")]
    pub source_config: Json,
    #[sea_orm(column_type = "Json")]
    pub parser_config: Json,
    pub kind: ConnectorKind,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "crate::source::logical::Entity",
        from = "Column::LogicalSource",
        to = "crate::source::logical::Column::Name",
        on_update = "Restrict",
        on_delete = "Restrict"
    )]
    LogicalSource,
    #[sea_orm(
        belongs_to = "crate::worker::Entity",
        from = "Column::HostAddr",
        to = "crate::worker::Column::HostAddr",
        on_update = "Restrict",
        on_delete = "Restrict"
    )]
    Worker,
}

impl Related<logical::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::LogicalSource.def()
    }
}

impl Related<worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(
    Arbitrary,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Display,
    EnumIter,
    DeriveActiveEnum,
    Serialize,
    Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum SourceType {
    File,
    Tcp,
    Generator,
    Network,
}

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::source::logical::DropLogicalSource;
    use crate::source::physical::{
        CreatePhysicalSource, DropPhysicalSource, GetPhysicalSource, InlineSourceWithRefs,
        PhysicalSourceWithRefs, SourceType,
    };
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        req.logical.execute(&db).await.unwrap();
        req.worker.execute(&db).await.unwrap();
        req.physical.execute(&db).await.unwrap();

        let sources = GetPhysicalSource::all()
            .with_logical_source(req.logical.name.clone())
            .execute(&db)
            .await
            .unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].logical_source, Some(req.logical.name));
        assert_eq!(sources[0].host_addr, req.worker.host_addr);
        assert_eq!(sources[0].source_type, req.physical.source_type);
    }

    #[proptest(async = "tokio")]
    async fn refs_must_exist(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        // neither logical source nor worker exist
        assert!(req.physical.execute(&db).await.is_err());

        // only logical source exists
        req.logical.execute(&db).await.unwrap();
        assert!(req.physical.execute(&db).await.is_err());

        // both exist
        req.worker.execute(&db).await.unwrap();
        req.physical.execute(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn drop_blocked_by_physical(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        req.logical.execute(&db).await.unwrap();
        req.worker.execute(&db).await.unwrap();
        req.physical.execute(&db).await.unwrap();

        assert!(
            DropLogicalSource::all()
                .with_name(req.logical.name)
                .execute(&db)
                .await
                .is_err()
        );
    }

    #[proptest(async = "tokio")]
    async fn drop_by_id(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        req.logical.execute(&db).await.unwrap();
        req.worker.execute(&db).await.unwrap();
        let created = req.physical.execute(&db).await.unwrap();

        let dropped = DropPhysicalSource::all()
            .with_id(created.id)
            .execute(&db)
            .await
            .unwrap();
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].id, created.id);

        let remaining = GetPhysicalSource::all()
            .with_id(created.id)
            .execute(&db)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn drop_by_logical_source(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        req.logical.execute(&db).await.unwrap();
        req.worker.execute(&db).await.unwrap();
        req.physical.execute(&db).await.unwrap();

        let dropped = DropPhysicalSource::all()
            .with_logical_source(req.logical.name.clone())
            .execute(&db)
            .await
            .unwrap();
        assert_eq!(dropped.len(), 1);

        let remaining = GetPhysicalSource::all()
            .with_logical_source(req.logical.name)
            .execute(&db)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn drop_no_match_noop(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        req.logical.execute(&db).await.unwrap();
        req.worker.execute(&db).await.unwrap();
        req.physical.execute(&db).await.unwrap();

        let dropped = DropPhysicalSource::all()
            .with_id(999999.into())
            .execute(&db)
            .await
            .unwrap();
        assert!(dropped.is_empty());

        let remaining = GetPhysicalSource::all()
            .with_host_addr(req.worker.host_addr)
            .execute(&db)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        req.logical.execute(&db).await.unwrap();
        req.worker.execute(&db).await.unwrap();
        let created = req.physical.execute(&db).await.unwrap();

        DropPhysicalSource::all()
            .with_id(created.id)
            .execute(&db)
            .await
            .unwrap();
        req.physical.execute(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn if_not_exists_returns_existing(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        req.logical.execute(&db).await.unwrap();
        req.worker.execute(&db).await.unwrap();
        let original = req.physical.execute(&db).await.unwrap();

        let retry = CreatePhysicalSource {
            logical_source: req.physical.logical_source.clone(),
            host_addr: req.physical.host_addr.clone(),
            source_type: req.physical.source_type,
            source_config: serde_json::json!({"different": "config"}),
            parser_config: serde_json::json!({"different": "parser"}),
            if_not_exists: true,
        };
        let returned = retry.execute(&db).await.unwrap();
        assert_eq!(returned, original);

        let all = GetPhysicalSource::all().execute(&db).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[proptest(async = "tokio")]
    async fn if_not_exists_creates_new_source_type(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        req.logical.execute(&db).await.unwrap();
        req.worker.execute(&db).await.unwrap();
        let original = req.physical.execute(&db).await.unwrap();

        let new_source_type = match req.physical.source_type {
            SourceType::File => SourceType::Tcp,
            _ => SourceType::File,
        };
        let retry = CreatePhysicalSource {
            logical_source: req.physical.logical_source.clone(),
            host_addr: req.physical.host_addr.clone(),
            source_type: new_source_type,
            source_config: Default::default(),
            parser_config: Default::default(),
            if_not_exists: true,
        };
        let new = retry.execute(&db).await.unwrap();
        assert_ne!(new.id, original.id);
        assert_eq!(new.source_type, new_source_type);

        let all = GetPhysicalSource::all().execute(&db).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[proptest(async = "tokio")]
    async fn identical_inline_sources(req1: InlineSourceWithRefs, req2: InlineSourceWithRefs) {
        let db = Database::for_test().await;
        req1.worker.execute(&db).await.unwrap();
        req2.worker.execute(&db).await.unwrap();

        let first = req1.physical.execute(&db).await.unwrap();
        let second = req2.physical.execute(&db).await.unwrap();

        assert_ne!(first.id, second.id);
        assert_eq!(first.logical_source, None);
        assert_eq!(second.logical_source, None);
    }

    #[proptest(async = "tokio")]
    async fn drop_blocked_by_active_query(mut req: crate::query::CreateQueryWithRefs) {
        use crate::query::{fragment::FragmentState, setup, walk_all};
        use crate::source::physical;
        use sea_orm::EntityTrait;

        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let source_id = req.query.source_ids[0];

        assert!(
            physical::Entity::delete_by_id(source_id)
                .exec(&db)
                .await
                .is_err()
        );

        walk_all(&fragments, FragmentState::Completed, &db).await;

        physical::Entity::delete_by_id(source_id)
            .exec(&db)
            .await
            .unwrap();
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct PhysicalSourceWithRefs {
    pub(crate) logical: CreateLogicalSource,
    pub(crate) worker: CreateWorker,
    pub(crate) physical: CreatePhysicalSource,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct InlineSourceWithRefs {
    pub(crate) worker: CreateWorker,
    pub(crate) physical: CreateInlineSource,
}

#[cfg(test)]
impl Arbitrary for PhysicalSourceWithRefs {
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        (
            any::<CreateLogicalSource>(),
            any::<CreateWorker>(),
            any::<SourceType>(),
        )
            .prop_map(|(logical, worker, source_type)| {
                let physical = CreatePhysicalSource {
                    logical_source: logical.name.clone(),
                    host_addr: worker.host_addr.clone(),
                    source_type,
                    source_config: Default::default(),
                    parser_config: Default::default(),
                    if_not_exists: false,
                };
                Self {
                    logical,
                    worker,
                    physical,
                }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

#[cfg(test)]
impl Arbitrary for InlineSourceWithRefs {
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        (any::<CreateWorker>(), any::<SourceType>(), any::<bool>())
            .prop_map(|(worker, source_type, internal)| {
                let physical = CreateInlineSource {
                    source_type,
                    source_config: Default::default(),
                    parser_config: Default::default(),
                    host_addr: worker.host_addr.clone(),
                    internal,
                };
                Self { worker, physical }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
