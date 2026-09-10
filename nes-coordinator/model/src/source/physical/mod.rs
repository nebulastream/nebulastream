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

//! The physical-source entity and the requests that manage it.

pub(crate) mod create;
mod drop;
pub(crate) mod get;

pub use create::{CreateAnonymousSource, CreatePhysicalSource};
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
use proptest::strategy::Strategy;
use sea_orm::entity::prelude::*;
use serde::Serialize;

/// A concrete source placed on a worker and bound to a logical source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "physical_source")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: SourceId,
    /// Nullable for query-owned (anonymous/internal) sources, which have no shared logical binding.
    pub logical_source: Option<String>,
    pub host_addr: NetworkAddr,
    /// Free-form connector type owned by the C++ side (for example `FILE` or `NETWORK`);
    /// the coordinator only stores and returns it.
    pub source_type: String,
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

#[cfg(test)]
mod tests {
    use crate::ConnectorKind;
    use crate::Execute;
    use crate::database::Database;
    use crate::error::{ErrorCode, code_of};
    use crate::source::logical::{CreateLogicalSource, DropLogicalSource};
    use crate::source::physical::{
        CreateAnonymousSource, CreatePhysicalSource, DropPhysicalSource, GetPhysicalSource,
        PhysicalSourceWithRefs,
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
        let missing = code_of(req.physical.execute(&db).await);
        assert_eq!(missing, Some(ErrorCode::UnknownSourceName));

        // only logical source exists
        req.logical.execute(&db).await.unwrap();
        let missing = code_of(req.physical.execute(&db).await);
        assert_eq!(missing, Some(ErrorCode::UnknownSourceName));

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

        let drop = DropLogicalSource::all().with_name(req.logical.name);
        let blocked = code_of(drop.execute(&db).await);
        assert_eq!(blocked, Some(ErrorCode::CatalogWriteRejected));
    }

    #[proptest(async = "tokio")]
    async fn drop_removes_only_the_matching_source(req: PhysicalSourceWithRefs, by_logical: bool) {
        let db = Database::for_test().await;
        req.logical.execute(&db).await.unwrap();
        req.worker.execute(&db).await.unwrap();
        let created = req.physical.execute(&db).await.unwrap();

        let unmatched = DropPhysicalSource::all().with_id(999999.into());
        assert!(unmatched.execute(&db).await.unwrap().is_empty());

        let matching = if by_logical {
            DropPhysicalSource::all().with_logical_source(req.logical.name)
        } else {
            DropPhysicalSource::all().with_id(created.id)
        };
        let dropped = matching.execute(&db).await.unwrap();
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].id, created.id);
        let remaining = GetPhysicalSource::all().execute(&db).await.unwrap();
        assert!(remaining.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn if_not_exists_matches_on_the_full_definition(req: PhysicalSourceWithRefs) {
        let db = Database::for_test().await;
        req.logical.execute(&db).await.unwrap();
        req.worker.execute(&db).await.unwrap();
        let original = req.physical.execute(&db).await.unwrap();

        // Same full definition (including config) is an idempotent no-op.
        let retry = CreatePhysicalSource {
            if_not_exists: true,
            ..req.physical
        };
        assert_eq!(retry.execute(&db).await.unwrap(), original);

        // Same (logical, host, type) but a different config is a distinct source
        // (for example two FILE sources that read different paths).
        let variant = serde_json::json!({ "variant": 1 });
        let changed = [
            CreatePhysicalSource {
                source_config: variant.clone(),
                ..retry.clone()
            },
            CreatePhysicalSource {
                parser_config: variant,
                ..retry.clone()
            },
            CreatePhysicalSource {
                source_type: format!("{}X", retry.source_type),
                ..retry.clone()
            },
        ];
        for (count, request) in changed.into_iter().enumerate() {
            assert_ne!(request.execute(&db).await.unwrap().id, original.id);
            let all = GetPhysicalSource::all().execute(&db).await.unwrap();
            assert_eq!(all.len(), count + 2);
        }
    }

    #[proptest(async = "tokio")]
    async fn filters_select_one_of_several(req: PhysicalSourceWithRefs, internal: bool) {
        let db = &Database::for_test().await;
        let other_logical = CreateLogicalSource {
            name: format!("{}_", req.logical.name),
            ..req.logical.clone()
        };
        req.logical.execute(db).await.unwrap();
        other_logical.execute(db).await.unwrap();
        req.worker.execute(db).await.unwrap();
        let first = req.physical.execute(db).await.unwrap();
        let second = CreatePhysicalSource {
            logical_source: other_logical.name,
            ..req.physical.clone()
        };
        let second = second.execute(db).await.unwrap();
        let anonymous = CreateAnonymousSource {
            source_type: req.physical.source_type,
            source_config: req.physical.source_config,
            parser_config: req.physical.parser_config,
            host_addr: req.physical.host_addr,
            internal,
        };
        let anonymous = anonymous.execute(db).await.unwrap();
        assert_eq!(anonymous.kind == ConnectorKind::Internal, internal);

        let get = |request: GetPhysicalSource| async move { request.execute(db).await.unwrap() };
        let all = GetPhysicalSource::all;
        let by_logical = all().with_logical_source(req.logical.name.clone());
        assert_eq!(get(by_logical).await, [first.clone()]);
        assert_eq!(get(all().with_id(second.id)).await, [second.clone()]);
        assert_eq!(
            get(all().with_kind(anonymous.kind)).await,
            [anonymous.clone()]
        );
        assert_eq!(
            get(all().with_host_addr(req.worker.host_addr)).await.len(),
            3
        );

        let by_logical = DropPhysicalSource::all().with_logical_source(req.logical.name);
        assert_eq!(by_logical.execute(db).await.unwrap(), [first]);
        assert_eq!(
            DropPhysicalSource::all().execute(db).await.unwrap(),
            [second]
        );
        assert_eq!(get(all()).await, [anonymous]);
    }

    #[proptest(async = "tokio")]
    async fn identical_anonymous_sources(req: PhysicalSourceWithRefs, internal: bool) {
        let db = Database::for_test().await;
        req.worker.execute(&db).await.unwrap();
        let anonymous = CreateAnonymousSource {
            source_type: req.physical.source_type,
            source_config: req.physical.source_config,
            parser_config: req.physical.parser_config,
            host_addr: req.physical.host_addr,
            internal,
        };

        let first = anonymous.execute(&db).await.unwrap();
        let second = anonymous.execute(&db).await.unwrap();

        assert_ne!(first.id, second.id);
        assert_eq!((first.logical_source, second.logical_source), (None, None));
    }
}

#[cfg(test)]
#[derive(Debug, Clone, test_strategy::Arbitrary)]
pub(crate) struct PhysicalSourceWithRefs {
    pub(crate) logical: CreateLogicalSource,
    pub(crate) worker: CreateWorker,
    #[strategy("[A-Za-z]{1,10}".prop_map(move |source_type| CreatePhysicalSource {
        logical_source: #logical.name.clone(),
        host_addr: #worker.host_addr.clone(),
        source_type,
        source_config: Default::default(),
        parser_config: Default::default(),
        if_not_exists: false,
    }))]
    pub(crate) physical: CreatePhysicalSource,
}
