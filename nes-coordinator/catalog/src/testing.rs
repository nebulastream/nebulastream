use crate::Catalog;
use model::query;
use model::query::fragment::{self, CreateFragment, FragmentState};
use model::query::query_state::QueryState;
use model::query::GetQuery;
use model::query::fragment::ValidFragments;
use sea_orm::ActiveValue::Set;
use std::future::Future;
use std::sync::Arc;

pub fn test_prop<F, Fut>(f: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime")
        .block_on(f());
}

pub async fn advance_all_fragments(
    catalog: &Catalog,
    query_id: i64,
    target: FragmentState,
) -> query::Model {
    let frags = catalog.query.get_fragments(query_id).await.unwrap();
    let updates: Vec<_> = frags
        .iter()
        .map(|f| {
            let mut am: fragment::ActiveModel = f.clone().into();
            am.current_state = Set(target);
            am
        })
        .collect();
    let (query, _) = catalog
        .query
        .update_fragment_states(query_id, updates)
        .await
        .unwrap();
    query
}

/// Walk a query through a state path by manipulating fragments.
/// The first element of `path` must be `Pending`. Fragments are created
/// from `setup` before the first transition (they exist while the query is still Pending).
pub async fn walk_query_via_fragments(
    catalog: &Catalog,
    created: &query::Model,
    setup: &ValidFragments,
    path: &[QueryState],
) -> Vec<query::Model> {
    assert_eq!(path.first(), Some(&QueryState::Pending));
    let reqs = setup.create_fragments(created.id);
    let (updated, _) = catalog
        .query
        .create_fragments(created, reqs)
        .await
        .expect("create_fragments should succeed");
    let mut models = vec![updated];

    for &target in &path[1..] {
        let current = models.last().unwrap();
        let updated = match target {
            QueryState::Registered => {
                advance_all_fragments(catalog, current.id, FragmentState::Registered).await
            }
            QueryState::Running => {
                advance_all_fragments(catalog, current.id, FragmentState::Started).await;
                advance_all_fragments(catalog, current.id, FragmentState::Running).await
            }
            QueryState::Completed => {
                advance_all_fragments(catalog, current.id, FragmentState::Completed).await
            }
            QueryState::Stopped => {
                let frags = catalog.query.get_fragments(current.id).await.unwrap();
                if frags.is_empty() {
                    catalog
                        .query
                        .stop_pending_query(current)
                        .await
                        .expect("stop_query should succeed")
                } else {
                    advance_all_fragments(catalog, current.id, FragmentState::Stopped).await
                }
            }
            QueryState::Failed => {
                let frags = catalog.query.get_fragments(current.id).await.unwrap();
                if frags.is_empty() {
                    catalog
                        .query
                        .fail_pending_query(current.clone(), "test failure".to_string())
                        .await
                        .expect("fail_query should succeed")
                } else {
                    advance_all_fragments(catalog, current.id, FragmentState::Failed).await
                }
            }
            QueryState::Pending => unreachable!("Cannot transition to Pending"),
        };
        models.push(updated);
    }
    models
}

pub async fn advance_query_to(catalog: &Arc<Catalog>, query_id: i64, target: QueryState) {
    let query = catalog
        .query
        .get_query(GetQuery::all().with_id(query_id))
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    match target {
        QueryState::Pending => {
            let workers = catalog.worker.get_worker(Default::default()).await.unwrap();
            let w = workers.first().expect("need at least one worker");
            catalog
                .query
                .create_fragments(
                    &query,
                    vec![CreateFragment {
                        query_id,
                        host_addr: w.host_addr.clone(),
                        grpc_addr: w.grpc_addr.clone(),
                        plan: serde_json::json!({}),
                        used_capacity: 0,
                        has_source: false,
                    }],
                )
                .await
                .unwrap();
        }
        QueryState::Registered => {
            advance_all_fragments(catalog, query_id, FragmentState::Registered).await;
        }
        QueryState::Running => {
            advance_all_fragments(catalog, query_id, FragmentState::Started).await;
            advance_all_fragments(catalog, query_id, FragmentState::Running).await;
        }
        QueryState::Completed => {
            advance_all_fragments(catalog, query_id, FragmentState::Completed).await;
        }
        QueryState::Stopped => {
            let frags = catalog.query.get_fragments(query_id).await.unwrap();
            if frags.is_empty() {
                catalog.query.stop_pending_query(&query).await.unwrap();
            } else {
                advance_all_fragments(catalog, query_id, FragmentState::Stopped).await;
            }
        }
        QueryState::Failed => {
            let frags = catalog.query.get_fragments(query_id).await.unwrap();
            if frags.is_empty() {
                catalog
                    .query
                    .fail_pending_query(query, "test failure".to_string())
                    .await
                    .unwrap();
            } else {
                advance_all_fragments(catalog, query_id, FragmentState::Failed).await;
            }
        }
    }
}
