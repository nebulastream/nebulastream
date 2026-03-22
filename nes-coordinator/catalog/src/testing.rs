use crate::Catalog;
use model::query;
use model::query::fragment::{CreateFragment, FragmentState, ValidFragments};
use model::query::query_state::QueryState;
use std::sync::Arc;

pub async fn advance_all_fragments(
    catalog: &Catalog,
    query_id: i64,
    target: FragmentState,
) -> query::Model {
    let frags = catalog.query.get_fragments(query_id).await.unwrap();
    let updates: Vec<_> = frags.iter().map(|f| f.with_state(target)).collect();
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
    catalog
        .query
        .create_fragments_for_query(reqs)
        .await
        .expect("create_fragments should succeed");
    let mut models = vec![created.clone()];

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
                advance_all_fragments(catalog, current.id, FragmentState::Stopped).await
            }
            QueryState::Failed => {
                advance_all_fragments(catalog, current.id, FragmentState::Failed).await
            }
            QueryState::Pending => unreachable!("Cannot transition to Pending"),
        };
        models.push(updated);
    }
    models
}

pub async fn advance_query_to(catalog: &Arc<Catalog>, query_id: i64, target: QueryState) {
    match target {
        QueryState::Pending => {
            let workers = catalog.worker.get_worker(Default::default()).await.unwrap();
            let w = workers.first().expect("need at least one worker");
            catalog
                .query
                .create_fragments_for_query(
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
            advance_all_fragments(catalog, query_id, FragmentState::Stopped).await;
        }
        QueryState::Failed => {
            advance_all_fragments(catalog, query_id, FragmentState::Failed).await;
        }
    }
}
