use crate::query::retry::RetryPolicy;
use crate::worker::worker_task::{
    GetFragmentStatusRequest, QueryStatusReply, Rpc, StopFragmentRequest, WorkerClientErr,
};
use crate::worker::worker_registry::{WorkerError, WorkerRegistryHandle};
use anyhow::{Result, anyhow};
use common::error::Retryable;
use catalog::Catalog;
use futures_util::future;
use model::Set;
use model::query;
use model::query::StopMode;
use model::query::fragment::{self, FragmentError, FragmentId, FragmentState};
use model::query::query_state::QueryState;
use std::sync::Arc;
use tokio::sync::oneshot;

fn transition_update(
    fragment: &fragment::Model,
    result: Result<(), WorkerError>,
    target: FragmentState,
) -> Option<fragment::ActiveModel> {
    if fragment.current_state.is_terminal() {
        return None;
    }
    let mut am: fragment::ActiveModel = fragment.clone().into();
    match result {
        Ok(_) => am.current_state = Set(target),
        Err(e) if e.retryable() => return None,
        Err(e) if e.fragment_not_found() => am.current_state = Set(FragmentState::Pending),
        Err(e) => {
            am.current_state = Set(FragmentState::Failed);
            am.error = Set(Some(FragmentError::from(e)));
        }
    }
    Some(am)
}

pub(crate) struct QueryContext {
    pub(crate) query: query::Model,
    pub(crate) fragments: Vec<fragment::Model>,
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) worker_registry: WorkerRegistryHandle,
}

impl QueryContext {
    async fn multicast<F, Rsp>(
        &self,
        mk_rpc: F,
        retry: &RetryPolicy,
    ) -> Vec<Result<Rsp, WorkerError>>
    where
        F: Fn(FragmentId) -> (oneshot::Receiver<Result<Rsp, WorkerClientErr>>, Rpc),
        Rsp: Send + 'static,
    {
        let mk_rpc = &mk_rpc;
        let futures = self
            .fragments
            .iter()
            .map(|fragment| retry.execute(&self.worker_registry, mk_rpc, fragment));
        future::join_all(futures).await
    }


    pub(crate) async fn transition_fragments<F, Rsp>(
        &mut self,
        required_state: FragmentState,
        mk_rpc: F,
        target: FragmentState,
    ) -> Result<()>
    where
        F: Fn(FragmentId) -> (oneshot::Receiver<Result<Rsp, WorkerClientErr>>, Rpc),
        Rsp: Send + 'static,
    {
        let mk_rpc = &mk_rpc;
        let retry = RetryPolicy::Transition;
        let eligible: Vec<&fragment::Model> = self
            .fragments
            .iter()
            .filter(|f| f.current_state == required_state)
            .collect();

        let results = future::join_all(
            eligible
                .iter()
                .map(|f| retry.execute(&self.worker_registry, mk_rpc, f)),
        )
        .await;

        let mut has_transient_failures = false;
        let updates: Vec<_> = eligible
            .iter()
            .zip(results)
            .inspect(|(_, r)| has_transient_failures |= matches!(r, Err(e) if e.retryable()))
            .filter_map(|(f, r)| transition_update(f, r.map(|_| ()), target))
            .collect();

        (self.query, self.fragments) = self
            .catalog
            .query
            .update_fragment_states(self.query.id, updates)
            .await?;

        anyhow::ensure!(!has_transient_failures && self.query.current_state != QueryState::Failed,
            "fragment transition failed");
        Ok(())
    }

    fn rollback_retry(&self) -> RetryPolicy {
        RetryPolicy::Rollback {
            catalog: self.catalog.clone(),
        }
    }

    pub(crate) async fn stop_fragments(&self, stop_mode: StopMode) -> Vec<Result<(), WorkerError>> {
        self.multicast(
            |id| {
                let (rx, req) = StopFragmentRequest::new((id, stop_mode));
                (rx, Rpc::StopFragment(req))
            },
            &self.rollback_retry(),
        )
        .await
    }

    pub(crate) async fn poll_fragment_status(&self) -> Vec<Result<QueryStatusReply, WorkerError>> {
        self.multicast(
            |id| {
                let (rx, req) = GetFragmentStatusRequest::new(id);
                (rx, Rpc::GetFragmentStatus(req))
            },
            &RetryPolicy::Transition,
        )
        .await
    }

    pub(crate) async fn apply_rpc_results<Rsp, F>(
        &mut self,
        results: Vec<Result<Rsp, WorkerError>>,
        apply: F,
    ) -> Result<()>
    where
        F: Fn(&fragment::Model, Result<Rsp, WorkerError>) -> Option<fragment::ActiveModel>,
    {
        let has_transient_failures = results.iter().any(|r| match r {
            Err(e) => e.retryable(),
            Ok(_) => false,
        });
        let updates: Vec<_> = self
            .fragments
            .iter()
            .zip(results)
            .filter_map(|(fragment, result)| apply(fragment, result))
            .collect();

        let (query, fragments) = self
            .catalog
            .query
            .update_fragment_states(self.query.id, updates)
            .await?;
        self.query = query;
        self.fragments = fragments;
        if self.query.current_state == QueryState::Failed {
            return Err(anyhow!("query failed"));
        }
        if has_transient_failures {
            return Err(anyhow!("transient fragment failures"));
        }
        Ok(())
    }

    pub(crate) async fn apply_transition_results(
        &mut self,
        results: Vec<Result<(), WorkerError>>,
        target: FragmentState,
    ) -> Result<()> {
        self.apply_rpc_results(results, |f, r| transition_update(f, r, target))
            .await
    }

    pub(crate) async fn stop_source_fragments(&mut self) {
        let source_fragments: Vec<_> = self.fragments.iter().filter(|f| f.has_source).collect();
        if source_fragments.is_empty() {
            self.rollback_fragments(StopMode::Graceful).await;
            return;
        }

        let retry = self.rollback_retry();
        let mk_rpc = |id: FragmentId| {
            let (rx, req) = StopFragmentRequest::new((id, StopMode::Graceful));
            (rx, Rpc::StopFragment(req))
        };
        let mk_rpc = &mk_rpc;
        let futures = source_fragments
            .iter()
            .map(|fragment| retry.execute(&self.worker_registry, mk_rpc, fragment));
        let results: Vec<_> = future::join_all(futures).await;

        let updates: Vec<_> = source_fragments
            .iter()
            .zip(results)
            .filter_map(|(fragment, result)| transition_update(fragment, result, FragmentState::Stopped))
            .collect();

        if let Ok((query, fragments)) = self
            .catalog
            .query
            .update_fragment_states(self.query.id, updates)
            .await
        {
            self.query = query;
            self.fragments = fragments;
        }
    }

    pub(crate) async fn rollback_fragments(&mut self, mode: StopMode) {
        let results = self.stop_fragments(mode).await;
        let _ = self.apply_transition_results(results, FragmentState::Stopped).await;
    }
}
