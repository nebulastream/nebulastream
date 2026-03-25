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

use crate::query::retry::RetryPolicy;
use crate::worker::worker_task::{
    GetFragmentStatusRequest, QueryStatusReply, Rpc, StopFragmentRequest, WorkerTaskError,
};
use crate::worker::worker_registry::{WorkerError, WorkerRegistryHandle};
use anyhow::{Result, anyhow};
use catalog::error::Retryable;
use catalog::Catalog;
use futures_util::future;
use model::Set;
use model::query;
use model::query::StopMode;
use model::query::fragment::{self, FragmentError, FragmentState};
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
        F: Fn(&fragment::Model) -> (oneshot::Receiver<Result<Rsp, WorkerTaskError>>, Rpc),
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
        F: Fn(&fragment::Model) -> (oneshot::Receiver<Result<Rsp, WorkerTaskError>>, Rpc),
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
            |f| {
                let (rx, req) = StopFragmentRequest::new((f.id, stop_mode));
                (rx, Rpc::StopFragment(req))
            },
            &self.rollback_retry(),
        )
        .await
    }

    pub(crate) async fn poll_fragment_status(&self) -> Vec<Result<QueryStatusReply, WorkerError>> {
        self.multicast(
            |f| {
                let (rx, req) = GetFragmentStatusRequest::new(f.id);
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
        let mk_rpc = |f: &fragment::Model| {
            let (rx, req) = StopFragmentRequest::new((f.id, StopMode::Graceful));
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
