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

use crate::worker::worker_task::{Rpc, WorkerTaskError};
use crate::worker::worker_registry::{WorkerError, WorkerRegistryHandle};
use catalog::Catalog;
use catalog::error::Retryable;
use model::query::fragment;
use model::worker::endpoint::NetworkAddr;
use model::worker::{DesiredWorkerState, GetWorker};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

const MAX_RPC_ATTEMPTS: usize = 5;
const BACKOFF_FACTOR: u64 = 2;
const ROLLBACK_RETRY_MAX: Duration = Duration::from_secs(30);
const EXECUTE_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) enum RetryPolicy {
    Transition,
    Rollback { catalog: Arc<Catalog> },
}

impl RetryPolicy {
    fn strategy(&self) -> Box<dyn Iterator<Item = Duration> + Send> {
        match self {
            Self::Transition => Box::new(
                ExponentialBackoff::from_millis(50)
                    .factor(BACKOFF_FACTOR)
                    .map(jitter)
                    .take(MAX_RPC_ATTEMPTS - 1),
            ),
            Self::Rollback { .. } => Box::new(
                ExponentialBackoff::from_millis(50)
                    .factor(BACKOFF_FACTOR)
                    .max_delay(ROLLBACK_RETRY_MAX)
                    .map(jitter),
            ),
        }
    }

    async fn action<F, Rsp>(
        &self,
        registry: &WorkerRegistryHandle,
        mk_rpc: &F,
        fragment: &fragment::Model,
    ) -> Result<Rsp, WorkerError>
    where
        F: Fn(&fragment::Model) -> (oneshot::Receiver<Result<Rsp, WorkerTaskError>>, Rpc),
        Rsp: Send + 'static,
    {
        let (rx, rpc) = mk_rpc(fragment);
        let addr = fragment.host_addr.clone();
        registry.send(&addr, rpc).await?;
        let result = rx
            .await
            .map_err(|_| WorkerError::ClientUnavailable(addr.clone()))?
            .map_err(WorkerError::from);

        match result {
            Err(e) if e.retryable() => {
                if let Self::Rollback { catalog } = self
                    && is_worker_removed(catalog, &fragment.host_addr).await
                {
                    Err(WorkerError::WorkerRemoved(fragment.host_addr.clone()))
                } else {
                    Err(e)
                }
            }
            other => other,
        }
    }

    pub(crate) async fn execute<F, Rsp>(
        &self,
        registry: &WorkerRegistryHandle,
        mk_rpc: &F,
        fragment: &fragment::Model,
    ) -> Result<Rsp, WorkerError>
    where
        F: Fn(&fragment::Model) -> (oneshot::Receiver<Result<Rsp, WorkerTaskError>>, Rpc),
        Rsp: Send + 'static,
    {
        let addr = fragment.host_addr.clone();
        let retry_fut = RetryIf::spawn(
            self.strategy(),
            || self.action(registry, mk_rpc, fragment),
            WorkerError::retryable,
        );

        match tokio::time::timeout(EXECUTE_TIMEOUT, retry_fut).await {
            Ok(result) => result,
            Err(_) => Err(WorkerError::ClientUnavailable(addr)),
        }
    }
}

async fn is_worker_removed(catalog: &Catalog, host_addr: &NetworkAddr) -> bool {
    match catalog
        .worker
        .get_worker(GetWorker::all().with_host_addr(host_addr.clone()))
        .await
    {
        Ok(workers) => workers
            .first()
            .is_none_or(|w| w.desired_state == DesiredWorkerState::Removed),
        Err(_) => false,
    }
}
