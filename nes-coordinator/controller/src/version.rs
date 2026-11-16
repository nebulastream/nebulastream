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

//! A worker's version is not known at registration, because clients register workers, so it is read from the worker.

use crate::config::VERSION_TIMEOUT;
use crate::in_process::WorkerFactory;
use crate::remote::WorkerRpcServiceClient;
use futures::future::join_all;
use model::worker;
use model::worker::WorkerVersion;
use model::worker::endpoint::NetworkAddr;
use std::sync::Arc;
use tonic::transport::Endpoint;
use tracing::debug;

/// The build of each given worker, read concurrently.
/// In-process workers all run the build that this binary was linked with, so none is contacted.
pub async fn worker_versions(
    workers: Vec<worker::Model>,
    embedded: Option<Arc<dyn WorkerFactory>>,
) -> Vec<WorkerVersion> {
    if let Some(factory) = embedded {
        let version = factory.version();
        return workers
            .into_iter()
            .map(|worker| WorkerVersion::reported(worker.host_addr, version.clone()))
            .collect();
    }

    join_all(workers.into_iter().map(|w| worker_version(w.host_addr))).await
}

async fn worker_version(addr: NetworkAddr) -> WorkerVersion {
    match request_version(&addr).await {
        Ok(version) => WorkerVersion::reported(addr, version),
        Err(err) => {
            debug!(%addr, error = %format_args!("{err:#}"), "could not read the worker version");
            WorkerVersion::unreachable(addr, err.to_string())
        }
    }
}

async fn request_version(addr: &NetworkAddr) -> anyhow::Result<String> {
    let dial_and_request = async {
        let channel = Endpoint::from_shared(format!("http://{addr}"))?
            .connect_timeout(VERSION_TIMEOUT)
            .connect()
            .await?;
        let response = WorkerRpcServiceClient::new(channel)
            .request_version(())
            .await?;
        anyhow::Ok(response.into_inner().version)
    };
    tokio::time::timeout(VERSION_TIMEOUT, dial_and_request)
        .await
        .map_err(|_| anyhow::anyhow!("timed out after {VERSION_TIMEOUT:?}"))?
}
