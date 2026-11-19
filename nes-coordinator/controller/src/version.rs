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

use crate::config::VERSION_TIMEOUT;
use crate::in_process::EmbeddedWorkerLauncher;
use crate::remote::WorkerRpcServiceClient;
use futures::future::join_all;
use model::worker;
use model::worker::WorkerVersion;
use model::worker::endpoint::NetworkAddr;
use std::sync::Arc;
use tonic::transport::Endpoint;
use tracing::debug;

/// Reads the build of each given worker concurrently, in the given order.
/// A worker that does not answer within the version timeout is reported unreachable.
/// In-process workers all run the build that this binary was linked with, so none is contacted.
pub async fn get_worker_versions(
    workers: Vec<worker::Model>,
    embedded: Option<Arc<dyn EmbeddedWorkerLauncher>>,
) -> Vec<WorkerVersion> {
    if let Some(launcher) = embedded {
        let version = launcher.version();
        return workers
            .into_iter()
            .map(|worker| WorkerVersion::reported(worker.host_addr, version.clone()))
            .collect();
    }

    join_all(workers.into_iter().map(|w| get_worker_version(w.host_addr))).await
}

async fn get_worker_version(addr: NetworkAddr) -> WorkerVersion {
    match request_version(&addr).await {
        Ok(version) => WorkerVersion::reported(addr, version),
        Err(err) => {
            debug!(%addr, error = %format_args!("{err:#}"), "could not read the worker version");
            WorkerVersion::unreachable(addr, err.to_string())
        }
    }
}

async fn request_version(addr: &NetworkAddr) -> anyhow::Result<String> {
    let connect_and_request = async {
        let channel = Endpoint::from_shared(format!("http://{addr}"))?
            .connect_timeout(VERSION_TIMEOUT)
            .connect()
            .await?;
        let response = WorkerRpcServiceClient::new(channel)
            .request_version(())
            .await?;
        anyhow::Ok(response.into_inner().version)
    };
    // The endpoint bounds only the connect, so this timeout also bounds the request.
    tokio::time::timeout(VERSION_TIMEOUT, connect_and_request)
        .await
        .map_err(|_| anyhow::anyhow!("timed out after {VERSION_TIMEOUT:?}"))?
}
