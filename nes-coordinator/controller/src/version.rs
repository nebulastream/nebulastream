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

//! Reads the build each worker runs.
//!
//! Nothing writes a version down, so answering means asking the workers. This sits in the
//! controller crate because reaching a worker is what the crate knows how to do, and it is offered
//! as a plain call rather than through the reconciliation loop, which is about fragments.

use crate::config::VERSION_TIMEOUT;
use crate::embedded::WorkerFactory;
use crate::remote::WorkerRpcServiceClient;
use futures::future::join_all;
use model::worker;
use model::worker::WorkerVersion;
use model::worker::endpoint::NetworkAddr;
use std::sync::Arc;
use tonic::transport::Endpoint;
use tracing::debug;

/// Asks every given worker which build it runs, all at once.
///
/// A factory means the workers run in this process, and every one of them is therefore the build
/// this binary was linked with. Without one they are processes of their own and each is asked over
/// the network; one that does not answer reports why rather than failing the others.
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

    join_all(
        workers
            .into_iter()
            .map(|worker| async move { ask(worker.host_addr).await }),
    )
    .await
}

/// Reaches one worker and reads its version, reporting what went wrong instead when it cannot.
async fn ask(addr: NetworkAddr) -> WorkerVersion {
    match request_version(&addr).await {
        Ok(version) => WorkerVersion::reported(addr, version),
        Err(err) => {
            debug!("could not read the version of worker {addr}: {err}");
            WorkerVersion::unreachable(addr, err.to_string())
        }
    }
}

async fn request_version(addr: &NetworkAddr) -> anyhow::Result<String> {
    // One deadline over dialling and asking together, so a worker that accepts the connection and
    // then never answers is reported just as promptly as one that refuses it.
    let ask = async {
        let channel = Endpoint::from_shared(format!("http://{addr}"))?
            .connect_timeout(VERSION_TIMEOUT)
            .connect()
            .await?;
        let response = WorkerRpcServiceClient::new(channel)
            .request_version(())
            .await?;
        anyhow::Ok(response.into_inner().version)
    };
    tokio::time::timeout(VERSION_TIMEOUT, ask)
        .await
        .map_err(|_| anyhow::anyhow!("timed out after {VERSION_TIMEOUT:?}"))?
}
