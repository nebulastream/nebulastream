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

use crate::worker::worker_task::Rpc;
use std::time::Duration;
use tokio::select;
use tracing::{debug, warn};

/// Abstraction over how RPCs are delivered to a worker.
/// `GrpcTransport` sends them over gRPC; `EmbeddedTransport` calls the
/// in-process worker directly.
pub(crate) trait WorkerTransport: Send + 'static {
    fn dispatch(&self, rpc: Rpc);
    async fn health_check(&self) -> bool;
}

/// Shared select loop: receive RPCs from the flume channel and dispatch them
/// via the transport, periodically running a health check. Returns when the
/// channel closes or a health check fails.
pub(crate) async fn serve(
    transport: &impl WorkerTransport,
    receiver: &flume::Receiver<Rpc>,
    heartbeat_interval: Duration,
) {
    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    loop {
        select! {
            rpc = receiver.recv_async() => {
                match rpc {
                    Ok(rpc) => transport.dispatch(rpc),
                    Err(_) => {
                        debug!("RPC channel closed");
                        break;
                    }
                }
            }
            _ = heartbeat.tick() => {
                if !transport.health_check().await {
                    warn!("health check failed");
                    break;
                }
            }
        }
    }
}
