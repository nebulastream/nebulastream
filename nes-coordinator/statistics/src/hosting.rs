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

use crate::grpc::{ControlService, ReportService};
use crate::proto;
use crate::service::{ProbeImpulse, QuerySubmitter, SchemaResolver, StatisticService};
use model::identifier::QueryId;
use model::query::{DropQuery, GetQuery};
use model::request::{Payload, Request};
use model::statement::{Statement, StatementResult};
use std::sync::Arc;
use std::time::Duration;

const SUBMIT_TIMEOUT: Duration = Duration::from_secs(30);
const BIND_ATTEMPTS: usize = 5;

pub struct ChannelSubmitter {
    sender: async_channel::Sender<Request>,
}

impl ChannelSubmitter {
    pub fn new(sender: async_channel::Sender<Request>) -> Self {
        Self { sender }
    }
}

#[async_trait::async_trait]
impl QuerySubmitter for ChannelSubmitter {
    async fn submit(&self, sql: String) -> Result<u64, String> {
        let (reply, request) = Request::from(Payload::sql(sql).until_running(Some(SUBMIT_TIMEOUT)));
        self.sender
            .send(request)
            .await
            .map_err(|error| format!("the coordinator is no longer accepting requests: {error}"))?;
        match reply.await {
            Ok(Ok(StatementResult::CreatedQuery(query, _))) => Ok(*query.id as u64),
            Ok(Ok(other)) => Err(format!(
                "a statistic query answered with {other:?} instead of a query"
            )),
            Ok(Err(error)) => Err(error.to_string()),
            Err(error) => Err(format!(
                "the coordinator dropped the statistic request: {error}"
            )),
        }
    }

    async fn stop(&self, query_id: u64) -> Result<(), String> {
        let filters = GetQuery::all().with_id(QueryId::new(query_id as i64));
        let statement = Statement::DropQuery(DropQuery::all().with_filters(filters));
        let (reply, request) =
            Request::from(Payload::parsed(statement).until_terminated(Some(SUBMIT_TIMEOUT)));
        self.sender
            .send(request)
            .await
            .map_err(|error| format!("the coordinator is no longer accepting requests: {error}"))?;
        match reply.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(error.to_string()),
            Err(error) => Err(format!(
                "the coordinator dropped the statistic request: {error}"
            )),
        }
    }
}

pub struct GrpcImpulse;

#[async_trait::async_trait]
impl ProbeImpulse for GrpcImpulse {
    async fn request(
        &self,
        port: u16,
        statistic_id: u64,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<(), String> {
        let endpoint = format!("http://127.0.0.1:{port}");
        let mut client =
            proto::statistic_source_service_client::StatisticSourceServiceClient::connect(endpoint)
                .await
                .map_err(|error| error.to_string())?;
        client
            .request_statistic(proto::StatisticRequest {
                statistic_id,
                start_ts,
                end_ts,
            })
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

pub struct UnresolvedSchemas;

#[async_trait::async_trait]
impl SchemaResolver for UnresolvedSchemas {
    async fn field_type(&self, _logical_source: &str, _field: &str) -> Option<String> {
        None
    }
}

pub fn pick_port(requested: u16) -> std::io::Result<u16> {
    if requested != 0 {
        return Ok(requested);
    }
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

pub async fn run(sender: async_channel::Sender<Request>, requested_port: u16) {
    for attempt in 1..=BIND_ATTEMPTS {
        let port = match pick_port(requested_port) {
            Ok(port) => port,
            Err(error) => {
                tracing::error!(%error, "statistic service could not reserve a port");
                return;
            }
        };
        match serve(sender.clone(), port).await {
            Ok(()) => return,
            Err(error) if requested_port == 0 && attempt < BIND_ATTEMPTS => {
                tracing::warn!(%error, port, attempt, "statistic service lost its port, retrying");
            }
            Err(error) => {
                tracing::error!(%error, port, "statistic service stopped");
                return;
            }
        }
    }
}

async fn serve(sender: async_channel::Sender<Request>, port: u16) -> Result<(), String> {
    let address = format!("127.0.0.1:{port}")
        .parse()
        .map_err(|error| format!("statistic service address is not parseable: {error}"))?;

    let service = Arc::new(StatisticService::new(
        Arc::new(ChannelSubmitter::new(sender)),
        Arc::new(UnresolvedSchemas),
        Arc::new(GrpcImpulse),
        format!("localhost:{port}"),
    ));

    tracing::info!(port, "statistic service listening");
    tonic::transport::Server::builder()
        .add_service(
            proto::statistic_control_service_server::StatisticControlServiceServer::new(
                ControlService::new(service.clone()),
            ),
        )
        .add_service(
            proto::statistic_report_service_server::StatisticReportServiceServer::new(
                ReportService::new(service),
            ),
        )
        .serve(address)
        .await
        .map_err(|error| error.to_string())
}
