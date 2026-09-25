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
use crate::service::{
    ProbeImpulse, QuerySubmitter, SchemaResolver, StatisticService, SubmitError, WorkerCatalog,
};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryId;
use model::query::{DropQuery, GetQuery};
use model::request::{Payload, Request};
use model::source::physical::GetPhysicalSource;
use model::statement::{Statement, StatementResult};
use model::worker::GetWorker;
use model::worker::endpoint::NetworkAddr;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

const SUBMIT_TIMEOUT: Duration = Duration::from_secs(30);
const BIND_ATTEMPTS: usize = 5;
pub const DEFAULT_ADVERTISED_HOST: &str = "localhost";

/// Hands a request to the coordinator and waits for its answer.
/// The outer error means the coordinator did not answer at all; the inner one is the statement's own failure.
async fn execute(
    sender: &async_channel::Sender<Request>,
    payload: Payload,
) -> Result<anyhow::Result<StatementResult>, String> {
    let (reply, request) = Request::from(payload);
    sender
        .send(request)
        .await
        .map_err(|error| format!("the coordinator is no longer accepting requests: {error}"))?;
    reply
        .await
        .map_err(|error| format!("the coordinator dropped the statistic request: {error}"))
}

fn is_placement_failure(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<CodedError>()
            .is_some_and(|coded| coded.code == ErrorCode::PlacementFailure)
    })
}

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
    async fn submit(&self, sql: String) -> Result<u64, SubmitError> {
        let payload = Payload::sql(sql).until_running(Some(SUBMIT_TIMEOUT));
        match execute(&self.sender, payload)
            .await
            .map_err(SubmitError::Other)?
        {
            Ok(StatementResult::CreatedQuery(query, _)) => Ok(*query.id as u64),
            Ok(other) => Err(SubmitError::Other(format!(
                "a statistic query answered with {other:?} instead of a query"
            ))),
            Err(error) if is_placement_failure(&error) => {
                Err(SubmitError::Placement(error.to_string()))
            }
            Err(error) => Err(SubmitError::Other(error.to_string())),
        }
    }

    async fn stop(&self, query_id: u64) -> Result<(), String> {
        let filters = GetQuery::all().with_id(QueryId::new(query_id as i64));
        let statement = Statement::DropQuery(DropQuery::all().with_filters(filters));
        let payload = Payload::parsed(statement).until_terminated(Some(SUBMIT_TIMEOUT));
        match execute(&self.sender, payload).await? {
            Ok(_) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }
}

/// Reads the workers and physical sources from the coordinator's catalog.
pub struct ChannelCatalog {
    sender: async_channel::Sender<Request>,
}

impl ChannelCatalog {
    pub fn new(sender: async_channel::Sender<Request>) -> Self {
        Self { sender }
    }
}

#[async_trait::async_trait]
impl WorkerCatalog for ChannelCatalog {
    async fn source_workers(&self, logical_source: &str) -> Result<Vec<NetworkAddr>, String> {
        let statement = Statement::GetPhysicalSource(GetPhysicalSource::all());
        match execute(&self.sender, Payload::parsed(statement)).await? {
            // The generated query names its source unquoted, so the catalog holds its name in canonical case.
            Ok(StatementResult::PhysicalSources(sources)) => Ok(sources
                .into_iter()
                .filter(|source| {
                    source
                        .logical_source
                        .as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(logical_source))
                })
                .map(|source| source.host_addr)
                .collect()),
            Ok(other) => Err(format!(
                "listing the physical sources answered with {other:?}"
            )),
            Err(error) => Err(error.to_string()),
        }
    }

    async fn workers(&self) -> Result<Vec<NetworkAddr>, String> {
        let statement = Statement::GetWorker(GetWorker::all());
        match execute(&self.sender, Payload::parsed(statement)).await? {
            Ok(StatementResult::Workers(workers)) => {
                Ok(workers.into_iter().map(|worker| worker.host_addr).collect())
            }
            Ok(other) => Err(format!("listing the workers answered with {other:?}")),
            Err(error) => Err(error.to_string()),
        }
    }
}

pub struct GrpcImpulse;

#[async_trait::async_trait]
impl ProbeImpulse for GrpcImpulse {
    async fn request(
        &self,
        endpoint: &NetworkAddr,
        statistic_id: u64,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<(), String> {
        let mut client =
            proto::statistic_source_service_client::StatisticSourceServiceClient::connect(format!(
                "http://{endpoint}"
            ))
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

/// The interface the service listens on, given the host workers are told to report to.
/// A loopback host only has to be reachable from this machine; any other host is reached from the workers', so the
/// service listens on every interface.
pub fn bind_ip(advertised_host: &str) -> IpAddr {
    if advertised_host.eq_ignore_ascii_case("localhost") {
        return IpAddr::V4(Ipv4Addr::LOCALHOST);
    }
    match advertised_host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
    {
        Ok(ip) if ip.is_loopback() => ip,
        Ok(IpAddr::V6(_)) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        _ => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
    }
}

pub fn pick_port(bind: IpAddr, requested: u16) -> std::io::Result<u16> {
    if requested != 0 {
        return Ok(requested);
    }
    let listener = std::net::TcpListener::bind((bind, 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

pub async fn run(
    sender: async_channel::Sender<Request>,
    requested_port: u16,
    advertised_host: &str,
) {
    let advertised_host = if advertised_host.is_empty() {
        DEFAULT_ADVERTISED_HOST
    } else {
        advertised_host
    };
    let bind = bind_ip(advertised_host);
    for attempt in 1..=BIND_ATTEMPTS {
        let port = match pick_port(bind, requested_port) {
            Ok(port) => port,
            Err(error) => {
                tracing::error!(%error, "statistic service could not reserve a port");
                return;
            }
        };
        match serve(sender.clone(), SocketAddr::new(bind, port), advertised_host).await {
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

async fn serve(
    sender: async_channel::Sender<Request>,
    address: SocketAddr,
    advertised_host: &str,
) -> Result<(), String> {
    let advertised = NetworkAddr::new(advertised_host, address.port())
        .map_err(|error| format!("statistic service address is not valid: {error}"))?;

    let service = Arc::new(StatisticService::new(
        Arc::new(ChannelSubmitter::new(sender.clone())),
        Arc::new(ChannelCatalog::new(sender)),
        Arc::new(UnresolvedSchemas),
        Arc::new(GrpcImpulse),
        advertised.to_string(),
    ));

    tracing::info!(%address, %advertised, "statistic service listening");
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
