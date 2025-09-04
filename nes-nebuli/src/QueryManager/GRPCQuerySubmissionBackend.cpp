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

#include <QueryManager/GRPCQuerySubmissionBackend.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
/// Both are needed, clang-tidy complains otherwise
#include <grpcpp/create_channel.h>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <grpcpp/security/credentials.h>
#include <DistributedQueryId.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{
GRPCQuerySubmissionBackend::GRPCQuerySubmissionBackend(std::vector<WorkerConfig> configs)
    : cluster{
          configs
          | std::views::transform(
              [](const WorkerConfig& config)
              {
                  const auto channel = grpc::CreateChannel(config.grpc, grpc::InsecureChannelCredentials());
                  return std::make_pair(config.grpc, ClusterNode{.stub = WorkerRPCService::NewStub(channel), .workerConfig = config});
              })
          | std::ranges::to<std::unordered_map>()}
{
}

std::expected<LocalQueryId, Exception> GRPCQuerySubmissionBackend::registerQuery(const GrpcAddr& grpcAddr, LogicalPlan localPlan)
{
    try
    {
        grpc::ClientContext context;
        RegisterQueryReply reply;
        RegisterQueryRequest request;
        INVARIANT(cluster.contains(grpcAddr), "Plan was assigned to a node ({}) that is not part of the cluster", grpcAddr);
        request.mutable_queryplan()->CopyFrom(QueryPlanSerializationUtil::serializeQueryPlan(localPlan));
        const auto status = cluster.at(grpcAddr).stub->RegisterQuery(&context, request, &reply);
        if (status.ok())
        {
            NES_DEBUG("Registration of local query {} to node {} was successful.", localPlan.getQueryId(), grpcAddr);
            return LocalQueryId{reply.queryid()};
        }
        return std::unexpected{QueryRegistrationFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details())};
    }
    catch (const std::exception& e)
    {
        return std::unexpected{QueryRegistrationFailed("Message from external exception: {}", e.what())};
    }
}

std::expected<void, Exception> GRPCQuerySubmissionBackend::start(const LocalQuery& query)
{
    try
    {
        grpc::ClientContext context;
        StartQueryRequest request;
        google::protobuf::Empty response;
        request.set_queryid(query.id.getRawValue());
        INVARIANT(
            cluster.contains(query.grpcAddr), "Node {}, which is contained in the query id is not part of the cluster", query.grpcAddr);
        const auto status = cluster.at(query.grpcAddr).stub->StartQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Starting query {} on node {} was successful.", query.id, query.grpcAddr);
            return {};
        }

        return std::unexpected{QueryStartFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details())};
    }
    catch (std::exception& e)
    {
        return std::unexpected{QueryStartFailed("Message from external exception: {} ", e.what())};
    }
}

std::expected<LocalQueryStatus, Exception> GRPCQuerySubmissionBackend::status(const LocalQuery& query) const
{
    try
    {
        grpc::ClientContext context;
        QuerySummaryRequest request;
        request.set_queryid(query.id.getRawValue());
        QuerySummaryReply response;

        INVARIANT(
            cluster.contains(query.grpcAddr), "Node {}, which is contained in the query id is not part of the cluster", query.grpcAddr);
        if (const auto status = cluster.at(query.grpcAddr).stub->RequestQuerySummary(&context, request, &response); status.ok())
        {
            NES_DEBUG("Status of query {} on node {} was successful.", query.id, query.grpcAddr);
        }
        else
        {
            if (status.error_code() == grpc::StatusCode::NOT_FOUND)
            {
                /// separate exception so that the embedded worker query manager can give back the same exception
                return std::unexpected{QueryNotFound("{}", query.id)};
            }
            return std::unexpected{QueryStatusFailed(
                "Could not request status for query {}.\nStatus: {}\nMessage: {}\nDetail: {}",
                query.id,
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details())};
        }

        QueryMetrics metrics;
        const std::chrono::system_clock::time_point startTimePoint(std::chrono::milliseconds(response.metrics().startunixtimeinms()));
        const std::chrono::system_clock::time_point runningTimePoint(std::chrono::milliseconds(response.metrics().runningunixtimeinms()));
        const std::chrono::system_clock::time_point stopTimePoint(std::chrono::milliseconds(response.metrics().stopunixtimeinms()));
        metrics.start = startTimePoint;
        metrics.running = runningTimePoint;
        metrics.stop = stopTimePoint;

        if (response.metrics().has_error())
        {
            const auto& runError = response.metrics().error();
            const Exception exception(runError.message(), runError.code());
            metrics.error = exception;
        }

        return LocalQueryStatus(LocalQueryId{response.queryid()}, static_cast<QueryState>(static_cast<uint8_t>(response.state())), metrics);
    }
    catch (std::exception& e)
    {
        return std::unexpected{QueryStatusFailed("Message from external exception: {} ", e.what())};
    }
}

std::expected<void, Exception> GRPCQuerySubmissionBackend::stop(const LocalQuery& query)
{
    try
    {
        grpc::ClientContext context;
        StopQueryRequest request;
        request.set_queryid(query.id.getRawValue());
        request.set_terminationtype(StopQueryRequest::Graceful);
        google::protobuf::Empty response;

        INVARIANT(
            cluster.contains(query.grpcAddr), "Node {}, which is contained in the query id is not part of the cluster", query.grpcAddr);
        const auto status = cluster.at(query.grpcAddr).stub->StopQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Stopping query {} on node {} was successful.", query.id, query.grpcAddr);
            return {};
        }

        return std::unexpected{NES::QueryStopFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details())};
    }
    catch (std::exception& e)
    {
        return std::unexpected{QueryStopFailed("Message from external exception: {} ", e.what())};
    }
}

std::expected<void, Exception> GRPCQuerySubmissionBackend::unregister(const LocalQuery& query)
{
    try
    {
        grpc::ClientContext context;
        UnregisterQueryRequest request;
        google::protobuf::Empty response;
        request.set_queryid(query.id.getRawValue());

        INVARIANT(
            cluster.contains(query.grpcAddr), "Node {}, which is contained in the query id is not part of the cluster", query.grpcAddr);
        const auto status = cluster.at(query.grpcAddr).stub->UnregisterQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Unregister of query {} on node {} was successful.", query.id, query.grpcAddr);
            return {};
        }

        return std::unexpected{QueryUnregistrationFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details())};
    }
    catch (std::exception& e)
    {
        return std::unexpected{QueryUnregistrationFailed("Message from external exception: {} ", e.what())};
    }
}
}
