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
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>
#include <WorkerStatus.hpp>

namespace NES
{
GRPCQuerySubmissionBackend::GRPCQuerySubmissionBackend(WorkerConfig config)
    : stub{WorkerRPCService::NewStub(grpc::CreateChannel(config.grpc.getRawValue(), grpc::InsecureChannelCredentials()))}
    , workerConfig{std::move(config)}
{
}

std::expected<LocalQueryId, Exception> GRPCQuerySubmissionBackend::registerQuery(LogicalPlan localPlan)
{
    try
    {
        grpc::ClientContext context;
        RegisterQueryReply reply;
        RegisterQueryRequest request;
        request.mutable_queryplan()->CopyFrom(QueryPlanSerializationUtil::serializeQueryPlan(localPlan));
        const auto status = stub->RegisterQuery(&context, request, &reply);
        if (status.ok())
        {
            NES_DEBUG("Registration to node {} was successful.", workerConfig.grpc);
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

std::expected<void, Exception> GRPCQuerySubmissionBackend::start(LocalQueryId queryId)
{
    try
    {
        grpc::ClientContext context;
        StartQueryRequest request;
        google::protobuf::Empty response;
        request.set_queryid(queryId.getRawValue());
        const auto status = stub->StartQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Starting query {} on node {} was successful.", queryId, workerConfig.grpc);
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

std::expected<LocalQueryStatus, Exception> GRPCQuerySubmissionBackend::status(LocalQueryId queryId) const
{
    try
    {
        grpc::ClientContext context;
        QueryStatusRequest request;
        request.set_queryid(queryId.getRawValue());
        QueryStatusReply response;

        if (const auto status = stub->RequestQueryStatus(&context, request, &response); status.ok())
        {
            NES_DEBUG("Status of query {} on node {} was successful.", queryId, workerConfig.grpc);
        }
        else
        {
            if (status.error_code() == grpc::StatusCode::NOT_FOUND)
            {
                /// separate exception so that the embedded worker query manager can give back the same exception
                return std::unexpected{QueryNotFound("{}", queryId)};
            }
            return std::unexpected{QueryStatusFailed(
                "Could not request status for query {}.\nStatus: {}\nMessage: {}\nDetail: {}",
                queryId,
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

std::expected<WorkerStatus, Exception> GRPCQuerySubmissionBackend::workerStatus(std::chrono::system_clock::time_point after) const
{
    CPPTRACE_TRY
    {
        grpc::ClientContext context;
        WorkerStatusRequest request;
        WorkerStatusResponse response;
        request.set_afterunixtimestampinms(std::chrono::duration_cast<std::chrono::milliseconds>(after.time_since_epoch()).count());
        const auto responseCode = stub->RequestStatus(&context, request, &response);
        if (!responseCode.ok())
        {
            return std::unexpected(UnknownException("GRPC Status: {}", responseCode.error_message()));
        }
        return deserializeWorkerStatus(&response);
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(UnknownException("Exception in workerStatus"));
    }
    std::unreachable();
}

std::expected<void, Exception> GRPCQuerySubmissionBackend::stop(LocalQueryId queryId)
{
    try
    {
        grpc::ClientContext context;
        StopQueryRequest request;
        request.set_queryid(queryId.getRawValue());
        request.set_terminationtype(StopQueryRequest::Graceful);
        google::protobuf::Empty response;

        const auto status = stub->StopQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Stopping query {} on node {} was successful.", queryId, workerConfig.grpc);
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

std::expected<void, Exception> GRPCQuerySubmissionBackend::unregister(LocalQueryId queryId)
{
    try
    {
        grpc::ClientContext context;
        UnregisterQueryRequest request;
        google::protobuf::Empty response;
        request.set_queryid(queryId.getRawValue());

        const auto status = stub->UnregisterQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Unregister of query {} on node {} was successful.", queryId, workerConfig.grpc);
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
