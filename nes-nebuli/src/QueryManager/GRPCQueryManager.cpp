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

#include <QueryManager/GRPCQueryManager.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
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
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{
GRPCQueryManager::GRPCQueryManager(const std::shared_ptr<grpc::Channel>& channel) : stub(WorkerRPCService::NewStub(channel))
{
}

std::expected<QueryId, Exception> GRPCQueryManager::registerQuery(const NES::LogicalPlan& plan) noexcept
{
    try
    {
        grpc::ClientContext context;
        RegisterQueryReply reply;
        RegisterQueryRequest request;
        request.mutable_queryplan()->CopyFrom(NES::QueryPlanSerializationUtil::serializeQueryPlan(plan));
        auto status = stub->RegisterQuery(&context, request, &reply);
        if (status.ok())
        {
            NES_DEBUG("Registration was successful.");
            return QueryId{reply.queryid()};
        }
        return std::unexpected{QueryRegistrationFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details())};
    }
    catch (std::exception& e)
    {
        return std::unexpected{QueryRegistrationFailed("Message from external exception: {} ", e.what())};
    }
}

std::expected<void, Exception> GRPCQueryManager::start(const QueryId queryId) noexcept
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
            NES_DEBUG("Starting was successful.");
            return {};
        }

        return std::unexpected{NES::QueryStartFailed(
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

std::expected<NES::QuerySummary, Exception> GRPCQueryManager::status(const QueryId queryId) const noexcept
{
    try
    {
        grpc::ClientContext context;
        QuerySummaryRequest request;
        request.set_queryid(queryId.getRawValue());
        QuerySummaryReply response;
        if (const auto status = stub->RequestQuerySummary(&context, request, &response); status.ok())
        {
            NES_DEBUG("Status was successful.");
        }
        else
        {
            if (status.error_code() == grpc::StatusCode::NOT_FOUND)
            {
                /// separate exception so that the embedded worker query mananager can give back the same exception
                return std::unexpected{NES::QueryNotFound("{}", queryId)};
            }
            return std::unexpected{NES::QueryStatusFailed(
                "Could not request status for query {}.\nStatus: {}\nMessage: {}\nDetail: {}",
                queryId,
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details())};
        }

        /// Convert the gRPC object to a C++ one
        std::vector<NES::QueryRunSummary> runs;
        for (const auto& run : response.runs())
        {
            const std::chrono::system_clock::time_point startTimePoint(std::chrono::milliseconds(run.startunixtimeinms()));
            const std::chrono::system_clock::time_point runningTimePoint(std::chrono::milliseconds(run.runningunixtimeinms()));
            const std::chrono::system_clock::time_point stopTimePoint(std::chrono::milliseconds(run.stopunixtimeinms()));
            /// Creating a new QueryRunSummary
            runs.emplace_back();
            runs.back().start = startTimePoint;
            runs.back().running = runningTimePoint;
            runs.back().stop = stopTimePoint;

            if (run.has_error())
            {
                const auto& runError = run.error();
                const NES::Exception exception(runError.message(), runError.code());
                runs.back().error = exception;
            }
        }
        /// First, we need to cast the gRPC enum value to an int and then to the C++ enum
        const auto queryStatus(static_cast<NES::QueryStatus>(static_cast<uint8_t>(response.status())));
        NES::QuerySummary querySummary = {.queryId = NES::QueryId(response.queryid()), .currentStatus = queryStatus, .runs = runs};
        return querySummary;
    }
    catch (std::exception& e)
    {
        return std::unexpected{QueryStatusFailed("Message from external exception: {} ", e.what())};
    }
}

std::expected<void, Exception> GRPCQueryManager::unregister(const QueryId queryId) noexcept
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
            NES_DEBUG("Unregister was successful.");
            return {};
        }

        return std::unexpected{NES::QueryUnregistrationFailed(
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

std::expected<void, Exception> GRPCQueryManager::stop(const QueryId queryId) noexcept
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
            NES_DEBUG("Stopping was successful.");
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
}
