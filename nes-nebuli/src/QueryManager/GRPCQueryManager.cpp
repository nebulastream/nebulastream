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

#include <google/protobuf/empty.pb.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <magic_enum/magic_enum.hpp>

#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
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

std::expected<LocalQueryStatus, Exception> GRPCQueryManager::status(const QueryId queryId) const noexcept
{
    try
    {
        grpc::ClientContext context;
        QueryStatusRequest request;
        request.set_queryid(queryId.getRawValue());
        QueryStatusReply response;
        if (const auto status = stub->RequestQueryStatus(&context, request, &response); status.ok())
        {
            NES_DEBUG("Status was successful.");
        }
        else
        {
            if (status.error_code() == grpc::StatusCode::NOT_FOUND)
            {
                /// separate exception so that the embedded worker query manager can give back the same exception
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
        /// Creating a new local query status
        LocalQueryStatus queryStatus;
        queryStatus.queryId = queryId;
        const auto responseMetrics = response.metrics();
        if (responseMetrics.has_startunixtimeinms())
        {
            const std::chrono::system_clock::time_point startTimePoint{std::chrono::milliseconds{response.metrics().startunixtimeinms()}};
            queryStatus.metrics.start = startTimePoint;
        }

        if (responseMetrics.has_runningunixtimeinms())
        {
            const std::chrono::system_clock::time_point runningTimePoint{std::chrono::milliseconds{responseMetrics.runningunixtimeinms()}};
            queryStatus.metrics.running = runningTimePoint;
        }

        if (responseMetrics.has_stopunixtimeinms())
        {
            const std::chrono::system_clock::time_point stopTimePoint{std::chrono::milliseconds{responseMetrics.stopunixtimeinms()}};
            queryStatus.metrics.stop = stopTimePoint;
        }

        if (responseMetrics.has_error())
        {
            const auto err = response.metrics().error();
            const Exception exception{err.message(), err.code()};
            queryStatus.metrics.error = exception;
        }

        queryStatus.state = magic_enum::enum_cast<QueryState>(response.state()).value(); /// Invalid state will throw
        return queryStatus;
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
