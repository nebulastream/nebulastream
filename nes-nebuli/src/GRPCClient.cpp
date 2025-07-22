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

#include <GRPCClient.hpp>

#include <cstddef>
#include <memory>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
/// Both are needed, clang-tidy complains otherwise
#include <Identifiers/Identifiers.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{

GRPCClient::GRPCClient(const std::shared_ptr<grpc::Channel>& channel) : stub(WorkerRPCService::NewStub(channel))
{
}

QueryId GRPCClient::registerQuery(const LogicalPlan& plan) const
{
    grpc::ClientContext context;
    RegisterQueryReply reply;
    RegisterQueryRequest request;
    request.mutable_queryplan()->CopyFrom(QueryPlanSerializationUtil::serializeQueryPlan(plan));
    if (const auto status = stub->RegisterQuery(&context, request, &reply); status.ok())
    {
        NES_DEBUG("Registration was successful.");
    }
    else
    {
        throw QueryRegistrationFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details());
    }
    return QueryId{reply.queryid()};
}

void GRPCClient::start(const QueryId queryId) const
{
    grpc::ClientContext context;
    StartQueryRequest request;
    google::protobuf::Empty response;
    request.set_queryid(queryId.getRawValue());
    if (const auto status = stub->StartQuery(&context, request, &response); status.ok())
    {
        NES_DEBUG("Starting was successful.");
    }
    else
    {
        throw QueryStartFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details());
    }
}


QuerySummary GRPCClient::status(const QueryId queryId) const
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
        throw NES::QueryStatusFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details());
    }

    /// Convert the gRPC object to a C++ one
    std::vector<NES::QueryRunSummary> runs;
    for (auto run : response.runs())
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
            const auto runError = run.error();
            NES::Exception exception(runError.message(), runError.code());
            runs.back().error = exception;
        }
    }
    /// First, we need to cast the gRPC enum value to an int and then to the C++ enum
    const auto queryStatus(static_cast<NES::QueryStatus>(static_cast<uint8_t>(response.status())));
    QuerySummary querySummary = {.queryId = NES::QueryId(response.queryid()), .currentStatus = queryStatus, .runs = runs};
    return querySummary;
}

void GRPCClient::unregister(const QueryId queryId) const
{
    grpc::ClientContext context;
    UnregisterQueryRequest request;
    google::protobuf::Empty response;
    request.set_queryid(queryId.getRawValue());
    if (const auto status = stub->UnregisterQuery(&context, request, &response); status.ok())
    {
        NES_DEBUG("Unregister was successful.");
    }
    else
    {
        throw QueryUnregistrationFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details());
    }
}

void GRPCClient::stop(const QueryId queryId) const
{
    grpc::ClientContext context;
    StopQueryRequest request;
    request.set_queryid(queryId.getRawValue());
    request.set_terminationtype(StopQueryRequest::Graceful);
    google::protobuf::Empty response;
    if (const auto status = stub->StopQuery(&context, request, &response); status.ok())
    {
        NES_DEBUG("Stopping was successful.");
    }
    else
    {
        throw QueryStopFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details());
    }
}

WorkerStatusResponse GRPCClient::summary(const std::chrono::system_clock::time_point after) const
{
    grpc::ClientContext context;
    WorkerStatusRequest request;
    request.set_afterunixtimestampinms(std::chrono::duration_cast<std::chrono::milliseconds>(after.time_since_epoch()).count());
    WorkerStatusResponse response;
    if (const auto status = stub->RequestStatus(&context, request, &response); status.ok())
    {
        NES_DEBUG("Unregister was successful.");
    }
    else
    {
        throw QueryStatusFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details());
    }
    return response;
}
}
