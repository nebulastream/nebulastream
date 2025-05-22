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


#include <cstddef>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <grpcpp/channel.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestGrpc.hpp>

GRPCClient::GRPCClient(std::shared_ptr<grpc::Channel> channel) : stub(WorkerRPCService::NewStub(channel))
{
}

size_t GRPCClient::registerQuery(const NES::LogicalPlan& queryPlan) const
{
    grpc::ClientContext context;
    RegisterQueryReply reply;
    RegisterQueryRequest request;
    request.mutable_queryplan()->CopyFrom(NES::QueryPlanSerializationUtil::serializeQueryPlan(queryPlan));
    auto status = stub->RegisterQuery(&context, request, &reply);
    if (status.ok())
    {
        NES_DEBUG("Registration was successful.");
    }
    else
    {
        throw NES::QueryRegistrationFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details());
    }
    return reply.queryid();
}

void GRPCClient::start(size_t queryId) const
{
    grpc::ClientContext context;
    StartQueryRequest request;
    google::protobuf::Empty response;
    request.set_queryid(queryId);
    auto status = stub->StartQuery(&context, request, &response);
    if (status.ok())
    {
        NES_DEBUG("Starting was successful.");
    }
    else
    {
        throw NES::QueryStartFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details());
    }
}

NES::QuerySummary GRPCClient::status(size_t queryId) const
{
    grpc::ClientContext context;
    QuerySummaryRequest request;
    request.set_queryid(queryId);
    QuerySummaryReply response;
    auto status = stub->RequestQuerySummary(&context, request, &response);
    if (status.ok())
    {
        NES_DEBUG("Stopping was successful.");
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
    NES::QuerySummary querySummary = {.queryId = NES::QueryId(response.queryid()), .currentStatus = queryStatus, .runs = runs};
    return querySummary;
}

void GRPCClient::unregister(size_t queryId) const
{
    grpc::ClientContext context;
    UnregisterQueryRequest request;
    google::protobuf::Empty response;
    request.set_queryid(queryId);
    auto status = stub->UnregisterQuery(&context, request, &response);
    if (status.ok())
    {
        NES_DEBUG("Unregister was successful.");
    }
    else
    {
        throw NES::QueryUnregistrationFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details());
    }
}
