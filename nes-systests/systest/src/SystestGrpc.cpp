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


#include <Operators/Serialization/QueryPlanSerializationUtil.hpp>
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
    NES::QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, request.mutable_queryplan());
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

QuerySummaryReply GRPCClient::status(size_t queryId) const
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
        throw NES::QueryStopFailed(
            "Status: {}\nMessage: {}\nDetail: {}",
            magic_enum::enum_name(status.error_code()),
            status.error_message(),
            status.error_details());
    }
    return response;
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
