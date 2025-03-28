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

#include <string>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <ErrorHandling.hpp>
#include <GrpcService.hpp>
#include <SingleNodeWorkerRPCService.pb.h>
#include <Plans/LogicalPlan.hpp>

namespace NES
{
grpc::Status handleError(const Exception& exception, grpc::ServerContext* context)
{
    context->AddTrailingMetadata("code", std::to_string(exception.code()));
    context->AddTrailingMetadata("what", exception.what());
    if (auto where = exception.where())
    {
        context->AddTrailingMetadata("where", where->to_string());
    }
    return grpc::Status(grpc::INTERNAL, exception.what());
}

grpc::Status GRPCServer::RegisterQuery(grpc::ServerContext* context, const RegisterQueryRequest* request, RegisterQueryReply* response)
{
    auto fullySpecifiedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(&request->queryplan());
    try
    {
        auto queryId = delegate.registerQuery(fullySpecifiedQueryPlan);
        response->set_queryid(queryId.getRawValue());
        return grpc::Status::OK;
    }
    catch (...)
    {
        return handleError(wrapExternalException(), context);
    }
}
grpc::Status GRPCServer::UnregisterQuery(grpc::ServerContext* context, const UnregisterQueryRequest* request, google::protobuf::Empty*)
{
    auto queryId = QueryId(request->queryid());
    try
    {
        delegate.unregisterQuery(queryId);
        return grpc::Status::OK;
    }
    catch (...)
    {
        return handleError(wrapExternalException(), context);
    }
}
grpc::Status GRPCServer::StartQuery(grpc::ServerContext* context, const StartQueryRequest* request, google::protobuf::Empty*)
{
    auto queryId = QueryId(request->queryid());
    try
    {
        delegate.startQuery(queryId);
        return grpc::Status::OK;
    }
    catch (...)
    {
        return handleError(wrapExternalException(), context);
    }
}
grpc::Status GRPCServer::StopQuery(grpc::ServerContext* context, const StopQueryRequest* request, google::protobuf::Empty*)
{
    auto queryId = QueryId(request->queryid());
    auto terminationType = static_cast<QueryTerminationType>(request->terminationtype());
    try
    {
        delegate.stopQuery(queryId, terminationType);
        return grpc::Status::OK;
    }
    catch (...)
    {
        return handleError(wrapExternalException(), context);
    }
}

grpc::Status GRPCServer::RequestQuerySummary(grpc::ServerContext* context, const QuerySummaryRequest* request, QuerySummaryReply* reply)
{
    try
    {
        auto queryId = QueryId(request->queryid());
        auto summary = delegate.getQuerySummary(queryId);
        if (summary.has_value())
        {
            reply->set_status(::QueryStatus(summary->currentStatus));
            reply->set_numberofrestarts(summary->numberOfRestarts);
            for (const auto& exception : summary->exceptions)
            {
                Error error;
                error.set_message(exception.what());
                error.set_stacktrace(exception.trace().to_string());
                error.set_code(exception.code());
                error.set_location(std::string(exception.where()->filename) + ":" + std::to_string(exception.where()->line.value_or(0)));
                reply->add_error()->CopyFrom(error);
            }
            return grpc::Status::OK;
        }
        return grpc::Status(grpc::NOT_FOUND, "Query does not exist");
    }
    catch (...)
    {
        return handleError(wrapExternalException(), context);
    }
}

grpc::Status GRPCServer::RequestQueryLog(grpc::ServerContext* context, const QueryLogRequest* request, QueryLogReply* reply)
{
    try
    {
        auto queryId = QueryId(request->queryid());
        auto log = delegate.getQueryLog(queryId);
        if (log.has_value())
        {
            for (const auto& entry : *log)
            {
                QueryLogEntry logEntry;
                logEntry.set_status((::QueryStatus)entry.state);
                logEntry.set_unixtimeinms(
                    std::chrono::duration_cast<std::chrono::milliseconds>(entry.timestamp.time_since_epoch()).count());
                if (entry.exception.has_value())
                {
                    Error error;
                    error.set_message(entry.exception.value().what());
                    error.set_stacktrace(entry.exception.value().trace().to_string());
                    error.set_code(entry.exception.value().code());
                    error.set_location(
                        std::string(entry.exception.value().where()->filename) + ":"
                        + std::to_string(entry.exception.value().where()->line.value_or(0)));
                    logEntry.mutable_error()->CopyFrom(error);
                }
                reply->add_entries()->CopyFrom(logEntry);
            }
            return grpc::Status::OK;
        }
        return grpc::Status(grpc::NOT_FOUND, "Query does not exist");
    }
    catch (...)
    {
        return handleError(wrapExternalException(), context);
    }
}

}
