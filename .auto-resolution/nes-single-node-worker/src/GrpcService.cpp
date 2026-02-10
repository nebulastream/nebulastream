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

#include <GrpcService.hpp>

#include <exception>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Strings.hpp>
#include <cpptrace/basic.hpp>
#include <cpptrace/from_current.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{
namespace
{
grpc::Status handleError(const std::exception& exception, grpc::ServerContext* context)
{
    NES_ERROR("GRPC Request failed with exception: {}", exception.what());
    context->AddTrailingMetadata("code", std::to_string(ErrorCode::UnknownException));
    context->AddTrailingMetadata("what", exception.what());
    context->AddTrailingMetadata("trace", replaceAll(cpptrace::from_current_exception().to_string(false), "\n", ""));
    return {grpc::INTERNAL, exception.what()};
}

grpc::Status handleError(const Exception& exception, grpc::ServerContext* context)
{
    NES_ERROR("GRPC Request failed with exception: {}", exception.what());
    context->AddTrailingMetadata("code", std::to_string(exception.code()));
    context->AddTrailingMetadata("what", exception.what());
    context->AddTrailingMetadata("trace", replaceAll(cpptrace::from_current_exception().to_string(false), "\n", ""));
    return {grpc::INTERNAL, exception.what()};
}

template <typename T>
T getValueOrThrow(std::expected<T, Exception> expected)
{
    if (expected.has_value())
    {
        return expected.value();
    }
    throw std::move(expected.error());
}
}

grpc::Status GRPCServer::RegisterQuery(grpc::ServerContext* context, const RegisterQueryRequest* request, RegisterQueryReply* response)
{
    auto fullySpecifiedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(request->queryplan());
    CPPTRACE_TRY
    {
        auto result = delegate.registerQuery(std::move(fullySpecifiedQueryPlan));
        if (result.has_value())
        {
            response->set_queryid(result->getRawValue());
            return grpc::Status::OK;
        }
        return handleError(result.error(), context);
    }
    CPPTRACE_CATCH(const std::exception& e)
    {
        return handleError(e, context);
    }
    return {grpc::INTERNAL, "unknown exception"};
}

grpc::Status GRPCServer::UnregisterQuery(grpc::ServerContext* context, const UnregisterQueryRequest* request, google::protobuf::Empty*)
{
    const auto queryId = QueryId(request->queryid());
    CPPTRACE_TRY
    {
        getValueOrThrow(delegate.unregisterQuery(queryId));
        return grpc::Status::OK;
    }
    CPPTRACE_CATCH(const Exception& e)
    {
        return handleError(e, context);
    }
    CPPTRACE_CATCH_ALT(const std::exception& e)
    {
        return handleError(e, context);
    }
    return {grpc::INTERNAL, "unkown exception"};
}

grpc::Status GRPCServer::StartQuery(grpc::ServerContext* context, const StartQueryRequest* request, google::protobuf::Empty*)
{
    const auto queryId = QueryId(request->queryid());
    CPPTRACE_TRY
    {
        getValueOrThrow(delegate.startQuery(queryId));
        return grpc::Status::OK;
    }
    CPPTRACE_CATCH(const Exception& e)
    {
        return handleError(e, context);
    }
    CPPTRACE_CATCH_ALT(const std::exception& e)
    {
        return handleError(e, context);
    }
    return {grpc::INTERNAL, "unknown exception"};
}

grpc::Status GRPCServer::StopQuery(grpc::ServerContext* context, const StopQueryRequest* request, google::protobuf::Empty*)
{
    const auto queryId = QueryId(request->queryid());
    const auto terminationType = static_cast<QueryTerminationType>(request->terminationtype());
    CPPTRACE_TRY
    {
        getValueOrThrow(delegate.stopQuery(queryId, terminationType));
        return grpc::Status::OK;
    }
    CPPTRACE_CATCH(const Exception& e)
    {
        return handleError(e, context);
    }
    CPPTRACE_CATCH_ALT(const std::exception& e)
    {
        return handleError(e, context);
    }
    return {grpc::INTERNAL, "unknown exception"};
}

grpc::Status GRPCServer::RequestQueryStatus(grpc::ServerContext* context, const QueryStatusRequest* request, QueryStatusReply* reply)
{
    CPPTRACE_TRY
    {
        const auto queryId = QueryId{request->queryid()};
        reply->set_queryid(queryId.getRawValue());
        if (const auto queryStatus = delegate.getQueryStatus(queryId); queryStatus.has_value())
        {
            const auto& [start, running, stop, error] = queryStatus->metrics;
            reply->set_state(static_cast<::QueryState>(queryStatus->state));

            if (start.has_value())
            {
                reply->mutable_metrics()->set_startunixtimeinms(
                    std::chrono::duration_cast<std::chrono::milliseconds>(start->time_since_epoch()).count());
            }

            if (running.has_value())
            {
                reply->mutable_metrics()->set_runningunixtimeinms(
                    std::chrono::duration_cast<std::chrono::milliseconds>(running->time_since_epoch()).count());
            }

            if (stop.has_value())
            {
                reply->mutable_metrics()->set_stopunixtimeinms(
                    std::chrono::duration_cast<std::chrono::milliseconds>(stop->time_since_epoch()).count());
            }

            if (error.has_value())
            {
                auto* errorProto = reply->mutable_metrics()->mutable_error();
                errorProto->set_message(error->what());
                errorProto->set_stacktrace(error->trace().to_string());
                errorProto->set_code(error->code());
                errorProto->set_location(std::string{error->where()->filename} + ":" + std::to_string(error->where()->line.value_or(0)));
            }
            return grpc::Status::OK;
        }
        return {grpc::NOT_FOUND, "Query does not exist"};
    }
    CPPTRACE_CATCH(const Exception& e)
    {
        return handleError(e, context);
    }
    CPPTRACE_CATCH_ALT(const std::exception& e)
    {
        return handleError(e, context);
    }
    return {grpc::INTERNAL, "unknown exception"};
}

grpc::Status GRPCServer::RequestQueryLog(grpc::ServerContext* context, const QueryLogRequest* request, QueryLogReply* reply)
{
    CPPTRACE_TRY
    {
        auto queryId = QueryId(request->queryid());
        auto log = delegate.getQueryLog(queryId);
        if (log.has_value())
        {
            for (const auto& entry : *log)
            {
                QueryLogEntry logEntry;
                logEntry.set_state(static_cast<::QueryState>(entry.state));
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
    CPPTRACE_CATCH(const Exception& e)
    {
        return handleError(e, context);
    }
    CPPTRACE_CATCH_ALT(const std::exception& e)
    {
        return handleError(e, context);
    }
    return {grpc::INTERNAL, "unkown exception"};
}

}
