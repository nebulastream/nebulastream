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
#include <Util/Strings.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <cpptrace/basic.hpp>
#include <cpptrace/from_current.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{

grpc::Status handleError(const std::exception& exception, grpc::ServerContext* context)
{
    context->AddTrailingMetadata("code", std::to_string(ErrorCode::UnknownException));
    context->AddTrailingMetadata("what", exception.what());
    context->AddTrailingMetadata("trace", Util::replaceAll(cpptrace::from_current_exception().to_string(false), "\n", ""));
    return {grpc::INTERNAL, exception.what()};
}

grpc::Status handleError(const Exception& exception, grpc::ServerContext* context)
{
    context->AddTrailingMetadata("code", std::to_string(exception.code()));
    context->AddTrailingMetadata("what", exception.what());
    context->AddTrailingMetadata("trace", Util::replaceAll(cpptrace::from_current_exception().to_string(false), "\n", ""));
    return {grpc::INTERNAL, exception.what()};
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
    auto queryId = QueryId(request->queryid());
    CPPTRACE_TRY
    {
        delegate.unregisterQuery(queryId);
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
    auto queryId = QueryId(request->queryid());
    CPPTRACE_TRY
    {
        delegate.startQuery(queryId);
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
grpc::Status GRPCServer::StopQuery(grpc::ServerContext* context, const StopQueryRequest* request, google::protobuf::Empty*)
{
    auto queryId = QueryId(request->queryid());
    auto terminationType = static_cast<QueryTerminationType>(request->terminationtype());
    CPPTRACE_TRY
    {
        delegate.stopQuery(queryId, terminationType);
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

grpc::Status GRPCServer::RequestQuerySummary(grpc::ServerContext* context, const QuerySummaryRequest* request, QuerySummaryReply* reply)
{
    CPPTRACE_TRY
    {
        auto queryId = QueryId(request->queryid());
        auto summary = delegate.getQuerySummary(queryId);
        if (summary.has_value())
        {
            reply->set_status(::QueryStatus(summary->currentStatus));
            for (const auto& [start, running, stop, error] : summary->runs)
            {
                auto* const replyRun = reply->add_runs();
                replyRun->set_startunixtimeinms(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        start.value_or(std::chrono::system_clock::time_point(std::chrono::seconds(0))).time_since_epoch())
                        .count());
                replyRun->set_runningunixtimeinms(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        running.value_or(std::chrono::system_clock::time_point(std::chrono::seconds(0))).time_since_epoch())
                        .count());
                replyRun->set_stopunixtimeinms(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        stop.value_or(std::chrono::system_clock::time_point(std::chrono::seconds(0))).time_since_epoch())
                        .count());
                if (error.has_value())
                {
                    auto* const runError = replyRun->mutable_error();
                    runError->set_message(error->what());
                    runError->set_stacktrace(error->trace().to_string());
                    runError->set_code(error->code());
                    runError->set_location(std::string(error->where()->filename) + ":" + std::to_string(error->where()->line.value_or(0)));
                }
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
                logEntry.set_status(static_cast<::QueryStatus>(entry.state));
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
grpc::Status GRPCServer::RequestStatus(grpc::ServerContext* context, const WorkerStatusRequest* request, WorkerStatusResponse* response)
{
    CPPTRACE_TRY
    {
        const auto status
            = delegate.getWorkerStatus(std::chrono::system_clock::time_point(std::chrono::milliseconds(request->afterunixtimestampinms())));

        for (const auto& activeQuery : status.activeQueries)
        {
            auto* activeQueryGRPC = response->add_activequeries();
            activeQueryGRPC->set_queryid(activeQuery.queryId.getRawValue());
            activeQueryGRPC->set_startedunixtimestampinms(
                std::chrono::duration_cast<std::chrono::milliseconds>(activeQuery.started.time_since_epoch()).count());
        }

        for (const auto& terminatedQuery : status.terminatedQueries)
        {
            auto* terminatedQueryGRPC = response->add_terminatedqueries();
            terminatedQueryGRPC->set_queryid(terminatedQuery.queryId.getRawValue());
            terminatedQueryGRPC->set_startedunixtimestampinms(
                std::chrono::duration_cast<std::chrono::milliseconds>(terminatedQuery.started.time_since_epoch()).count());
            terminatedQueryGRPC->set_terminatedunixtimestampinms(
                std::chrono::duration_cast<std::chrono::milliseconds>(terminatedQuery.terminated.time_since_epoch()).count());
            if (terminatedQuery.error)
            {
                const auto& exception = terminatedQuery.error.value();
                auto* errorGRPC = terminatedQueryGRPC->mutable_error();
                errorGRPC->set_message(exception.what());
                errorGRPC->set_stacktrace(exception.trace().to_string());
                errorGRPC->set_code(exception.code());
                errorGRPC->set_location(
                    std::string(exception.where()->filename) + ":" + std::to_string(exception.where()->line.value_or(0)));
            }
        }
        response->set_afterunixtimestampinms(
            std::chrono::duration_cast<std::chrono::milliseconds>(status.after.time_since_epoch()).count());

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

}
