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

#include <chrono>
#include <cstddef>
#include <exception>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>

#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <cpptrace/basic.hpp>
#include <cpptrace/from_current.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerRPCService.pb.h>
#include <WorkerStatus.hpp>

namespace NES
{
namespace
{
/// Sanitize a string for use as GRPC ASCII metadata value.
/// GRPC metadata values must contain only printable ASCII (0x20-0x7E) and TAB (0x09).
constexpr char PRINTABLE_ASCII_MIN = 0x20;
constexpr char PRINTABLE_ASCII_MAX = 0x7E;
constexpr char HORIZONTAL_TAB = 0x09;

std::string sanitizeForGrpcMetadata(const std::string& value)
{
    std::string result;
    result.reserve(value.size());
    for (const char chr : value)
    {
        if ((chr >= PRINTABLE_ASCII_MIN && chr <= PRINTABLE_ASCII_MAX) || chr == HORIZONTAL_TAB)
        {
            result += chr;
        }
    }
    return result;
}

/// gRPC has a total trailing-metadata soft limit of 8192 bytes (by default).
/// With several entries (trace, what, grpc-message) each entry costs key+value+32 bytes overhead.
/// Cap each value at 2000 bytes so the combined metadata stays well under the limit.
constexpr std::size_t GRPC_METADATA_VALUE_MAX = 2000;

std::string truncateForGrpcMetadata(const std::string& value)
{
    auto sanitized = sanitizeForGrpcMetadata(value);
    if (sanitized.size() > GRPC_METADATA_VALUE_MAX)
    {
        sanitized.resize(GRPC_METADATA_VALUE_MAX);
        sanitized += "...[truncated]";
    }
    return sanitized;
}

grpc::Status handleError(const std::exception& exception, grpc::ServerContext* context)
{
    NES_ERROR("GRPC Request failed with exception: {}", exception.what());
    context->AddTrailingMetadata("code", std::to_string(ErrorCode::UnknownException));
    context->AddTrailingMetadata("what", truncateForGrpcMetadata(exception.what()));
    context->AddTrailingMetadata("trace", truncateForGrpcMetadata(formatStacktrace(cpptrace::from_current_exception(), false)));
    return {grpc::INTERNAL, truncateForGrpcMetadata(exception.what())};
}

grpc::Status handleError(const Exception& exception, grpc::ServerContext* context)
{
    NES_ERROR("GRPC Request failed with exception: {}", exception.what());
    context->AddTrailingMetadata("code", std::to_string(exception.code()));
    context->AddTrailingMetadata("what", truncateForGrpcMetadata(exception.what()));
    context->AddTrailingMetadata("trace", truncateForGrpcMetadata(formatStacktrace(exception.trace(), false)));
    return {grpc::INTERNAL, truncateForGrpcMetadata(exception.what())};
}

grpc::Status handleError(grpc::ServerContext* context)
{
    NES_ERROR("GRPC Request failed with unknown exception");
    context->AddTrailingMetadata("code", std::to_string(ErrorCode::UnknownException));
    context->AddTrailingMetadata("what", "unknown exception");
    context->AddTrailingMetadata("trace", sanitizeForGrpcMetadata(formatStacktrace(cpptrace::from_current_exception(), false)));
    return {grpc::INTERNAL, "unknown exception"};
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

grpc::Status tryWithDefaultHandling(const std::function<grpc::Status()>& f, grpc::ServerContext* context)
{
    grpc::Status status{};
    cpptrace::try_catch(
        [&] { status = f(); },
        [&](const Exception& e) { status = handleError(e, context); },
        [&](const std::exception& e) { status = handleError(e, context); },
        [&]() { status = handleError(context); });
    return status;
}

}

grpc::Status GRPCServer::StartQuery(grpc::ServerContext* context, const StartQueryRequest* request, StartQueryReply* response)
{
    auto queryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(request->queryplan());
    return tryWithDefaultHandling(
        [&]
        {
            auto result = delegate.startQuery(std::move(queryPlan));
            if (result.has_value())
            {
                *response->mutable_queryid() = QueryPlanSerializationUtil::serializeQueryId(*result);
                return grpc::Status::OK;
            }
            return handleError(result.error(), context);
        },
        context);
}

grpc::Status GRPCServer::StopQuery(grpc::ServerContext* context, const StopQueryRequest* request, google::protobuf::Empty*)
{
    const auto queryId = QueryPlanSerializationUtil::deserializeQueryId(request->queryid());
    return tryWithDefaultHandling(
        [&]
        {
            getValueOrThrow(delegate.stopQuery(queryId));
            return grpc::Status::OK;
        },
        context);
}

grpc::Status GRPCServer::RequestQueryStatus(grpc::ServerContext* context, const QueryStatusRequest* request, QueryStatusReply* reply)
{
    return tryWithDefaultHandling(
        [&]
        {
            const auto queryId = QueryPlanSerializationUtil::deserializeQueryId(request->queryid());
            *reply->mutable_queryid() = QueryPlanSerializationUtil::serializeQueryId(queryId);
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
                    errorProto->set_location(
                        std::string{error->where()->filename} + ":" + std::to_string(error->where()->line.value_or(0)));
                }
                return grpc::Status::OK;
            }
            return grpc::Status{grpc::NOT_FOUND, "Query does not exist"};
        },
        context);
}

grpc::Status GRPCServer::RequestStatus(grpc::ServerContext* context, const WorkerStatusRequest* request, WorkerStatusResponse* response)
{
    return tryWithDefaultHandling(
        [&]
        {
            const auto status = delegate.getWorkerStatus(
                std::chrono::system_clock::time_point(std::chrono::milliseconds(request->after_unix_timestamp_in_milli_seconds())));

            serializeWorkerStatus(status, response);

            return grpc::Status::OK;
        },
        context);
}

}
