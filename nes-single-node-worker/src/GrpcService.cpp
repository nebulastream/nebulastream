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

#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <GrpcService.hpp>

grpc::Status NES::GRPCServer::RegisterQuery(grpc::ServerContext*, const RegisterQueryRequest* request, RegisterQueryReply* response)
{
    auto fullySpecifiedQueryPlan = DecomposedQueryPlanSerializationUtil::deserializeDecomposedQueryPlan(&request->decomposedqueryplan());
    try
    {
        auto queryId = delegate.registerQuery(fullySpecifiedQueryPlan);
        response->set_queryid(queryId.getRawValue());
        return grpc::Status::OK;
    }
    catch (...)
    {
        return {grpc::StatusCode::UNKNOWN, "This could have been a nice error message, sorry"};
    }
}
grpc::Status NES::GRPCServer::UnregisterQuery(grpc::ServerContext*, const UnregisterQueryRequest* request, google::protobuf::Empty*)
{
    auto queryId = QueryId(request->queryid());
    try
    {
        delegate.unregisterQuery(queryId);
        return grpc::Status::OK;
    }
    catch (...)
    {
        return {grpc::StatusCode::UNKNOWN, "This could have been a nice error message, sorry"};
    }
}
grpc::Status NES::GRPCServer::StartQuery(grpc::ServerContext*, const StartQueryRequest* request, google::protobuf::Empty*)
{
    auto queryId = QueryId(request->queryid());
    try
    {
        delegate.startQuery(queryId);
        return grpc::Status::OK;
    }
    catch (...)
    {
        return {grpc::StatusCode::UNKNOWN, "This could have been a nice error message, sorry"};
    }
}
grpc::Status NES::GRPCServer::StopQuery(grpc::ServerContext*, const StopQueryRequest* request, google::protobuf::Empty*)
{
    auto queryId = QueryId(request->queryid());
    auto terminationType = static_cast<Runtime::QueryTerminationType>(request->terminationtype());
    try
    {
        delegate.stopQuery(queryId, terminationType);
        return grpc::Status::OK;
    }
    catch (...)
    {
        return {grpc::StatusCode::UNKNOWN, "This could have been a nice error message, sorry"};
    }
}

grpc::Status NES::GRPCServer::RequestQuerySummary(grpc::ServerContext*, const QuerySummaryRequest* request, QuerySummaryReply* reply)
{
    try
    {
        auto queryId = QueryId(request->queryid());
        auto summary = delegate.getQuerySummary(queryId);
        if (summary.has_value())
        {
            reply->set_status(QueryStatus(summary->currentStatus));
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
        }
        else
        {
            reply->set_status(QueryStatus::Invalid);
            reply->set_numberofrestarts(0);
            reply->clear_error();
        }
        return grpc::Status::OK;
    }
    catch (...)
    {
        return {grpc::StatusCode::UNKNOWN, "This could have been a nice error message, sorry"};
    }
}

grpc::Status NES::GRPCServer::RequestQueryLog(grpc::ServerContext*, const QueryLogRequest* request, QueryLogReply* reply)
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
                logEntry.set_status((QueryStatus)entry.state);
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
        }
        else
        {
            reply->clear_entries();
        }
        return grpc::Status::OK;
    }
    catch (...)
    {
        return {grpc::StatusCode::UNKNOWN, "This could have been a nice error message, sorry"};
    }
}
