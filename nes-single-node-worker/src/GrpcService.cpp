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
grpc::Status NES::GRPCServer::QueryStatus(grpc::ServerContext*, const QueryStatusRequest*, QueryStatusReply*)
{
    return {grpc::StatusCode::UNIMPLEMENTED, "Query Status is not implemented"};
}
