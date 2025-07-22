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

#pragma once
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>

namespace NES
{
/**
 * @brief GRPC Interface to interact with the SingleNodeWorker. It handles deserialization of requests and delegates them to the
 * @link SingleNodeWorker.
 */
class GRPCServer final : public WorkerRPCService::Service
{
public:
    grpc::Status RegisterQuery(grpc::ServerContext*, const RegisterQueryRequest*, RegisterQueryReply*) override;

    grpc::Status UnregisterQuery(grpc::ServerContext*, const UnregisterQueryRequest*, google::protobuf::Empty*) override;

    grpc::Status StartQuery(grpc::ServerContext*, const StartQueryRequest*, google::protobuf::Empty*) override;

    grpc::Status StopQuery(grpc::ServerContext*, const StopQueryRequest*, google::protobuf::Empty*) override;

    grpc::Status RequestQuerySummary(grpc::ServerContext*, const QuerySummaryRequest*, QuerySummaryReply*) override;

    grpc::Status RequestQueryLog(grpc::ServerContext* context, const QueryLogRequest* request, QueryLogReply* response) override;

    grpc::Status RequestStatus(grpc::ServerContext* context, const WorkerStatusRequest* request, WorkerStatusResponse* response) override;

    explicit GRPCServer(SingleNodeWorker&& delegate) : delegate(std::move(delegate)) { }

private:
    SingleNodeWorker delegate;
};
}
