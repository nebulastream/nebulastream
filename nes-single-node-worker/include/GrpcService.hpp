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
#include <functional>
#include <utility>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>
#include <Thread.hpp>

namespace NES
{
/**
 * @brief GRPC Interface to interact with the SingleNodeWorker. It handles deserialization of requests and delegates them to the
 * @link SingleNodeWorker.
 */
class GRPCServer final : public WorkerRPCService::Service
{
public:
    grpc::Status StartQuery(grpc::ServerContext*, const StartQueryRequest*, StartQueryReply*) override;

    grpc::Status StopQuery(grpc::ServerContext*, const StopQueryRequest*, google::protobuf::Empty*) override;

    grpc::Status RequestQueryStatus(grpc::ServerContext*, const QueryStatusRequest*, QueryStatusReply*) override;

    grpc::Status RequestStatus(grpc::ServerContext* context, const WorkerStatusRequest* request, WorkerStatusResponse* response) override;

    grpc::Status RequestVersion(grpc::ServerContext* context, const google::protobuf::Empty* request, VersionResponse* response) override;

    /// Constructed on the worker's main thread, whose logging identity is captured so that the
    /// gRPC-owned handler threads can inherit it (see tryWithDefaultHandling).
    explicit GRPCServer(SingleNodeWorker&& delegate) : delegate(std::move(delegate)), workerHost(Thread::getThisWorkerNodeId()) { }

private:
    /// Runs one RPC body with the shared error handling, after labelling the calling thread.
    grpc::Status tryWithDefaultHandling(const std::function<grpc::Status()>& body, grpc::ServerContext* context) const;

    SingleNodeWorker delegate;
    Host workerHost;
};
}
