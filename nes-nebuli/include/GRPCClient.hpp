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
#include <cstddef>
#include <memory>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <grpcpp/client_context.h>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>

class GRPCClient
{
    std::unique_ptr<WorkerRPCService::Stub> stub;

public:
    explicit GRPCClient(const std::shared_ptr<grpc::Channel>& channel);

    [[nodiscard]] size_t registerQuery(const NES::DecomposedQueryPlan& plan) const;

    void stop(size_t queryId) const;

    [[nodiscard]] QuerySummaryReply status(size_t queryId) const;

    void start(size_t queryId) const;

    void unregister(size_t queryId) const;
};
