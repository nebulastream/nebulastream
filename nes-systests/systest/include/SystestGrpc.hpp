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
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>

/// TODO #460: We should use on GRPC client class for nebuli and systests
class GRPCClient
{
public:
    explicit GRPCClient(std::shared_ptr<grpc::Channel> channel);
    std::unique_ptr<WorkerRPCService::Stub> stub;

    [[nodiscard]] size_t registerQuery(const NES::LogicalPlan& queryPlan) const;
    void start(size_t queryId) const;
    [[nodiscard]] NES::QuerySummary status(size_t queryId) const;
    void unregister(size_t queryId) const;
};
