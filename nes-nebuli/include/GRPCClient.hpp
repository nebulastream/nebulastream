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
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <grpcpp/client_context.h>
#include <SingleNodeWorkerRPCService.grpc.pb.h>

namespace NES
{
class GRPCClient
{
    std::unique_ptr<WorkerRPCService::Stub> stub;

public:
    explicit GRPCClient(const std::shared_ptr<grpc::Channel>& channel);
    [[nodiscard]] QueryId registerQuery(const NES::LogicalPlan& plan) const;
    void stop(QueryId queryId) const;

    [[nodiscard]] NES::QuerySummary status(QueryId queryId) const;
    void start(QueryId queryId) const;
    void unregister(QueryId queryId) const;
};
}
