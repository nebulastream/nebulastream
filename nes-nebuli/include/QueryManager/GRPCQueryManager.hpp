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

#include <QueryManager/QueryManager.hpp>

#include <cstddef>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <grpcpp/client_context.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>

namespace NES
{
class GRPCQueryManager final : public QueryManager
{
    std::unique_ptr<WorkerRPCService::Stub> stub;

public:
    explicit GRPCQueryManager(const std::shared_ptr<grpc::Channel>& channel);
    std::expected<QueryId, Exception> registerQuery(const LogicalPlan& plan) noexcept override;
    std::expected<void, Exception> stop(QueryId queryId) noexcept override;
    std::expected<void, Exception> start(QueryId queryId) noexcept override;
    std::expected<void, Exception> stopAll() noexcept override;
    std::expected<void, Exception> unregister(QueryId queryId) noexcept override;
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(QueryId queryId) const noexcept override;
};
}
