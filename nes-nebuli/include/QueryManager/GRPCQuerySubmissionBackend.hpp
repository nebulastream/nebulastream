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

#include <memory>
#include <unordered_map>
#include <vector>

#include <Listeners/QueryLog.hpp>
#include <QueryManager/QueryManager.hpp>
#include <DistributedQueryId.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <WorkerConfig.hpp>

namespace NES
{
class GRPCQuerySubmissionBackend final : public QuerySubmissionBackend
{
    std::unique_ptr<WorkerRPCService::Stub> stub;
    WorkerConfig workerConfig;

public:
    explicit GRPCQuerySubmissionBackend(WorkerConfig config);
    [[nodiscard]] std::expected<LocalQueryId, Exception> registerQuery(LogicalPlan) override;
    std::expected<void, Exception> start(LocalQueryId) override;
    std::expected<void, Exception> stop(LocalQueryId) override;
    std::expected<void, Exception> unregister(LocalQueryId) override;
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(LocalQueryId) const override;
    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const override;
};

BackendProvider createGRPCBackend();

}
