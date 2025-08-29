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

#include <vector>

#include <Listeners/QueryLog.hpp>
#include <QueryManager/QueryManager.hpp>
#include <DistributedQueryId.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{
class EmbeddedWorkerQuerySubmissionBackend final : public QuerySubmissionBackend
{
public:
    EmbeddedWorkerQuerySubmissionBackend(std::vector<std::pair<WorkerConfig, SingleNodeWorkerConfiguration>> workers);
    EmbeddedWorkerQuerySubmissionBackend(std::vector<WorkerConfig> worker, const SingleNodeWorkerConfiguration& globalConfig);
    [[nodiscard]] std::expected<LocalQueryId, Exception> registerQuery(const GrpcAddr& grpc, LogicalPlan) override;
    std::expected<void, Exception> start(const LocalQuery&) override;
    std::expected<void, Exception> stop(const LocalQuery&) override;
    std::expected<void, Exception> unregister(const LocalQuery&) override;
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(const LocalQuery&) const override;

private:
    SingleNodeWorker& getWorker(const GrpcAddr& grpc);
    const SingleNodeWorker& getWorker(const GrpcAddr& grpc) const;
    using Cluster = std::unordered_map<GrpcAddr, SingleNodeWorker>;
    Cluster cluster;
};
}
