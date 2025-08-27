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
#include <DistributedQueryId.hpp>
#include <QueryManager/QueryManager.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <QueryPlanning.hpp>

namespace NES
{
class EmbeddedWorkerQueryManager final : public QueryManager
{
public:
    explicit EmbeddedWorkerQueryManager(const SingleNodeWorkerConfiguration& configuration);
    [[nodiscard]] std::expected<Query, Exception>
    registerQuery(const PlanStage::DistributedLogicalPlan& plan) override;
    std::expected<void, Exception> start(const Query& query) override;
    std::expected<void, std::vector<Exception>> stop(const Query& query) override;
    std::expected<void, std::vector<Exception>> unregister(const Query& query) override;
    [[nodiscard]] std::expected<DistributedQueryStatus, std::vector<Exception>> status(const Query& query) const override;

private:
    SingleNodeWorker worker;
};
}
