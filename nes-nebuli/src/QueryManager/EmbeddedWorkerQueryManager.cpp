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

#include <QueryManager/EmbeddedWorkerQueryManager.hpp>

#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <DistributedQueryId.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{

EmbeddedWorkerQueryManager::EmbeddedWorkerQueryManager(const SingleNodeWorkerConfiguration& configuration) : worker(configuration)
{
}

std::expected<Query, Exception> EmbeddedWorkerQueryManager::registerQuery(const PlanStage::DistributedLogicalPlan& plan)
{
    return worker.registerQuery(*plan);
}

std::expected<void, Exception> EmbeddedWorkerQueryManager::start(const Query& query)
{
    return worker.startQuery(LocalQueryId{query.getEmbeddedId()});
}

std::expected<void, std::vector<Exception>> EmbeddedWorkerQueryManager::stop(const Query& query)
{
    return worker.stopQuery(LocalQueryId{query.getEmbeddedId()}, QueryTerminationType::Graceful)
        .transform_error([](auto&& exception) { return std::vector{exception}; });
}

std::expected<void, std::vector<Exception>> EmbeddedWorkerQueryManager::unregister(const Query& query)
{
    return worker.unregisterQuery(LocalQueryId{query.getEmbeddedId()}).transform_error([](auto&& exception) { return std::vector{exception}; });
}

std::expected<DistributedQueryStatus, std::vector<Exception>> EmbeddedWorkerQueryManager::status(const Query& query) const
{
    auto localStatus = worker.getLocalStatusForQuery(LocalQueryId{query.getEmbeddedId()});
    return localStatus.transform([](auto&& status) { return DistributedQueryStatus{.localStatusSnapshots = {status}}; })
        .transform_error([](auto&& exception) { return std::vector{exception}; });
}

}
