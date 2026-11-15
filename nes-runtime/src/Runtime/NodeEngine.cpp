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

#include <Runtime/NodeEngine.hpp>

#include <chrono>
#include <exception>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Runtime/BufferManager.hpp>

#include <Util/Logger/Logger.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngine.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>

namespace NES
{

NodeEngine::~NodeEngine()
{
    NES_DEBUG("Shutting down NodeEngine");
    queryEngine.reset();
    sourceProvider.reset();

    bufferManager->destroy();
    bufferManager.reset();
}

NodeEngine::NodeEngine(
    std::shared_ptr<BufferManager> bufferManager,
    std::shared_ptr<SystemEventListener> systemEventListener,
    std::shared_ptr<QueryLog> queryLog,
    std::unique_ptr<QueryEngine> queryEngine,
    std::unique_ptr<SourceProvider> sourceProvider)
    : bufferManager(std::move(bufferManager))
    , queryLog(std::move(queryLog))
    , systemEventListener(std::move(systemEventListener))
    , queryEngine(std::move(queryEngine))
    , sourceProvider(std::move(sourceProvider))
{
}

void NodeEngine::startQuery(QueryId queryId, std::unique_ptr<CompiledQueryPlan> compiledQueryPlan)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    PRECONDITION(queryId == compiledQueryPlan->queryId, "Compiled plan must have the submitted query ID");
    std::vector<std::unique_ptr<CompiledQueryPlan>> plans;
    plans.push_back(std::move(compiledQueryPlan));
    startQueries(prepareQueries(std::move(plans)));
}

std::vector<std::unique_ptr<ExecutableQueryPlan>>
NodeEngine::prepareQueries(std::vector<std::unique_ptr<CompiledQueryPlan>> compiledQueryPlans)
{
    std::vector<std::unique_ptr<ExecutableQueryPlan>> plans;
    std::unordered_set<QueryId> ids;
    for (const auto& compiled : compiledQueryPlans)
    {
        PRECONDITION(compiled && compiled->queryId != INVALID_QUERY_ID, "A deployment requires valid query plans");
        if (!ids.insert(compiled->queryId).second)
        {
            throw QueryAlreadyRegistered("Duplicate local query ID in deployment: {}", compiled->queryId);
        }
        plans.push_back(ExecutableQueryPlan::instantiate(*compiled, *sourceProvider));
    }
    return plans;
}

void NodeEngine::startQueries(std::vector<std::unique_ptr<ExecutableQueryPlan>> plans)
{
    std::vector<QueryId> started;
    started.reserve(plans.size());
    try
    {
        for (auto& plan : plans)
        {
            const auto queryId = plan->queryId;
            queryLog->logQueryStatusChange(queryId, QueryStatus::Registered, std::chrono::system_clock::now());
            systemEventListener->onEvent(StartQuerySystemEvent(queryId));
            queryEngine->start(std::move(plan));
            started.push_back(queryId);
        }
    }
    catch (...) /// NOLINT(no-raw-catch-all) roll back the batch without replacing the original start failure
    {
        for (const auto& queryId : started)
        {
            CPPTRACE_TRY
            {
                stopQuery(queryId);
            }
            CPPTRACE_CATCH(const std::exception& exception)
            {
                NES_WARNING("Could not stop query {} during deployment rollback: {}", queryId, exception.what());
            }
        }
        throw;
    }
}

void NodeEngine::stopQuery(QueryId queryId)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    NES_INFO("Stop {}", queryId);
    systemEventListener->onEvent(StopQuerySystemEvent(queryId));
    queryEngine->stop(queryId);
}

}
