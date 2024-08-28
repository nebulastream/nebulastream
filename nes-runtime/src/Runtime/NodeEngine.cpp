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

#include <string>
#include <utility>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime
{

NodeEngine::NodeEngine(
    std::vector<std::shared_ptr<Memory::BufferManager>>&& bufferManagers,
    std::shared_ptr<QueryManager>&& queryManager,
    std::shared_ptr<QueryLog>&& queryLog)
    : bufferManagers(std::move(bufferManagers)), queryManager(std::move(queryManager)), queryLog(std::move(queryLog))
{
    this->queryManager->startThreadPool(100);
}

QueryId NodeEngine::registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan)
{
    auto queryId = queryExecutionPlan->getQueryId();
    queryManager->registerQuery(queryExecutionPlan);

    registeredQueries[queryId] = queryExecutionPlan;
    return queryId;
}

void NodeEngine::startQuery(QueryId queryId)
{
    if (!queryManager->startQuery(registeredQueries[queryId]))
    {
        NES_THROW_RUNTIME_ERROR("Could not start the query");
    }
}

void NodeEngine::unregisterQuery(QueryId queryId)
{
    registeredQueries.erase(queryId);
}

void NodeEngine::stopQuery(QueryId queryId, QueryTerminationType type)
{
    if (!queryManager->stopQuery(registeredQueries[queryId], type))
    {
        NES_THROW_RUNTIME_ERROR("Could not stop the query");
    }
}

} /// namespace NES::Runtime
