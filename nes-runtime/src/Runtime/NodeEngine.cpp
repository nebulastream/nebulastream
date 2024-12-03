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

#include <utility>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/AsyncSourceExecutor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Runtime
{

NodeEngine::NodeEngine(
    const std::shared_ptr<Memory::BufferManager>& bufferManager,
    const std::shared_ptr<QueryManager>& queryManager,
    const std::shared_ptr<QueryLog>& queryLog)
    : bufferManager(bufferManager)
    , queryManager(queryManager)
    , sourceExecutor(std::make_shared<Sources::AsyncSourceExecutor>(1))
    , queryLog(queryLog)
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
    if (const auto query = registeredQueries.find(queryId); query != registeredQueries.end() and query->second)
    {
        if (!queryManager->startQuery(query->second))
        {
            throw CannotStartQuery("Cannot start query with id '{}'.", queryId);
        }
    }
    else
    {
        throw QueryNotRegistered("Cannot find query with id '{}' in registered queries.", queryId);
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

}
