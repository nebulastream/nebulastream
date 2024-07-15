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

#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>

#include <string>
#include <utility>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime
{

NodeEngine::NodeEngine(std::vector<BufferManagerPtr>&& bufferManagers, QueryManagerPtr&& queryManager)
    : bufferManagers(std::move(bufferManagers)), queryManager(std::move(queryManager))
{
    this->queryManager->startThreadPool(100);
}

QueryId NodeEngine::registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan)
{
    // TODO(#123): Query Instantiation
    queryManager->registerQuery(queryExecutionPlan);
    return QueryId(1);
}

void NodeEngine::startQuery(QueryId)
{
    NES_NOT_IMPLEMENTED();
}

void NodeEngine::unregisterQuery(QueryId)
{
    NES_NOT_IMPLEMENTED();
}

void NodeEngine::stopQuery(QueryId, QueryTerminationType)
{
    NES_NOT_IMPLEMENTED();
}

} // namespace NES::Runtime
