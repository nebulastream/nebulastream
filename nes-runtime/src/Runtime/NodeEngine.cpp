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
#include <memory>
#include <utility>
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
    std::lock_guard lock(nodeEngineMutex);
    auto status = getQueryLog()->getQueryStatus(queryId);
    if (status.has_value() && status.value().state != QueryStatus::Stopped && status.value().state != QueryStatus::Stopped)
    {
        throw std::unexpected{QueryAlreadyRegistered("{}", queryId)};
    }
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    queryLog->logQueryStatusChange(queryId, QueryStatus::Registered, std::chrono::system_clock::now());
    systemEventListener->onEvent(StartQuerySystemEvent(std::move(queryId)));
    queryEngine->start(ExecutableQueryPlan::instantiate(*compiledQueryPlan, *sourceProvider));
}

void NodeEngine::stopQuery(QueryId queryId, bool graceful)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    NES_INFO("Stop {}", queryId);
    systemEventListener->onEvent(StopQuerySystemEvent(queryId));
    queryEngine->stop(queryId, graceful);
}

}
