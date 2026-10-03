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

#include <ReferenceNodeEngine.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>

namespace NES
{
ReferenceNodeEngine::ReferenceNodeEngine(
    const WorkerConfiguration& configuration, std::shared_ptr<StatisticListener> statisticsListener, const Host& host)
    : bufferManager(BufferManager::create(
          configuration.totalMemoryInBytes.getValue(),
          configuration.unpooledMemoryFraction.getValue(),
          BufferAlignment{static_cast<uint32_t>(configuration.bufferAlignmentInBytes.getValue())},
          static_cast<uint32_t>(configuration.defaultQueryExecution.operatorBufferSize.getValue()),
          std::make_shared<NesDefaultMemoryAllocator>()))
    , queryLog(std::make_shared<QueryLog>())
    , systemEventListener(statisticsListener)
    , queryEngine(std::make_unique<ReferenceQueryEngine>(configuration.queryEngine, statisticsListener, queryLog, bufferManager, host))
    , sourceProvider(std::make_unique<SourceProvider>(configuration.defaultMaxInflightBuffers.getValue(), bufferManager))
{
}

ReferenceNodeEngine::~ReferenceNodeEngine()
{
    queryEngine.reset();
    sourceProvider.reset();
    bufferManager->destroy();
    bufferManager.reset();
}

void ReferenceNodeEngine::startQuery(QueryId queryId, std::unique_ptr<CompiledQueryPlan> compiledQueryPlan)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    queryLog->logQueryStatusChange(queryId, QueryStatus::Registered, std::chrono::system_clock::now());
    systemEventListener->onEvent(StartQuerySystemEvent(std::move(queryId)));
    queryEngine->start(ExecutableQueryPlan::instantiate(*compiledQueryPlan, *sourceProvider));
}

void ReferenceNodeEngine::stopQuery(QueryId queryId)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    NES_INFO("Stop {}", queryId);
    systemEventListener->onEvent(StopQuerySystemEvent(queryId));
    queryEngine->stop(queryId);
}
}
