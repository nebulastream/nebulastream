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

#include <chrono>
#include <memory>
#include <unordered_map>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/AtomicState.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngine.hpp>

namespace NES::Runtime
{

class QueryTracker
{
    struct Idle
    {
        std::unique_ptr<Execution::CompiledQueryPlan> qep;
    };

    struct Executing
    {
    };

    using QueryState = AtomicState<Idle, Executing>;
    folly::Synchronized<std::unordered_map<QueryId, std::unique_ptr<QueryState>>> queries;

public:
    QueryId registerQuery(std::unique_ptr<Execution::CompiledQueryPlan> qep)
    {
        NES_INFO("Register {}", qep->queryId);
        QueryId queryId = qep->queryId;
        queries.wlock()->emplace(queryId, std::make_unique<QueryState>(Idle{std::move(qep)}));
        return queryId;
    }

    std::unique_ptr<Execution::CompiledQueryPlan> moveToExecuting(QueryId qid)
    {
        auto rlocked = queries.rlock();
        std::unique_ptr<Execution::CompiledQueryPlan> qep;
        if (auto it = rlocked->find(qid); it != rlocked->end())
        {
            it->second->transition(
                [&](Idle&& idle)
                {
                    qep = std::move(std::move(idle).qep);
                    return Executing{};
                });
        }
        return qep;
    }
};

NodeEngine::~NodeEngine() = default;

NodeEngine::NodeEngine(
    std::shared_ptr<Memory::BufferManager> bufferManager,
    std::shared_ptr<SystemEventListener> systemEventListener,
    std::shared_ptr<QueryLog> queryLog,
    std::unique_ptr<QueryEngine> queryEngine,
    int numberOfBuffersInSourceLocalBufferPool)
    : bufferManager(std::move(bufferManager))
    , queryLog(std::move(queryLog))
    , systemEventListener(std::move(systemEventListener))
    , queryEngine(std::move(queryEngine))
    , queryTracker(std::make_unique<QueryTracker>())
    , numberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool)
{
}

QueryId NodeEngine::registerExecutableQueryPlan(std::unique_ptr<Execution::CompiledQueryPlan> queryExecutionPlan)
{
    auto queryId = queryTracker->registerQuery(std::move(queryExecutionPlan));
    queryLog->logQueryStatusChange(queryId, Execution::QueryStatus::Registered, std::chrono::system_clock::now());
    return queryId;
}

void NodeEngine::startQuery(QueryId queryId)
{
    if (auto qep = queryTracker->moveToExecuting(queryId))
    {
        systemEventListener->onEvent(StartQuerySystemEvent(queryId));
        queryEngine->start(ExecutableQueryPlan::instantiate(*qep, bufferManager, numberOfBuffersInSourceLocalBufferPool));
    }
    else
    {
        throw QueryNotRegistered("Query with queryId {} is not currently idle", queryId);
    }
}

void NodeEngine::unregisterQuery(QueryId queryId)
{
    NES_INFO("Unregister {}", queryId);
    queryEngine->stop(queryId);
}

void NodeEngine::stopQuery(QueryId queryId, QueryTerminationType)
{
    NES_INFO("Stop {}", queryId);
    systemEventListener->onEvent(StopQuerySystemEvent(queryId));
    queryEngine->stop(queryId);
}

}
