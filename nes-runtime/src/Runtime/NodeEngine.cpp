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
#include <unordered_map>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/AtomicState.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngine.hpp>

namespace NES
{

class QueryTracker
{
    struct Idle
    {
        std::unique_ptr<CompiledQueryPlan> qep;
    };

    struct Executing
    {
    };

    using QueryState = AtomicState<Idle, Executing>;
    folly::Synchronized<std::unordered_map<QueryId, std::unique_ptr<QueryState>>> queries;

public:
    QueryId registerQuery(std::unique_ptr<CompiledQueryPlan> qep)
    {
        NES_INFO("Register {}", qep->queryId);
        QueryId queryId = qep->queryId;
        queries.wlock()->emplace(queryId, std::make_unique<QueryState>(Idle{std::move(qep)}));
        return queryId;
    }

    std::unique_ptr<CompiledQueryPlan> moveToExecuting(QueryId qid)
    {
        auto rlocked = queries.rlock();
        std::unique_ptr<CompiledQueryPlan> qep;
        if (auto it = rlocked->find(qid); it != rlocked->end())
        {
            it->second->transition(
                [&](Idle&& idle)
                {
                    qep = std::move(idle).qep;
                    return Executing{};
                });
        }
        return qep;
    }

    std::vector<QueryId> getAllQueryIds() const
    {
        auto rlocked = queries.rlock();
        std::vector<QueryId> ids;
        ids.reserve(rlocked->size());
        for (const auto& [id, state] : *rlocked)
        {
            ids.push_back(id);
        }
        return ids;
    }
};

NodeEngine::~NodeEngine()
{
    queryEngine.reset();
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
    , queryTracker(std::make_unique<QueryTracker>())
    , sourceProvider(std::move(sourceProvider))
{
}

QueryId NodeEngine::registerCompiledQueryPlan(std::unique_ptr<CompiledQueryPlan> compiledQueryPlan)
{
    auto queryId = queryTracker->registerQuery(std::move(compiledQueryPlan));
    queryLog->logQueryStatusChange(queryId, QueryState::Registered, std::chrono::system_clock::now());
    return queryId;
}

void NodeEngine::startQuery(QueryId queryId)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");

    if (auto qep = queryTracker->moveToExecuting(queryId))
    {
        systemEventListener->onEvent(StartQuerySystemEvent(queryId));
        queryEngine->start(ExecutableQueryPlan::instantiate(*qep, *sourceProvider));
    }
    else
    {
        throw QueryNotRegistered("Query with queryId {} is not currently idle", queryId);
    }
}

void NodeEngine::unregisterQuery(QueryId queryId)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    NES_INFO("Unregister {}", queryId);
    queryEngine->stop(queryId);
}

void NodeEngine::stopQuery(QueryId queryId, QueryTerminationType)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    NES_INFO("Stop {}", queryId);
    systemEventListener->onEvent(StopQuerySystemEvent(queryId));
    queryEngine->stop(queryId);
}

void NodeEngine::stopAllQueries(QueryTerminationType terminationType)
{
    NES_INFO("NodeEngine: Stopping all queries with type {}", magic_enum::enum_name(terminationType));

    std::vector<QueryId> runningQueries = queryTracker->getAllQueryIds();

    for (const auto& qId : runningQueries)
    {
        CPPTRACE_TRY
        {
            stopQuery(qId, terminationType);
        }
        CPPTRACE_CATCH(...)
        {
            NES_ERROR("Failed to stop query {} during stopAll", qId);
        }
    }
}

}
