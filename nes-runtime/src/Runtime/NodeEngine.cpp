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

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Util/AtomicState.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <ExecutableQueryPlan.hpp>

namespace NES::Runtime
{

class QueryTracker
{
    struct Idle
    {
        std::unique_ptr<Execution::ExecutableQueryPlan> qep;
    };
    struct Executing
    {
    };
    std::atomic<QueryId::Underlying> queryIdGenerator = QueryId::INITIAL;
    using QueryState = AtomicState<Idle, Executing>;
    folly::Synchronized<std::unordered_map<QueryId, std::unique_ptr<QueryState>>> queries;

public:
    QueryId registerQuery(std::unique_ptr<Execution::ExecutableQueryPlan> qep)
    {
        QueryId qid = QueryId(queryIdGenerator++);
        queries.wlock()->emplace(qid, std::make_unique<QueryState>(Idle{std::move(qep)}));
        return qid;
    }

    std::unique_ptr<Execution::ExecutableQueryPlan> moveToExecuting(QueryId qid)
    {
        auto rlocked = queries.rlock();
        std::unique_ptr<Execution::ExecutableQueryPlan> qep;
        if (auto it = rlocked->find(qid); it != rlocked->end())
        {
            it->second->transition(
                [&](Idle&& idle)
                {
                    qep = std::move(idle.qep);
                    return Executing{};
                });
        }
        return qep;
    }
};

NodeEngine::~NodeEngine()
{
}

NodeEngine::NodeEngine(
    std::shared_ptr<Sources::SourceProvider> source_provider,
    std::shared_ptr<Memory::BufferManager> buffer_manager,
    std::shared_ptr<QueryLog> query_log,
    std::unique_ptr<QueryEngine> query_manager)
    : sourceProvider(std::move(source_provider))
    , bufferManager(std::move(buffer_manager))
    , queryLog(std::move(query_log))
    , queryManager(std::move(query_manager))
    , queryTracker(std::make_unique<QueryTracker>())
{
}

QueryId NodeEngine::registerExecutableQueryPlan(std::unique_ptr<Execution::ExecutableQueryPlan> queryExecutionPlan)
{
    auto queryId = queryTracker->registerQuery(std::move(queryExecutionPlan));
    queryLog->logQueryStatusChange(queryId, Execution::QueryStatus::Registered);
    return queryId;
}

void NodeEngine::startQuery(QueryId queryId)
{
    if (auto qep = queryTracker->moveToExecuting(queryId))
    {
        queryManager->start(queryId, InstantiatedQueryPlan::instantiate(std::move(qep), *sourceProvider, bufferManager));
    }
    else
    {
        throw QueryNotFound("Query with queryId {} is not currently idle", queryId);
    }
}

void NodeEngine::unregisterQuery(QueryId queryId)
{
    NES_INFO("Unregister {}", queryId);
    queryManager->stop(queryId);
}

void NodeEngine::stopQuery(QueryId queryId, QueryTerminationType)
{
    NES_INFO("Stop {}", queryId);
    queryManager->stop(queryId);
}

}
