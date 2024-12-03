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

#pragma once
#include <unordered_map>
#include <Exceptions/RuntimeException.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include "Sources/AsyncSourceExecutor.hpp"

namespace NES::Runtime
{
/// Forward declaration of QueryManager, which includes Task, which includes SinkMedium, which includes NodeEngine
class QueryManager;

namespace Execution
{
/// Forward declaration of ExecutableQueryPlan, which includes SinkMedium, which includes NodeEngine
class ExecutableQueryPlan;
using ExecutableQueryPlanPtr = std::shared_ptr<ExecutableQueryPlan>;
}
/**
 * @brief this class represents the interface and entrance point into the
 * query processing part of NES. It provides basic functionality
 * such as registering, unregistering, starting, and stopping.
 */
class NodeEngine
{
    friend class NodeEngineBuilder;

public:
    NodeEngine() = delete;
    NodeEngine(const NodeEngine&) = delete;
    NodeEngine& operator=(const NodeEngine&) = delete;

    explicit NodeEngine(
        const std::shared_ptr<Memory::BufferManager>&, const std::shared_ptr<QueryManager>&, const std::shared_ptr<QueryLog>&);

    [[nodiscard]] QueryId registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan);
    void unregisterQuery(QueryId queryId);
    void startQuery(QueryId queryId);
    /// Termination will happen asynchronously, thus the query might very well be running for an indeterminate time after this method has
    /// been called.
    void stopQuery(QueryId queryId, QueryTerminationType terminationType = QueryTerminationType::HardStop);

    [[nodiscard]] std::shared_ptr<Memory::BufferManager> getBufferManager() { return bufferManager; }
    [[nodiscard]] std::shared_ptr<QueryManager> getQueryManager() { return queryManager; }
    [[nodiscard]] std::shared_ptr<QueryLog> getQueryLog() { return queryLog; }
    [[nodiscard]] std::shared_ptr<Sources::AsyncSourceExecutor> getAsyncSourceExecutor() { return sourceExecutor; }

private:
    std::shared_ptr<Memory::BufferManager> bufferManager;
    std::shared_ptr<QueryManager> queryManager;
    std::shared_ptr<Sources::AsyncSourceExecutor> sourceExecutor;
    std::shared_ptr<QueryLog> queryLog;

    std::unordered_map<QueryId, Execution::ExecutableQueryPlanPtr> registeredQueries;
};
using NodeEnginePtr = std::shared_ptr<NodeEngine>;
}
