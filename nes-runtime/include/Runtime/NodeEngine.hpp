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
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <CompiledQueryPlan.hpp>
#include <QueryEngine.hpp>

namespace NES::Runtime
{
/// Forward declaration of QueryEngineTest, which includes Task, which includes SinkMedium, which includes NodeEngine
class QueryTracker;

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
    ~NodeEngine();

    NodeEngine(
        std::shared_ptr<Memory::BufferManager> bufferManager,
        std::shared_ptr<SystemEventListener> systemEventListener,
        std::shared_ptr<QueryLog> queryLog,
        std::unique_ptr<QueryEngine> queryEngine,
        int numberOfBuffersInSourceLocalBufferPool);

    [[nodiscard]] QueryId registerExecutableQueryPlan(std::unique_ptr<Execution::CompiledQueryPlan> queryExecutionPlan);
    void unregisterQuery(QueryId queryId);
    void startQuery(QueryId queryId);
    /// Termination will happen asynchronously, thus the query might very well be running for an indeterminate time after this method has
    /// been called.
    void stopQuery(QueryId queryId, QueryTerminationType terminationType);

    [[nodiscard]] std::shared_ptr<Memory::BufferManager> getBufferManager() { return bufferManager; }

    [[nodiscard]] std::shared_ptr<QueryLog> getQueryLog() { return queryLog; }

private:
    std::shared_ptr<Memory::BufferManager> bufferManager;
    std::shared_ptr<QueryLog> queryLog;

    std::shared_ptr<SystemEventListener> systemEventListener;
    std::unique_ptr<QueryEngine> queryEngine;
    std::unique_ptr<QueryTracker> queryTracker;
    int numberOfBuffersInSourceLocalBufferPool;
};
}
