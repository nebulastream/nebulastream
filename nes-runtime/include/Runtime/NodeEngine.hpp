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
#include <vector>
#include <Exceptions/RuntimeException.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Runtime
{
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

    explicit NodeEngine(std::vector<Memory::BufferManagerPtr>&&, QueryManagerPtr&&, QueryLogPtr&&);

    [[nodiscard]] QueryId registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan);
    void unregisterQuery(QueryId queryId);
    void startQuery(QueryId queryId);
    /// Termination will happen asynchronously, thus the query might very well be running for an indeterminate time after this method has
    /// been called.
    void stopQuery(QueryId queryId, QueryTerminationType terminationType = QueryTerminationType::HardStop);

    [[nodiscard]] Memory::BufferManagerPtr getBufferManager() { return bufferManagers[0]; }
    [[nodiscard]] QueryManagerPtr getQueryManager() { return queryManager; }
    [[nodiscard]] QueryLogPtr getQueryLog() { return queryLog; }

private:
    std::vector<Memory::BufferManagerPtr> bufferManagers;
    std::unordered_map<QueryId, Execution::ExecutableQueryPlanPtr> registeredQueries;
    QueryManagerPtr queryManager;
    QueryLogPtr queryLog;
};
using NodeEnginePtr = std::shared_ptr<NodeEngine>;
} /// namespace NES::Runtime
