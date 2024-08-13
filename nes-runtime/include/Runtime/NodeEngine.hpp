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
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Runtime
{

class QueryStartException : NES::Exceptions::RuntimeException
{
};

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

    /**
     * @brief registers a new ExecutableQueryPlan
     * @param queryExecutionPlan plan to register
     * @throws QueryRegistrationException
     * @return QueryId of the newly registered Query
     */
    [[nodiscard]] QueryId registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan);

    /**
     * @brief unregisters a stopped query
     * @param QueryId unregister query
     * @throws QueryUnregistrationException if the query could not be unregistered
     * @return true if succeeded, else false
     */
    void unregisterQuery(QueryId queryId);

    /**
     * @brief method to start a registered query
     * @param queryId identifies the target query
     * @throws QueryStartException of the query could not be started
     */
    void startQuery(QueryId queryId);

    /**
     * @brief method to initiate a query stop
     * @note Termination will happen asynchronously, thus the query might very well be running for an indeterminate time after
     * this method has returned
     * @param queryId identifies the target query
     * @param terminationType termination type. Read more @link Runtime::QueryTerminationType
     */
    void stopQuery(QueryId queryId, QueryTerminationType terminationType = QueryTerminationType::HardStop);

    BufferManagerPtr getBufferManager() { return bufferManagers[0]; }
    QueryManagerPtr getQueryManager() { return queryManager; }

    /**
     * @brief Create a node engine and gather node information
     * and initialize QueryManager, BufferManager and ThreadPool
     */
    explicit NodeEngine(std::vector<BufferManagerPtr>&&, QueryManagerPtr&&);

private:
    std::vector<BufferManagerPtr> bufferManagers;
    std::unordered_map<QueryId, Execution::ExecutableQueryPlanPtr> registeredQueries;
    QueryManagerPtr queryManager;
};

using NodeEnginePtr = std::shared_ptr<NodeEngine>;

} /// namespace NES::Runtime
