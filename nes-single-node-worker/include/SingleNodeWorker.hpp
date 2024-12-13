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

#include <Listeners/QueryLog.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{
namespace Runtime
{
struct PrintingStatisticListener;
class NodeEngine;
}
namespace QueryCompilation
{
class QueryCompiler;
}

/**
 * @brief The SingleNodeWorker is a compiling StreamProcessingEngine, working alone on local sources and sinks, without external
 * coordination. The SingleNodeWorker can register LogicalQueryPlans which are lowered into an executable format, by the
 * QueryCompiler. The user can manage the lifecycle of queries inside the NodeEngine using the SingleNodeWorkers interface.
 * The Class itself is NonCopyable, but Movable, it owns the QueryCompiler and the NodeEngine.
 */
class SingleNodeWorker
{
    std::unique_ptr<QueryCompilation::QueryCompiler> qc;
    std::shared_ptr<Runtime::PrintingStatisticListener> listener;
    std::shared_ptr<Runtime::NodeEngine> nodeEngine;
    size_t bufferSize;

public:
    explicit SingleNodeWorker(const Configuration::SingleNodeWorkerConfiguration&);
    ~SingleNodeWorker();
    /// Non-Copyable
    SingleNodeWorker(const SingleNodeWorker& other) = delete;
    SingleNodeWorker& operator=(const SingleNodeWorker& other) = delete;

    /// Movable
    SingleNodeWorker(SingleNodeWorker&& other) noexcept;
    SingleNodeWorker& operator=(SingleNodeWorker&& other) noexcept;

    /**
     * Registers a DecomposedQueryPlan which internally triggers the QueryCompiler and registers the executable query plan. Once
     * returned the query can be started with the QueryId. The registered Query will be in the StoppedState
     * @param plan Fully Specified LogicalQueryPlan.
     * @throws RuntimeException the QueryCompilation fails
     * @return QueryId which identifies the registered Query
     */
    QueryId registerQuery(DecomposedQueryPlanPtr plan);

    /**
     * Starts the Query asynchronously and moves it into the RunningState. Query execution error are only reported during runtime
     * of the query.
     * @param queryId identifies the registered query
     */
    void startQuery(QueryId queryId);

    /**
     * Stops the Query and moves it into the StoppedState. The exact semantics and guarantees depend on the chosen
     * QueryTerminationType
     * @param queryId identifies the registered query
     * @param terminationType dictates what happens with in in-flight data
     */
    void stopQuery(QueryId queryId, Runtime::QueryTerminationType terminationType);

    /**
     * Unregisters a stopped Query.
     * @throws RuntimeException if Query is still running.
     * @param queryId identifies the registered stopped query
     */
    void unregisterQuery(QueryId queryId);

    /// Complete history of query status changes.
    [[nodiscard]] std::optional<Runtime::QueryLog::Log> getQueryLog(QueryId queryId) const;
    /// Summary sturcture for query.
    [[nodiscard]] std::optional<Runtime::QuerySummary> getQuerySummary(QueryId queryId) const;
};
}
