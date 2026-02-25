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

#include <chrono>
#include <expected>
#include <memory>
#include <optional>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Pointers.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

/// @brief The SingleNodeWorker is a compiling StreamProcessingEngine, working alone on local sources and sinks, without external
/// coordination. The SingleNodeWorker can register LogicalQueryPlans which are lowered into an executable format, by the
/// QueryCompiler. The user can manage the lifecycle of queries inside the NodeEngine using the SingleNodeWorkers interface.
/// The Class itself is NonCopyable, but Movable, it owns the QueryCompiler and the NodeEngine.
class SingleNodeWorker
{
    SharedPtr<CompositeStatisticListener> listener;
    SharedPtr<NodeEngine> nodeEngine;
    UniquePtr<QueryOptimizer> optimizer;
    UniquePtr<QueryCompilation::QueryCompiler> compiler;
    SingleNodeWorkerConfiguration configuration;

public:
    explicit SingleNodeWorker(const SingleNodeWorkerConfiguration&, WorkerId = WorkerId("SingleNodeWorker"));
    ~SingleNodeWorker();
    /// Non-Copyable
    SingleNodeWorker(const SingleNodeWorker& other) = delete;
    SingleNodeWorker& operator=(const SingleNodeWorker& other) = delete;

    /// Movable
    SingleNodeWorker(SingleNodeWorker&& other) noexcept;
    SingleNodeWorker& operator=(SingleNodeWorker&& other) noexcept;

    /// Registers a DecomposedQueryPlan which internally triggers the QueryCompiler and registers the executable query plan. Once
    /// returned the query can be started with the QueryId. The registered Query will be in the StoppedState
    /// @param plan Fully Specified LogicalQueryPlan.
    /// @return QueryId which identifies the registered Query
    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(LogicalPlan plan) noexcept;

    /// Starts the Query asynchronously and moves it into the RunningState. Query execution error are only reported during runtime
    /// of the query.
    /// @param queryId identifies the registered query
    std::expected<void, Exception> startQuery(QueryId queryId) noexcept;

    /// Stops the Query and moves it into the StoppedState. The exact semantics and guarantees depend on the chosen
    ///  QueryTerminationType
    /// @param queryId identifies the registered query
    /// @param terminationType dictates what happens with in in-flight data
    std::expected<void, Exception> stopQuery(QueryId queryId, QueryTerminationType terminationType) noexcept;

    /// Stops all Queries and moves them into the StoppedState. The exact semantics and guarantees depend on the chosen
    ///  QueryTerminationType
    /// @param terminationType dictates what happens with in in-flight data
    std::expected<void, Exception> stopAllQueries(QueryTerminationType type) noexcept;

    /// Unregisters a stopped Query.
    /// @param queryId identifies the registered stopped query
    std::expected<void, Exception> unregisterQuery(QueryId queryId) noexcept;

    /// Complete history of query status changes.
    [[nodiscard]] std::optional<QueryLog::Log> getQueryLog(QueryId queryId) const;
    /// Summary structure for query.
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> getQueryStatus(QueryId queryId) const noexcept;
    [[nodiscard]] WorkerStatus getWorkerStatus(std::chrono::system_clock::time_point after) const;
};
}
