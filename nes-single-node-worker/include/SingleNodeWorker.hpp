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
#include <string_view>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/NodeEngine.hpp>

#include <Util/Pointers.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <QueryCheckpointing.hpp>
#include <QueryCompiler.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

/// Binary name reported by `nes-single-node-worker --version` and by the RequestVersion RPC.
/// Both have to agree, so the literal lives in exactly one place.
inline constexpr std::string_view SingleNodeWorkerBinaryName{"nes-single-node-worker"};

/// @brief The SingleNodeWorker is a compiling StreamProcessingEngine, working alone on local sources and sinks, without external
/// coordination. The SingleNodeWorker compiles and immediately starts LogicalQueryPlans via the QueryCompiler and NodeEngine.
/// The Class itself is NonCopyable, but Movable, it owns the QueryCompiler and the NodeEngine.
class SingleNodeWorker
{
    SharedPtr<CompositeStatisticListener> listener;
    /// Null, if the worker neither checkpoints nor recovers queries.
    std::shared_ptr<CheckpointManager> checkpointManager;
    SharedPtr<NodeEngine> nodeEngine;
    UniquePtr<QueryCompilation::QueryCompiler> compiler;
    SingleNodeWorkerConfiguration configuration;
    /// Declared after the node engine, so that it stops cleaning up the checkpoints of stopped queries before the node engine stops all
    /// queries during its destruction.
    std::unique_ptr<QueryCheckpointing> queryCheckpointing;

    void recoverQueries();

public:
    explicit SingleNodeWorker(const SingleNodeWorkerConfiguration&, const Host& = Host("SingleNodeWorker"));
    ~SingleNodeWorker();
    /// Non-Copyable
    SingleNodeWorker(const SingleNodeWorker& other) = delete;
    SingleNodeWorker& operator=(const SingleNodeWorker& other) = delete;

    /// Movable
    SingleNodeWorker(SingleNodeWorker&& other) noexcept;
    SingleNodeWorker& operator=(SingleNodeWorker&& other) noexcept;

    /// Compiles the LogicalPlan and immediately starts the query asynchronously. Query execution errors are only reported
    /// during runtime of the query.
    /// @param plan Fully Specified LogicalQueryPlan.
    /// @return QueryId which identifies the started query
    [[nodiscard]] std::expected<QueryId, Exception> startQuery(LogicalPlan plan) noexcept;

    /// Stops the Query and moves it into the StoppedState.
    /// @param queryId identifies the registered query
    std::expected<void, Exception> stopQuery(QueryId queryId) noexcept;

    /// Runs a checkpoint round in addition to the periodic ones. Requires that the worker checkpoints its queries.
    std::expected<void, Exception> checkpoint() noexcept;

    /// Summary structure for query.
    [[nodiscard]] std::expected<LocalQueryStatusSnapshot, Exception> getQueryStatus(QueryId queryId) const noexcept;
    [[nodiscard]] WorkerStatus getWorkerStatus(std::chrono::system_clock::time_point after) const;
};
}
