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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/AbstractQueryStatusListener.hpp>
#include <Runtime/BufferManager.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngineConfiguration.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES
{
/// Where a resumed source continues: raw bytes already consumed and the next sequence number.
struct ReferenceSourcePosition
{
    uint64_t bytes = 0;
    uint64_t nextSequence = SequenceNumber::INITIAL;
};

/// Pipeline and source IDs of a running reference query, in plan order.
struct ReferenceQueryShape
{
    std::vector<size_t> pipelines;
    std::vector<size_t> sources;
};

/// In-memory snapshot of a paused query: each pipeline's exported state and the source position.
/// The state buffers belong to this engine's buffer manager, so a snapshot only resumes in the same process.
struct ReferenceSnapshot
{
    ReferenceSnapshot() = default;
    ReferenceSnapshot(const ReferenceSnapshot&) = delete;
    ReferenceSnapshot& operator=(const ReferenceSnapshot&) = delete;
    ~ReferenceSnapshot();

    void* handle = nullptr;
    size_t pipelines = 0;
};

class ReferenceQueryEngine final
{
public:
    explicit ReferenceQueryEngine(
        const QueryEngineConfiguration& configuration,
        std::shared_ptr<QueryEngineStatisticListener> statListener,
        std::shared_ptr<AbstractQueryStatusListener> listener,
        std::shared_ptr<BufferManager> bm,
        const Host& host);
    ~ReferenceQueryEngine();

    void start(std::unique_ptr<ExecutableQueryPlan> plan);
    bool adapt(std::unique_ptr<ExecutableQueryPlan> replacement, const std::vector<std::pair<PipelineId, PipelineId>>& stateTransfers);
    void stop(QueryId queryId);
    /// Snapshots every pipeline and the source at one cut and removes the running query.
    std::unique_ptr<ReferenceSnapshot> pause(QueryId queryId);
    /// Starts `plan` with each pipeline loading the paused pipeline at the same position and the source skipping consumed input.
    void resume(std::unique_ptr<ExecutableQueryPlan> plan, const ReferenceSnapshot& snapshot);

    std::shared_ptr<BufferManager> bufferManager;
    std::shared_ptr<AbstractQueryStatusListener> statusListener;
    std::shared_ptr<QueryEngineStatisticListener> statisticListener;
    Host host;
    void* referenceManager = nullptr;
    std::atomic_size_t nextReferenceQueryId = 1;
    std::mutex referenceQueryMutex;
    std::unordered_map<size_t, QueryId> referenceQueries;
    /// First pipeline failure per reference query, reported once the reference manager fails the query.
    std::unordered_map<size_t, Exception> referenceFailures;
    std::unordered_map<size_t, ReferenceQueryShape> referenceShapes;
};
}
