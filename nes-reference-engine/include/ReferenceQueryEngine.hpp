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
#include <QueryId.hpp>

namespace NES
{
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

    std::shared_ptr<BufferManager> bufferManager;
    std::shared_ptr<AbstractQueryStatusListener> statusListener;
    std::shared_ptr<QueryEngineStatisticListener> statisticListener;
    Host host;
    void* referenceManager = nullptr;
    std::atomic_size_t nextReferenceQueryId = 1;
    std::mutex referenceQueryMutex;
    std::unordered_map<size_t, QueryId> referenceQueries;
};
}
