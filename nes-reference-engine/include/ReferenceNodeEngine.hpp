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

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include <Configuration/WorkerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Listeners/StatisticListener.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceProvider.hpp>
#include <CompiledQueryPlan.hpp>
#include <QueryId.hpp>
#include <ReferenceQueryEngine.hpp>

namespace NES
{
class ReferenceNodeEngine
{
public:
    ReferenceNodeEngine(const WorkerConfiguration& configuration, std::shared_ptr<StatisticListener> statisticsListener, const Host& host);
    ~ReferenceNodeEngine();

    void startQuery(QueryId queryId, std::unique_ptr<CompiledQueryPlan> compiledQueryPlan, ExecutableQueryPlan::SharingIds sharingIds = {});
    void stopQuery(QueryId queryId);
    bool adaptQuery(
        std::unique_ptr<CompiledQueryPlan> replacement,
        const std::vector<std::pair<PipelineId, PipelineId>>& stateTransfers,
        ExecutableQueryPlan::SharingIds sharingIds = {});

    [[nodiscard]] std::shared_ptr<QueryLog> getQueryLog() { return queryLog; }

    [[nodiscard]] std::shared_ptr<const QueryLog> getQueryLog() const { return queryLog; }

private:
    std::shared_ptr<BufferManager> bufferManager;
    std::shared_ptr<QueryLog> queryLog;
    std::shared_ptr<SystemEventListener> systemEventListener;
    std::unique_ptr<ReferenceQueryEngine> queryEngine;
    std::unique_ptr<SourceProvider> sourceProvider;
};
}
