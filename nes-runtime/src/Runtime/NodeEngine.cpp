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

#include <Runtime/NodeEngine.hpp>

#include <chrono>
#include <memory>
#include <utility>
#if defined(NES_EVENT_TRACE_RING) || defined(NES_EVENT_TRACE_ITT)
    #include <algorithm>
    #include <sstream>
    #include <string>
    #include <Sources/SourceHandle.hpp>
    #include <fmt/format.h>
    #include <ExecutablePipelineStage.hpp>
#endif
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Runtime/BufferManager.hpp>

#include <Util/EventTrace.hpp>
#include <Util/Logger/Logger.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngine.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>

namespace NES
{

NodeEngine::~NodeEngine()
{
    NES_DEBUG("Shutting down NodeEngine");
    queryEngine.reset();
    sourceProvider.reset();

    bufferManager->destroy();
    bufferManager.reset();
}

#ifdef NES_EVENT_TRACE_ENABLED
namespace
{
/// One line per source and pipeline: `<kind> <id> <comma separated successor ids or -> <description>`.
std::string latencyTracePlan(const ExecutableQueryPlan& plan)
{
    auto successorIds = [](const std::vector<std::weak_ptr<ExecutablePipeline>>& successors)
    {
        std::string ids;
        for (const auto& successor : successors)
        {
            if (const auto pipeline = successor.lock())
            {
                ids += (ids.empty() ? "" : ",") + std::to_string(pipeline->id.getRawValue());
            }
        }
        return ids.empty() ? std::string("-") : ids;
    };
    auto oneLine = [](const auto& printable)
    {
        std::stringstream stream;
        stream << printable;
        auto text = stream.str();
        std::ranges::replace(text, '\n', ' ');
        return text;
    };

    std::stringstream out;
    out << "query " << plan.queryId << '\n';
    for (const auto& [source, successors] : plan.sources)
    {
        out << "source " << source->getSourceId() << ' ' << successorIds(successors) << ' ' << oneLine(*source) << '\n';
    }
    for (const auto& pipeline : plan.pipelines)
    {
        out << "pipeline " << pipeline->id << ' ' << successorIds(pipeline->successors) << ' ' << oneLine(*pipeline->stage) << '\n';
    }
    return out.str();
}
}
#endif

NodeEngine::NodeEngine(
    std::shared_ptr<BufferManager> bufferManager,
    std::shared_ptr<SystemEventListener> systemEventListener,
    std::shared_ptr<QueryLog> queryLog,
    std::unique_ptr<QueryEngine> queryEngine,
    std::unique_ptr<SourceProvider> sourceProvider)
    : bufferManager(std::move(bufferManager))
    , queryLog(std::move(queryLog))
    , systemEventListener(std::move(systemEventListener))
    , queryEngine(std::move(queryEngine))
    , sourceProvider(std::move(sourceProvider))
{
}

void NodeEngine::startQuery(QueryId queryId, std::unique_ptr<CompiledQueryPlan> compiledQueryPlan)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    queryLog->logQueryStatusChange(queryId, QueryStatus::Registered, std::chrono::system_clock::now());
    systemEventListener->onEvent(StartQuerySystemEvent(std::move(queryId)));
    auto executableQueryPlan = ExecutableQueryPlan::instantiate(*compiledQueryPlan, *sourceProvider);
    /// Every query's plan is written, because latency tracing can be turned on while it runs.
    NES_EVENT(EventTrace::writePlan(latencyTracePlan(*executableQueryPlan)));
    queryEngine->start(std::move(executableQueryPlan));
}

void NodeEngine::stopQuery(QueryId queryId)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    NES_INFO("Stop {}", queryId);
    systemEventListener->onEvent(StopQuerySystemEvent(queryId));
    queryEngine->stop(queryId);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) the trace backends are chosen at build time
void NodeEngine::setLatencyTracing([[maybe_unused]] QueryId queryId, [[maybe_unused]] bool enabled)
{
#ifdef NES_EVENT_TRACE_ENABLED
    queryEngine->setLatencyTraced(queryId, enabled);
#else
    throw NotImplemented("Latency tracing needs a build with -DNES_EVENT_TRACE_BACKENDS");
#endif
}

}
