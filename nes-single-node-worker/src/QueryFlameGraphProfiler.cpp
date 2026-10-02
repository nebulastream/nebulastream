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

#include <QueryFlameGraphProfiler.hpp>

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <memory>
#include <mutex>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <nautilus/compiler/JitSymbolRegistry.hpp>
#include <nautilus/profiling/sampler.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>

namespace NES
{

QueryFlameGraphProfiler::QueryFlameGraphProfiler(std::filesystem::path directory) : directory(std::move(directory))
{
    std::error_code error;
    std::filesystem::create_directories(this->directory, error);
    if (error)
    {
        NES_WARNING("Could not create the flame graph directory {}: {}", this->directory.string(), error.message());
    }
}

QueryFlameGraphProfiler::~QueryFlameGraphProfiler() = default;

void QueryFlameGraphProfiler::onEvent(Event event)
{
    std::visit(
        Overloaded{
            [this](const QueryStart& start) { startQuery(start.queryId); },
            [this](const TaskExecutionStart& start) { startTask(start.queryId); },
            [this](const TaskExecutionComplete& complete) { stopTask(complete.queryId); },
            [this](const PipelineStart& start) { startPipeline(start.queryId, start.pipelineId); },
            [this](const PipelineStop& stop) { stopPipeline(stop.queryId, stop.pipelineId); },
            [this](const QueryStop& stop) { finishQuery(stop.queryId); },
            [this](const QueryFail& fail) { finishQuery(fail.queryId); },
            [](const auto&) {}},
        event);
}

void QueryFlameGraphProfiler::startQuery(const QueryId& queryId)
{
    nautilus::profiling::Sampler::Options options;
    /// Record call stacks, so the flame graph shows the worker and the host code a JIT frame was called from.
    options.callchain = true;
    auto sampler = std::make_shared<nautilus::profiling::Sampler>(options);

    const std::scoped_lock lock(mutex);
    if (not sampler->available())
    {
        if (not reportedUnavailable)
        {
            NES_WARNING("Cannot write query flame graphs, perf sampling is unavailable: {}", sampler->unavailableReason());
            reportedUnavailable = true;
        }
        return;
    }
    profiles.emplace(queryId, QueryProfile{.sampler = std::move(sampler), .runningPipelines = {}, .jitSymbols = {}});
}

std::shared_ptr<nautilus::profiling::Sampler> QueryFlameGraphProfiler::findSampler(const QueryId& queryId) const
{
    const std::scoped_lock lock(mutex);
    if (const auto it = profiles.find(queryId); it != profiles.end())
    {
        return it->second.sampler;
    }
    return nullptr;
}

void QueryFlameGraphProfiler::startTask(const QueryId& queryId)
{
    if (const auto sampler = findSampler(queryId))
    {
        sampler->start();
    }
}

void QueryFlameGraphProfiler::stopTask(const QueryId& queryId)
{
    if (const auto sampler = findSampler(queryId))
    {
        sampler->stop();
    }
}

void QueryFlameGraphProfiler::startPipeline(const QueryId& queryId, const PipelineId pipelineId)
{
    const std::scoped_lock lock(mutex);
    if (const auto it = profiles.find(queryId); it != profiles.end())
    {
        it->second.runningPipelines.insert(pipelineId);
    }
}

void QueryFlameGraphProfiler::stopPipeline(const QueryId& queryId, const PipelineId pipelineId)
{
    bool lastPipeline = false;
    {
        const std::scoped_lock lock(mutex);
        if (const auto it = profiles.find(queryId); it != profiles.end())
        {
            /// The stopping pipeline's compiled code is still alive here, but freed soon after.
            rememberJitSymbols(it->second);
            lastPipeline = it->second.runningPipelines.erase(pipelineId) > 0 and it->second.runningPipelines.empty();
        }
    }
    if (lastPipeline)
    {
        finishQuery(queryId);
    }
}

void QueryFlameGraphProfiler::rememberJitSymbols(QueryProfile& profile)
{
    for (auto& symbol : nautilus::compiler::JitSymbolRegistry::instance().snapshot())
    {
        if (symbol.moduleIndex != nautilus::compiler::NO_MODULE)
        {
            auto& symbols = profile.jitSymbols[symbol.moduleIndex];
            if (std::ranges::none_of(symbols, [&](const auto& known) { return known.start == symbol.start; }))
            {
                symbols.push_back(std::move(symbol));
            }
        }
    }
}

void QueryFlameGraphProfiler::finishQuery(const QueryId& queryId)
{
    QueryProfile profile;
    {
        const std::scoped_lock lock(mutex);
        const auto it = profiles.find(queryId);
        if (it == profiles.end())
        {
            return;
        }
        profile = std::move(it->second);
        profiles.erase(it);
    }
    rememberJitSymbols(profile);

    /// Registers the names of the query's modules that were freed in the meantime again, for as long as the samples are resolved.
    auto& registry = nautilus::compiler::JitSymbolRegistry::instance();
    std::unordered_set<nautilus::compiler::ModuleIndex> registered;
    for (const auto& symbol : registry.snapshot())
    {
        registered.insert(symbol.moduleIndex);
    }
    std::vector<nautilus::compiler::ModuleIndex> restored;
    std::vector<nautilus::compiler::JitSymbol> restoredSymbols;
    for (auto& [moduleIndex, symbols] : profile.jitSymbols)
    {
        if (not registered.contains(moduleIndex))
        {
            restored.push_back(moduleIndex);
            std::ranges::move(symbols, std::back_inserter(restoredSymbols));
        }
    }
    registry.addAll(std::move(restoredSymbols));

    /// Stops the threads that still sample into this query (e.g. after a task failed) and merges every thread's samples.
    const auto sampler = std::move(profile.sampler);
    sampler->stopAll();
    for (const auto moduleIndex : restored)
    {
        registry.remove(moduleIndex);
    }

    const auto& report = sampler->report();
    const auto path = directory / fmt::format("query-{}.svg", queryId.getLocalQueryId().getRawValue());
    if (report.empty())
    {
        NES_INFO("No samples for query {}, so no flame graph was written", queryId);
        return;
    }
    if (not report.writeFlameGraph(path.string(), fmt::format("NebulaStream query {}", queryId)))
    {
        NES_WARNING("Could not write the flame graph of query {} to {}", queryId, path.string());
        return;
    }
    NES_INFO(
        "Wrote the flame graph of query {} ({} samples, {} in JIT code) to {}",
        queryId,
        report.total(),
        report.jitSamples(),
        path.string());
}

}
