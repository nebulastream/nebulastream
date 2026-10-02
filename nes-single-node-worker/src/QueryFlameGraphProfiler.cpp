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

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Pipelines/CompiledCodeRetention.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <nautilus/profiling/sample_report.hpp>
#include <nautilus/profiling/sampler.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>

namespace NES
{

namespace
{
/// Lines of unannotated IR kept around each sampled line of an annotated IR.
constexpr size_t ANNOTATED_IR_CONTEXT_LINES = 3;
}

QueryFlameGraphProfiler::QueryFlameGraphProfiler(std::filesystem::path directory) : directory(std::move(directory))
{
    std::error_code error;
    std::filesystem::create_directories(this->directory, error);
    if (error)
    {
        NES_WARNING("Could not create the flame graph directory {}: {}", this->directory.string(), error.message());
    }
}

QueryFlameGraphProfiler::~QueryFlameGraphProfiler()
{
    /// Queries that never terminated keep no compiled code alive beyond the worker.
    const std::scoped_lock lock(mutex);
    for (const auto& [queryId, profile] : profiles)
    {
        CompiledCodeRetention::release(queryId);
    }
}

void QueryFlameGraphProfiler::onEvent(Event event)
{
    std::visit(
        Overloaded{
            [this](const QueryStart& start) { startQuery(start.queryId); },
            [this](const PipelineStart& start) { startPipeline(start.queryId, start.pipelineId); },
            [this](const TaskExecutionStart& start) { startTask(start.queryId, start.pipelineId); },
            [this](const TaskExecutionComplete& complete) { stopTask(complete.queryId, complete.pipelineId); },
            [this](const QueryStop& stop) { finishQuery(stop.queryId); },
            [this](const QueryFail& fail) { finishQuery(fail.queryId); },
            [](const auto&) {}},
        event);
}

void QueryFlameGraphProfiler::startQuery(const QueryId& queryId)
{
    const std::scoped_lock lock(mutex);
    profiles.try_emplace(queryId);
}

void QueryFlameGraphProfiler::startPipeline(const QueryId& queryId, const PipelineId pipelineId)
{
    nautilus::profiling::Sampler::Options options;
    /// Record call stacks, so the flame graph shows the worker and the host code a JIT frame was called from.
    options.callchain = true;
    auto sampler = std::make_shared<nautilus::profiling::Sampler>(options);

    const std::scoped_lock lock(mutex);
    const auto it = profiles.find(queryId);
    if (it == profiles.end())
    {
        return;
    }
    if (not sampler->available())
    {
        if (not reportedUnavailable)
        {
            NES_WARNING("Cannot profile queries, perf sampling is unavailable: {}", sampler->unavailableReason());
            reportedUnavailable = true;
        }
        return;
    }
    it->second.try_emplace(pipelineId, std::move(sampler));
}

std::shared_ptr<nautilus::profiling::Sampler>
QueryFlameGraphProfiler::findSampler(const QueryId& queryId, const PipelineId pipelineId) const
{
    const std::scoped_lock lock(mutex);
    if (const auto query = profiles.find(queryId); query != profiles.end())
    {
        if (const auto pipeline = query->second.find(pipelineId); pipeline != query->second.end())
        {
            return pipeline->second;
        }
    }
    return nullptr;
}

void QueryFlameGraphProfiler::startTask(const QueryId& queryId, const PipelineId pipelineId)
{
    if (const auto sampler = findSampler(queryId, pipelineId))
    {
        sampler->start();
    }
}

void QueryFlameGraphProfiler::stopTask(const QueryId& queryId, const PipelineId pipelineId)
{
    if (const auto sampler = findSampler(queryId, pipelineId))
    {
        sampler->stop();
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
    writeProfile(queryId, profile);
    CompiledCodeRetention::release(queryId);
}

void QueryFlameGraphProfiler::writeProfile(const QueryId& queryId, const QueryProfile& profile) const
{
    const auto queryName = queryId.getLocalQueryId().getRawValue();
    std::vector<nautilus::profiling::SampleStack> stacks;
    uint64_t total = 0;
    uint64_t jitSamples = 0;
    for (const auto& [pipelineId, sampler] : profile)
    {
        /// Stops the threads that still sample into this pipeline (e.g. after a task failed) and merges every thread's samples.
        sampler->stopAll();
        const auto& report = sampler->report();
        total += report.total();
        jitSamples += report.jitSamples();
        stacks.insert(stacks.end(), report.stacks().begin(), report.stacks().end());

        /// A pipeline's samples only land in its own module, so its line table annotates exactly that pipeline's IR.
        if (not report.sourceLines().empty())
        {
            const auto irPath = directory / fmt::format("query-{}-pipeline_{}.ir.txt", queryName, pipelineId);
            std::ofstream irFile(irPath);
            irFile << report.annotateSource({}, ANNOTATED_IR_CONTEXT_LINES);
            if (not irFile)
            {
                NES_WARNING("Could not write the annotated IR of pipeline {} of query {} to {}", pipelineId, queryId, irPath.string());
            }
        }
    }

    if (total == 0)
    {
        NES_INFO("No samples for query {}, so no flame graph was written", queryId);
        return;
    }
    const nautilus::profiling::SampleReport merged({}, std::move(stacks), total);
    const auto path = directory / fmt::format("query-{}.svg", queryName);
    if (not merged.writeFlameGraph(path.string(), fmt::format("NebulaStream query {}", queryId)))
    {
        NES_WARNING("Could not write the flame graph of query {} to {}", queryId, path.string());
        return;
    }
    NES_INFO("Wrote the flame graph of query {} ({} samples, {} in JIT code) to {}", queryId, total, jitSamples, path.string());
}

}
