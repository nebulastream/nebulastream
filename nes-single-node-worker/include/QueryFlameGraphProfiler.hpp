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

#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>

namespace nautilus::profiling
{
class Sampler;
}

namespace NES
{

/// Samples every query with nautilus' in-process profiler (the nautilus-profiling plugin, built on perf-cpp). When a query stops or
/// fails, it writes to the directory
/// - `query-<id>.svg`: a flame graph of the query, with one root per pipeline (`pipeline_<id>`) and its nautilus regions below it;
/// - `query-<id>-pipeline_<id>.ir.txt` per pipeline: the pipeline's nautilus IR with the share of samples per IR line.
///
/// Each pipeline has its own sampler, and a worker samples into it only while it executes one of that pipeline's tasks (between
/// TaskExecutionStart and TaskExecutionComplete, which the query engine emits on the worker thread). So concurrent queries and the
/// pipelines of one query stay apart, and each pipeline's samples only hit its own compiled module, whose IR they annotate.
///
/// JIT frames and IR lines are only resolved if the queries are compiled with `perf.sample`, which needs the MLIR backend (execution
/// mode COMPILER). nautilus forgets a module's names and line table when its code is freed, so a profiled query's pipelines hand their
/// code to CompiledCodeRetention, and the profiler releases it once the profile is written.
///
/// Sampling needs perf_event_open: on a kernel with `perf_event_paranoid > 2`, or in a container whose seccomp profile blocks it, the
/// profiler logs why once and writes nothing.
class QueryFlameGraphProfiler final : public QueryEngineStatisticListener
{
public:
    explicit QueryFlameGraphProfiler(std::filesystem::path directory);
    ~QueryFlameGraphProfiler() override;

    QueryFlameGraphProfiler(const QueryFlameGraphProfiler&) = delete;
    QueryFlameGraphProfiler(QueryFlameGraphProfiler&&) = delete;
    QueryFlameGraphProfiler& operator=(const QueryFlameGraphProfiler&) = delete;
    QueryFlameGraphProfiler& operator=(QueryFlameGraphProfiler&&) = delete;

    void onEvent(Event event) override;

private:
    /// The samplers of a query's pipelines, ordered by pipeline id.
    using QueryProfile = std::map<PipelineId, std::shared_ptr<nautilus::profiling::Sampler>>;

    void startQuery(const QueryId& queryId);
    void startPipeline(const QueryId& queryId, PipelineId pipelineId);
    void startTask(const QueryId& queryId, PipelineId pipelineId);
    void stopTask(const QueryId& queryId, PipelineId pipelineId);
    void finishQuery(const QueryId& queryId);
    void writeProfile(const QueryId& queryId, const QueryProfile& profile) const;
    [[nodiscard]] std::shared_ptr<nautilus::profiling::Sampler> findSampler(const QueryId& queryId, PipelineId pipelineId) const;

    std::filesystem::path directory;
    mutable std::mutex mutex;
    std::unordered_map<QueryId, QueryProfile> profiles;
    bool reportedUnavailable = false;
};

}
