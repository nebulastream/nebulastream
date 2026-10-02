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
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <nautilus/compiler/JitSymbolRegistry.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>

namespace nautilus::profiling
{
class Sampler;
}

namespace NES
{

/// Samples every query with nautilus' in-process profiler (the nautilus-profiling plugin, built on perf-cpp) and writes a flame graph
/// per query to `<directory>/query-<id>.svg` once its last pipeline stopped, or the query failed.
///
/// Worker threads run the tasks of all queries, so each query has its own sampler, and a worker samples into it only while it executes
/// one of that query's tasks (between TaskExecutionStart and TaskExecutionComplete, which the query engine emits on the worker thread).
/// JIT frames carry their nautilus region names if the queries are compiled with `perf.sample`, which needs the MLIR backend
/// (execution mode COMPILER); other modes still sample, but leave JIT frames unnamed. The names are resolved when the profile is written,
/// once the query's last pipeline stopped, but nautilus drops a module's names from its JIT symbol registry when the compiled code is freed,
/// which happens as soon as a pipeline stops. So the profiler copies the registered names whenever a pipeline stops, while its code is
/// still alive, and briefly registers the names of freed modules again while it resolves the query's samples.
///
/// Sampling needs perf_event_open: on a kernel with `perf_event_paranoid > 2`, or in a container whose seccomp profile blocks it, the
/// profiler logs why once and writes no flame graphs.
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
    struct QueryProfile
    {
        std::shared_ptr<nautilus::profiling::Sampler> sampler;
        std::unordered_set<PipelineId> runningPipelines;
        /// JIT symbols of the modules seen while the query ran, by module.
        std::unordered_map<nautilus::compiler::ModuleIndex, std::vector<nautilus::compiler::JitSymbol>> jitSymbols;
    };

    static void rememberJitSymbols(QueryProfile& profile);

    void startQuery(const QueryId& queryId);
    void startTask(const QueryId& queryId);
    void stopTask(const QueryId& queryId);
    void startPipeline(const QueryId& queryId, PipelineId pipelineId);
    void stopPipeline(const QueryId& queryId, PipelineId pipelineId);
    void finishQuery(const QueryId& queryId);
    [[nodiscard]] std::shared_ptr<nautilus::profiling::Sampler> findSampler(const QueryId& queryId) const;

    std::filesystem::path directory;
    mutable std::mutex mutex;
    std::unordered_map<QueryId, QueryProfile> profiles;
    bool reportedUnavailable = false;
};

}
