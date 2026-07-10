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

#include <memory>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <Util/DumpMode.hpp>
#include <fmt/format.h>
#include <QueryEngineConfiguration.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <WorkerNetworkConfiguration.hpp>

namespace NES
{
class WorkerConfiguration final : public BaseConfiguration
{
public:
    WorkerConfiguration() = default;
    WorkerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    QueryEngineConfiguration queryEngine = {"query_engine", "Configuration for the query engine"};
    QueryExecutionConfiguration defaultQueryExecution = {"default_query_execution", "Default configuration for query executions"};
    QueryOptimizerConfiguration defaultQueryOptimization = {"default_query_optimization", "Default configuration for query optimizations"};
    WorkerNetworkConfiguration network = {"network", "Default configuration for network sources and sinks"};

    /// The number of buffers in the global buffer manager. Controls how much memory is consumed by the system.
    UIntOption numberOfBuffersInGlobalBufferManager
        = {"number_of_buffers_in_global_buffer_manager",
           "32768",
           "Number buffers in global buffer pool.",
           {std::make_shared<NumberValidation>()}};

    /// Alignment (in bytes, a power of two and <= page size) of every payload in the global buffer pool.
    /// Default 64 (cache line). Set to the storage device's logical block size (typically 512) to enable
    /// io_uring registered fixed-buffer O_DIRECT reads in IoUringFileSource, which require device-block
    /// aligned payloads; otherwise that source transparently falls back to non-fixed reads. Larger alignment
    /// costs a little control-block padding per buffer, which matters only for very large (million-buffer) pools.
    UIntOption globalBufferAlignment
        = {"global_buffer_alignment",
           "64",
           "Alignment in bytes of global pool buffer payloads (power of two, <= page size).",
           {std::make_shared<NumberValidation>()}};

    /// Indicates how many buffers a single data source can allocate. This property controls the backpressure mechanism as a data source that can't allocate new records can't ingest more data.
    UIntOption defaultMaxInflightBuffers
        = {"default_max_inflight_buffers",
           "64",
           "Number of buffers a source can have inflight before blocking. May be overwritten by a source-specific configuration (see "
           "SourceDescriptor).",
           {std::make_shared<NumberValidation>()}};

    /// Per-CCX SHARED source pools (requires query_engine.ccx_aware_task_queues + pin_threads,
    /// i.e. the CCX-striped cell layout): all non-prefill sources whose io threads live on one
    /// CCX draw raw buffers from ONE shared pool instead of a private pool each, capping the
    /// recycled-ring working set at numCcx x ccx_source_pool_buffers regardless of source count
    /// (per-source rings spill the L3 at >=16 sources and halve the aggregate supply rate).
    /// Scope: prefill sources (io_uring variants, InMemory prefill) and sources with an explicit
    /// MAX_INFLIGHT_BUFFERS override keep private pools. Fairness: the per-source inflight
    /// semaphore is capped at half the cell pool; there is NO per-source reserved minimum, so a
    /// heavily backlogged source can transiently starve cell peers (buffers return via worker
    /// task completion, so this cannot deadlock).
    BoolOption ccxSharedSourcePools
        = {"ccx_shared_source_pools",
           "false",
           "Share one source buffer pool per CCX cell (needs ccx_aware_task_queues + pin_threads); see header comment."};
    UIntOption ccxSourcePoolBuffers
        = {"ccx_source_pool_buffers",
           "128",
           "Buffers per CCX-cell source pool when ccx_shared_source_pools is on (128 x 128 KiB = 16 MB ~ 0.5x per-CCX L3).",
           {std::make_shared<NumberValidation>()}};

    EnumOption<DumpMode::Options> dumpQueryCompilationIR
        = {"dump_compilation_result",
           DumpMode::Options::NONE,
           fmt::format("If and where to dump query compilation results: {}", enumPipeList<DumpMode::Options>())};

    BoolOption dumpGraph = {"dump_graph", "false", "If to dump graph of the compilation results"};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &queryEngine,
            &defaultQueryExecution,
            &defaultQueryOptimization,
            &network,
            &numberOfBuffersInGlobalBufferManager,
            &globalBufferAlignment,
            &defaultMaxInflightBuffers,
            &ccxSharedSourcePools,
            &ccxSourcePoolBuffers,
            &dumpQueryCompilationIR,
            &dumpGraph};
    }
};
}
