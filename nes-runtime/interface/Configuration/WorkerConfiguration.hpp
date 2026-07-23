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
#include <Configurations/Validation/FloatValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <Configurations/Validation/PowerOfTwoValidation.hpp>
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

    /// Total memory budget in bytes shared by the global buffer pool. The buffer manager splits it into an unpooled
    /// share (see unpooled_memory_fraction) and a pooled share; the pooled share is divided into operator buffers.
    UIntOption totalMemoryInBytes
        = {"total_memory_in_bytes",
           "268435456",
           "Total memory budget in bytes for the global buffer pool (pooled + unpooled).",
           {std::make_shared<NumberValidation>()}};

    /// Share (0.0-1.0) of total_memory_in_bytes reserved for unpooled (variable-sized) buffers, used by operator state
    /// (hash maps, paged vectors, var-sized data). On breach, the requesting query fails with CannotAllocateBuffer
    /// instead of the worker running out of physical memory.
    FloatOption unpooledMemoryFraction
        = {"unpooled_memory_fraction",
           "0.5",
           "Fraction (0.0-1.0) of total memory reserved for unpooled buffers.",
           {std::make_shared<FloatValidation>(0, 1)}};

    /// Byte alignment of every pooled and unpooled buffer. Must be a power of two and at most the page size;
    /// the default is a cache line (64 B).
    UIntOption bufferAlignmentInBytes
        = {"buffer_alignment_in_bytes",
           "64",
           "Byte alignment of every buffer (power of two, <= page size).",
           {std::make_shared<PowerOfTwoValidation>()}};

    /// Indicates how many buffers a single data source can allocate. This property controls the backpressure mechanism as a data source that can't allocate new records can't ingest more data.
    UIntOption defaultMaxInflightBuffers
        = {"default_max_inflight_buffers",
           "64",
           "Number of buffers a source can have inflight before blocking. May be overwritten by a source-specific configuration (see "
           "SourceDescriptor).",
           {std::make_shared<NumberValidation>()}};

    /// #1713: enables adaptive (AIMD) per-source inflight-buffer provisioning -- the inflight cap starts low and grows
    /// toward default_max_inflight_buffers under load, decaying when idle. Off by default (fixed cap).
    BoolOption enableAdaptiveInflightBuffers
        = {"enable_adaptive_inflight_buffers", "false", "Enable adaptive (AIMD) per-source inflight-buffer provisioning."};

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
            &totalMemoryInBytes,
            &unpooledMemoryFraction,
            &bufferAlignmentInBytes,
            &defaultMaxInflightBuffers,
            &enableAdaptiveInflightBuffers,
            &dumpQueryCompilationIR,
            &dumpGraph};
    }
};
}
