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

#include <cstdint>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/DumpMode.hpp>
#include <QueryEngineConfiguration.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <WorkerNetworkConfiguration.hpp>

namespace NES
{
struct WorkerConfiguration
{
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    WorkerConfiguration() = delete;

    WorkerConfiguration(
        QueryEngineConfiguration queryEngine,
        QueryExecutionConfiguration defaultQueryExecution,
        WorkerNetworkConfiguration network,
        uint64_t totalMemoryInBytes,
        float unpooledMemoryFraction,
        uint64_t bufferAlignmentInBytes,
        uint64_t defaultMaxInflightBuffers,
        DumpMode::Options dumpQueryCompilationIR,
        bool dumpGraph)
        : queryEngine(queryEngine)
        , defaultQueryExecution(defaultQueryExecution)
        , network(network)
        , totalMemoryInBytes(totalMemoryInBytes)
        , unpooledMemoryFraction(unpooledMemoryFraction)
        , bufferAlignmentInBytes(bufferAlignmentInBytes)
        , defaultMaxInflightBuffers(defaultMaxInflightBuffers)
        , dumpQueryCompilationIR(dumpQueryCompilationIR)
        , dumpGraph(dumpGraph)
    {
    }

    QueryEngineConfiguration queryEngine;
    QueryExecutionConfiguration defaultQueryExecution;
    WorkerNetworkConfiguration network;

    /// Total memory budget in bytes shared by the global buffer pool. The buffer manager splits it into an unpooled
    /// share (see unpooled_memory_fraction) and a pooled share; the pooled share is divided into operator buffers.
    uint64_t totalMemoryInBytes;

    /// Share (0.0-1.0) of total_memory_in_bytes reserved for unpooled (variable-sized) buffers, used by operator state
    /// (hash maps, paged vectors, var-sized data). On breach, the requesting query fails with CannotAllocateBuffer
    /// instead of the worker running out of physical memory.
    float unpooledMemoryFraction;

    /// Byte alignment of every pooled and unpooled buffer. Must be a power of two and at most the page size;
    /// the default is a cache line (64 B).
    uint64_t bufferAlignmentInBytes;

    /// Indicates how many buffers a single data source can allocate. This property controls the backpressure mechanism as a data source that can't allocate new records can't ingest more data.
    uint64_t defaultMaxInflightBuffers;

    DumpMode::Options dumpQueryCompilationIR;

    bool dumpGraph;

    static WorkerConfiguration fromConfig(const InstantiatedConfig& config);
};
}
