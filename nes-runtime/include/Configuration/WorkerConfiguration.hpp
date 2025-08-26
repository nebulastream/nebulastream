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
#include <QueryEngineConfiguration.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{
class WorkerConfiguration final : public BaseConfiguration
{
public:
    WorkerConfiguration() = default;
    WorkerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    QueryEngineConfiguration queryEngine = {"query_engine", "Configuration for the query engine"};
    QueryExecutionConfiguration defaultQueryExecution = {"default_query_execution", "Default configuration for query executions"};

    /// The number of buffers in the global buffer manager. Controls how much memory is consumed by the system.
    UIntOption numberOfBuffersInGlobalBufferManager
        = {"number_of_buffers_in_global_buffer_manager",
           "32768",
           "Number buffers in global buffer pool.",
           {std::make_shared<NumberValidation>()}};

    /// Configures the buffer size of individual TupleBuffers in bytes. This property has to be the same over a whole deployment.
    UIntOption bufferSizeInBytes
        = {"buffer_size_in_bytes",
           "4096",
           "Configures the buffer size of individual TupleBuffers in bytes.",
           {std::make_shared<NumberValidation>()}};

    /// Indicates how many buffers a single data source can allocate. This property controls the backpressure mechanism as a data source that can't allocate new records can't ingest more data.
    UIntOption numberOfBuffersInSourceLocalPools
        = {"number_of_buffers_in_source_local_pools",
           "64",
           "Number buffers in source local buffer pool. May be overwritten by a source-specific configuration (see SourceDescriptor).",
           {std::make_shared<NumberValidation>()}};

    EnumOption<DumpMode> dumpQueryCompilationIntermediateRepresentations
        = {"dump_compilation_result",
           DumpMode::NONE,
           fmt::format("If and where to dump query compilation results: {}", enumPipeList<DumpMode>())};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &queryEngine,
            &defaultQueryExecution,
            &numberOfBuffersInGlobalBufferManager,
            &numberOfBuffersInSourceLocalPools,
            &bufferSizeInBytes,
            &dumpQueryCompilationIntermediateRepresentations};
    }
};
}
