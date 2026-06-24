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
#include <VoidSink.hpp>

#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{
VoidSink::VoidSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , outputFilePath(sinkDescriptor.tryGetFromConfig(ConfigParametersVoid::FILE_PATH))
    , schemaFormatter(sinkDescriptor.getSchema())
{
}

void VoidSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up void sink: {}", *this);
    /// When driven by the systest harness a result-file path is injected; write just the schema header
    /// line (no data rows) so the harness sees a matching schema and an empty result set. We do this in
    /// start() -- guaranteed to run once -- rather than stop(), so a result file still exists even when
    /// no buffers reach the sink (e.g. under the NES_DISABLE_EMIT diagnostic).
    if (outputFilePath.has_value())
    {
        std::ofstream outputFileStream(*outputFilePath, std::ofstream::binary | std::ofstream::trunc);
        outputFileStream << schemaFormatter.getFormattedSchema();
    }
}

void VoidSink::stop(PipelineExecutionContext&)
{
    NES_INFO("Void Sink completed.")
}

void VoidSink::execute([[maybe_unused]] const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in VoidSink.");
}

DescriptorConfig::Config VoidSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersVoid>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterVoidSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return VoidSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterVoidSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<VoidSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
