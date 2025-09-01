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
VoidSink::VoidSink(BackpressureController backpressureController, const SinkDescriptor&) : Sink(std::move(backpressureController))
{
}

void VoidSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up void sink: {}", *this);
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
