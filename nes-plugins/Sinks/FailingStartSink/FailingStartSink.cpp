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

#include <FailingStartSink.hpp>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

namespace NES
{

FailingStartSink::FailingStartSink(BackpressureController backpressureController, const SinkDescriptor&)
    : Sink(std::move(backpressureController))
{
}

void FailingStartSink::start(PipelineExecutionContext&)
{
    throw CannotOpenSink("FailingStartSink intentionally fails in start()");
}

void FailingStartSink::stop(PipelineExecutionContext&)
{
}

void FailingStartSink::execute([[maybe_unused]] const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in FailingStartSink.");
}

DescriptorConfig::Config FailingStartSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersFailingStart>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterFailingStartSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return FailingStartSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterFailingStartSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<FailingStartSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
