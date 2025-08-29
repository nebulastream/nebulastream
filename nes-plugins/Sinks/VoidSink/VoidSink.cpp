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

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES::Sinks
{
VoidSink::VoidSink(const SinkDescriptor& sinkDescriptor)
    : isOpen(false)
    , outputFilePath(sinkDescriptor.getFromConfig(SinkDescriptor::FILE_PATH))
    , formatter(std::make_unique<CSVFormat>(*sinkDescriptor.getSchema(), true))
{
}

void VoidSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up void sink: {}", *this);
    if (std::filesystem::exists(outputFilePath.c_str()))
    {
        std::error_code errorCode;
        if (!std::filesystem::remove(outputFilePath.c_str(), errorCode))
        {
            throw CannotOpenSink("Could not remove existing output file: filepath={}", outputFilePath);
        }
    }

    if (!outputFileStream.is_open())
    {
        outputFileStream.open(outputFilePath, std::ofstream::binary | std::ofstream::app);
    }
    isOpen = outputFileStream.is_open() && outputFileStream.good();
    if (!isOpen)
    {
        throw CannotOpenSink(
            "Could not open output file; filePathOutput={}, is_open()={}, good={}",
            outputFilePath,
            outputFileStream.is_open(),
            outputFileStream.good());
    }

    /// Write the schema to the file, if it is empty.
    if (outputFileStream.tellp() == 0)
    {
        const auto schemaStr = formatter->getFormattedSchema();
        outputFileStream.write(schemaStr.c_str(), static_cast<int64_t>(schemaStr.length()));
    }
}

void VoidSink::stop(PipelineExecutionContext&)
{
    NES_INFO("Void Sink completed.")
    outputFileStream.close();
    isOpen = false;
}

void VoidSink::execute([[maybe_unused]] const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in VoidSink.");
}

DescriptorConfig::Config VoidSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersVoid>(std::move(config), NAME);
}

SinkValidationRegistryReturnType SinkValidationGeneratedRegistrar::RegisterVoidSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return VoidSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterVoidSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<VoidSink>(sinkRegistryArguments.sinkDescriptor);
}

}
