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

#include <Sinks/FileSink.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>

#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <SinksParsing/SchemaFormatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

FileSink::FileSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , outputFilePath(sinkDescriptor.getFromConfig(SinkDescriptor::FILE_PATH))
    , isAppend(sinkDescriptor.getFromConfig(ConfigParametersFile::APPEND))
    , isOpen(false)
    , schemaFormatter(SchemaFormatter(sinkDescriptor.getSchema()))
{
}

std::ostream& FileSink::toString(std::ostream& str) const
{
    str << fmt::format("FileSink(filePathOutput: {}, isAppend: {})", outputFilePath, isAppend);
    return str;
}

void FileSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up file sink: {}", *this);
    const auto stream = outputFileStream.wlock();
    /// Remove an existing file unless the isAppend mode is isAppend.
    if (!isAppend)
    {
        if (std::filesystem::exists(outputFilePath.c_str()))
        {
            if (std::error_code ec; !std::filesystem::remove(outputFilePath.c_str(), ec))
            {
                isOpen = false;
                throw CannotOpenSink("Could not remove existing output file: filePath={} ", outputFilePath);
            }
        }
    }

    /// Open the file stream
    if (!stream->is_open())
    {
        stream->open(outputFilePath, std::ofstream::binary | std::ofstream::app);
    }
    isOpen = stream->is_open() && stream->good();
    if (!isOpen)
    {
        throw CannotOpenSink(
            "Could not open output file; filePathOutput={}, is_open()={}, good={}", outputFilePath, stream->is_open(), stream->good());
    }

    /// Write the schema to the file, if it is empty.
    if (stream->tellp() == 0)
    {
        const auto schemaStr = schemaFormatter.getFormattedSchema();
        stream->write(schemaStr.c_str(), static_cast<int64_t>(schemaStr.length()));
    }
}

void FileSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in FileSink.");
    PRECONDITION(isOpen, "Sink was not opened");

    {
        {
            const auto wlocked = outputFileStream.wlock();
            /// Create a buffer iterator to help iterate through the tuplebuffer and its children
            BufferIterator iterator(std::move(inputTupleBuffer));

            bool processedAllBuffers = false;
            while (!processedAllBuffers)
            {
                /// Get the next buffer to be written
                BufferIterator::BufferElement element = iterator.getNextElement();
                wlocked->write(element.buffer.getAvailableMemoryArea<char>().data(), static_cast<long>(element.contentLength));
                processedAllBuffers = element.isLastElement;
            }
            wlocked->flush();
        }
    }
}

void FileSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Closing file sink, filePathOutput={}", outputFilePath);
    const auto stream = outputFileStream.wlock();
    stream->flush();
    stream->close();
}

DescriptorConfig::Config FileSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersFile>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterFileSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return FileSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterFileSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<FileSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
