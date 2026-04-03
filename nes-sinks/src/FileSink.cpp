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

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <SinksParsing/NoneWithIteratorFormat.hpp>
#include <SinksParsing/SchemaFormatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

FileSink::FileSink(
    BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor, std::optional<std::unique_ptr<Encoder>> encoder)
    : Sink(std::move(backpressureController))
    , outputFilePath(sinkDescriptor.getFromConfig(ConfigParametersFile::FILE_PATH))
    , isAppend(sinkDescriptor.getFromConfig(ConfigParametersFile::APPEND))
    , isOpen(false)
    , schemaFormatter(SchemaFormatter(sinkDescriptor.getSchema()))
    , encoder(std::move(encoder))
{
    const auto legacyOutputFormat = toUpperCase(sinkDescriptor.getFromConfig(ConfigParametersFile::LEGACY_OUTPUT_FORMAT));
    if (legacyOutputFormat == "CSV")
    {
        format = std::make_unique<CSVFormat>(*sinkDescriptor.getSchema());
    }
    else if (legacyOutputFormat == "JSON")
    {
        format = std::make_unique<JSONFormat>(*sinkDescriptor.getSchema());
    }
    else
    {
        format = std::make_unique<NoneWithIteratorFormat>(*sinkDescriptor.getSchema());
    }
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
        auto schemaStr = schemaFormatter.getFormattedSchema();

        /// Encode schema string if encoder was provided
        if (encoder)
        {
            const auto stringSpan = std::as_bytes(std::span(schemaStr));
            std::vector<char> encodedData{};
            auto encodingResult = encoder.value()->encodeBuffer(stringSpan, encodedData);
            PRECONDITION(
                encodingResult.status == Encoder::EncodeStatusType::SUCCESSFULLY_ENCODED,
                "Error occured during encoding process of schema string.");
            schemaStr = std::string(encodedData.data(), encodingResult.compressedSize);
        }
        stream->write(schemaStr.c_str(), static_cast<int64_t>(schemaStr.length()));
    }
}

void FileSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in FileSink.");
    PRECONDITION(isOpen, "Sink was not opened");
    std::string formattedBufferString = format->getFormattedBuffer(inputTupleBuffer);
    /// If an encoder was given, encode the data here
    if (encoder)
    {
        const auto stringSpan = std::as_bytes(std::span(formattedBufferString));
        std::vector<char> encodedData{};
        auto encodingResult = encoder.value()->encodeBuffer(stringSpan, encodedData);
        PRECONDITION(
            encodingResult.status == Encoder::EncodeStatusType::SUCCESSFULLY_ENCODED, "Error occured during encoding process.");
        formattedBufferString = std::string(encodedData.data(), encodingResult.compressedSize);
    }
    {
        const auto wlocked = outputFileStream.wlock();
        wlocked->write(formattedBufferString.data(), static_cast<std::streamsize>(formattedBufferString.length()));
        wlocked->flush();
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
    return std::make_unique<FileSink>(
        std::move(sinkRegistryArguments.backpressureController),
        sinkRegistryArguments.sinkDescriptor,
        std::move(sinkRegistryArguments.encoder));
}

}
