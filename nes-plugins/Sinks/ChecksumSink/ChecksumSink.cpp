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

#include <ChecksumSink.hpp>

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{

/// Config fields of the checksum sink, shared by getConfigSchema (declaration) and
/// ChecksumSinkConfig::fromConfig (typed extraction).
/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<std::filesystem::path> FILE_PATH{
    Identifier::parse("FILE_PATH"),
    "The path to the file to wich to wruiteite the final chescksum.",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .transform([](const auto& val) { return std::filesystem::path{val}; });
    }};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> ChecksumSink::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("CHECKSUM_SINK"), FILE_PATH);
}

std::expected<ChecksumSinkConfig, Exception> ChecksumSinkConfig::fromConfig(const InstantiatedConfig& config)
{
    return ChecksumSinkConfig{.filePath = config.get(FILE_PATH)};
}

ChecksumSink::ChecksumSink(BackpressureController backpressureController, const ChecksumSinkConfig& config)
    : Sink(std::move(backpressureController)), isOpen(false), outputFilePath(config.filePath)
{
}

void ChecksumSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up checksum sink: {}", *this);
    if (std::filesystem::exists(outputFilePath.c_str()))
    {
        std::error_code ec;
        if (!std::filesystem::remove(outputFilePath.c_str(), ec))
        {
            throw CannotOpenSink("Could not remove existing output file: filePath={} ", outputFilePath);
        }
    }

    /// Open the file stream
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
}

void ChecksumSink::stop(PipelineExecutionContext&)
{
    NES_INFO("Checksum Sink completed. Checksum: {}", fmt::streamed(checksum));

    outputFileStream << "Count:UINT64:" << magic_enum::enum_name(DataType::NULLABLE::NOT_NULLABLE)
                     << ",Checksum:UINT64:" << magic_enum::enum_name(DataType::NULLABLE::NOT_NULLABLE) << '\n';
    outputFileStream << checksum.numberOfTuples << "," << checksum.checksum << '\n';
    outputFileStream.close();
    isOpen = false;
}

void ChecksumSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in ChecksumSink.");
    /// Create a buffer iterator to help iterate through the tuplebuffer and its children
    BufferIterator iterator{inputBuffer};

    std::optional<BufferIterator::BufferElement> element = iterator.getNextElement();
    while (element.has_value())
    {
        /// Create string out of formatted buffer
        const std::string formatted{element.value().buffer.getAvailableMemoryArea<char>().data(), element.value().contentLength};
        checksum.add(formatted);
        /// Get the next buffer
        element = iterator.getNextElement();
    }
}

}
