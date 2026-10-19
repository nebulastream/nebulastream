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

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <QueryId.hpp>

namespace NES
{
namespace
{
constexpr std::string_view SinkCheckpointFileName = "sink";

std::optional<std::pair<size_t, size_t>> parseChecksumCheckpoint(const std::string_view contents)
{
    const auto separator = contents.find(',');
    if (separator == std::string_view::npos)
    {
        return std::nullopt;
    }
    size_t numberOfTuples = 0;
    size_t checksum = 0;
    const auto count = contents.substr(0, separator);
    const auto sum = contents.substr(separator + 1);
    if (std::from_chars(count.data(), count.data() + count.size(), numberOfTuples).ec != std::errc{}
        or std::from_chars(sum.data(), sum.data() + sum.size(), checksum).ec != std::errc{})
    {
        return std::nullopt;
    }
    return std::pair{numberOfTuples, checksum};
}

void persistChecksum(
    const CheckpointManager& checkpointManager,
    const QueryId& queryId,
    const uint64_t round,
    const size_t numberOfTuples,
    const size_t checksum)
{
    checkpointManager.persistFile(
        checkpointManager.getRoundDirectory(queryId, round) / SinkCheckpointFileName, fmt::format("{},{}", numberOfTuples, checksum));
}
}

ChecksumSink::ChecksumSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , isOpen(false)
    , outputFilePath(sinkDescriptor.getFromConfig(ConfigParametersChecksum::FILE_PATH))
{
}

ChecksumSink::~ChecksumSink()
{
    if (checkpointCallback)
    {
        checkpointManager->unregisterCallback(*checkpointCallback);
    }
}

void ChecksumSink::enableCheckpointing(std::shared_ptr<CheckpointManager> checkpointManager, QueryId queryId)
{
    this->checkpointManager = std::move(checkpointManager);
    checkpointedQuery = std::move(queryId);
}

void ChecksumSink::restoreChecksum()
{
    if (not checkpointManager or not checkpointManager->shouldRecoverFromCheckpoint())
    {
        return;
    }
    const auto recoveryRound = checkpointManager->getRecoveryRoundDirectory(*checkpointedQuery);
    if (not recoveryRound)
    {
        return;
    }
    const auto contents = CheckpointManager::loadFile(*recoveryRound / SinkCheckpointFileName);
    if (not contents)
    {
        return;
    }
    const auto restored = parseChecksumCheckpoint(*contents);
    if (not restored)
    {
        throw CheckpointError("Cannot read the checksum checkpointed in {}", recoveryRound->string());
    }
    checksum.numberOfTuples = restored->first;
    checksum.checksum = restored->second;
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

    restoreChecksum();
    if (checkpointManager and checkpointManager->isCheckpointingEnabled())
    {
        checkpointCallback = CheckpointManager::getQueryCallbackPrefix(*checkpointedQuery) + "sink";
        checkpointManager->registerCallback(
            *checkpointCallback,
            [this](const uint64_t round)
            {
                const std::unique_lock lock(checksumMutex);
                persistChecksum(*checkpointManager, *checkpointedQuery, round, checksum.numberOfTuples, checksum.checksum);
            },
            CheckpointManager::CallbackPhase::Prepare);
    }
}

void ChecksumSink::stop(PipelineExecutionContext&)
{
    NES_INFO("Checksum Sink completed. Checksum: {}", fmt::streamed(checksum));
    if (checkpointCallback)
    {
        /// The rounds until the query stopped must still restore the final checksum of a recovered query.
        checkpointManager->registerCallback(
            *checkpointCallback,
            [checkpointManager = checkpointManager.get(),
             queryId = *checkpointedQuery,
             numberOfTuples = checksum.numberOfTuples.load(),
             checksumValue = checksum.checksum.load()](const uint64_t round)
            { persistChecksum(*checkpointManager, queryId, round, numberOfTuples, checksumValue); },
            CheckpointManager::CallbackPhase::Prepare);
        checkpointCallback.reset();
    }

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
    const std::shared_lock lock(checksumMutex);

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

DescriptorConfig::Config ChecksumSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersChecksum>(std::move(config), NAME);
}

}
