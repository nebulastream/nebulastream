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
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>

#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <SinksParsing/SchemaFormatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <QueryId.hpp>

namespace NES
{

FileSink::FileSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , outputFilePath(sinkDescriptor.getFromConfig(ConfigParametersFile::FILE_PATH))
    , isAppend(sinkDescriptor.getFromConfig(ConfigParametersFile::APPEND))
    , isOpen(false)
    , schemaFormatter(
          SchemaFormatter(NES::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptor.getSchema())))
{
}

std::ostream& FileSink::toString(std::ostream& str) const
{
    str << fmt::format("FileSink(filePathOutput: {}, isAppend: {})", outputFilePath, isAppend);
    return str;
}

FileSink::~FileSink()
{
    unregisterCheckpointCallback();
}

void FileSink::enableCheckpointing(std::shared_ptr<CheckpointManager> checkpointManager, QueryId queryId)
{
    this->checkpointManager = std::move(checkpointManager);
    checkpointedQuery = std::move(queryId);
}

std::optional<uint64_t> FileSink::getRecoveredOutputLength() const
{
    if (not checkpointManager or not checkpointManager->shouldRecoverFromCheckpoint())
    {
        return std::nullopt;
    }
    const auto recoveryRound = checkpointManager->getRecoveryRoundDirectory(*checkpointedQuery);
    if (not recoveryRound)
    {
        return std::nullopt;
    }
    return CheckpointManager::loadFile(*recoveryRound / "sink").transform([](const auto& length) { return std::stoull(length); });
}

void FileSink::unregisterCheckpointCallback()
{
    if (checkpointCallback)
    {
        checkpointManager->unregisterCallback(*checkpointCallback);
        checkpointCallback.reset();
    }
}

void FileSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up file sink: {}", *this);
    const auto recoveredOutputLength = getRecoveredOutputLength();
    const auto stream = outputFileStream.wlock();
    /// A recovered query emits again what it emitted after its checkpoint.
    if (recoveredOutputLength and std::filesystem::exists(outputFilePath))
    {
        std::filesystem::resize_file(outputFilePath, *recoveredOutputLength);
    }
    /// Remove an existing file unless the isAppend mode is isAppend.
    else if (!isAppend)
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

    if (checkpointManager and checkpointManager->isCheckpointingEnabled())
    {
        checkpointCallback = CheckpointManager::getQueryCallbackPrefix(*checkpointedQuery) + "sink";
        checkpointManager->registerCallback(
            *checkpointCallback,
            [this](const uint64_t round)
            {
                const auto length = [this]
                {
                    const auto lockedStream = outputFileStream.wlock();
                    lockedStream->flush();
                    return static_cast<uint64_t>(lockedStream->tellp());
                }();
                checkpointManager->persistFile(
                    checkpointManager->getRoundDirectory(*checkpointedQuery, round) / "sink", std::to_string(length));
            },
            CheckpointManager::CallbackPhase::Prepare);
    }
}

void FileSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in FileSink.");
    PRECONDITION(isOpen, "Sink was not opened");

    {
        const auto wlocked = outputFileStream.wlock();
        /// Create a buffer iterator to help iterate through the tuplebuffer and its children
        BufferIterator iterator{inputTupleBuffer};

        std::optional<BufferIterator::BufferElement> element = iterator.getNextElement();
        while (element.has_value())
        {
            wlocked->write(
                element.value().buffer.getAvailableMemoryArea<char>().data(), static_cast<std::streamsize>(element.value().contentLength));
            /// Get the next buffer to be written
            element = iterator.getNextElement();
        }
        wlocked->flush();
    }
}

void FileSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Closing file sink, filePathOutput={}", outputFilePath);
    const auto outputLength = [this]
    {
        const auto stream = outputFileStream.wlock();
        stream->flush();
        return static_cast<uint64_t>(stream->tellp());
    }();
    keepOutputLengthInCheckpoints(outputLength);
    outputFileStream.wlock()->close();
}

void FileSink::keepOutputLengthInCheckpoints(const uint64_t outputLength)
{
    if (not checkpointCallback)
    {
        return;
    }
    /// The rounds until the query stopped must still truncate the output of a recovered query to its final length.
    checkpointManager->registerCallback(
        *checkpointCallback,
        [checkpointManager = checkpointManager.get(), queryId = *checkpointedQuery, outputLength](const uint64_t round)
        { checkpointManager->persistFile(checkpointManager->getRoundDirectory(queryId, round) / "sink", std::to_string(outputLength)); },
        CheckpointManager::CallbackPhase::Prepare);
    checkpointCallback.reset();
}

DescriptorConfig::Config FileSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersFile>(std::move(config), NAME);
}

}
