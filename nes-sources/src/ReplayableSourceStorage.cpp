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

#include <Sources/ReplayableSourceStorage.hpp>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <utility>
#include <vector>
#include <string_view>
#include <fmt/format.h>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{
namespace
{
constexpr std::string_view CheckpointMetaFileName = "checkpoint.meta";
constexpr std::string_view FileExtension = ".bin";
}

ReplayableSourceStorage::ReplayableSourceStorage(OriginId originId, std::filesystem::path storageDirectory)
    : originId(originId)
    , storageDir(std::move(storageDirectory))
    , metaFilePath(storageDir / CheckpointMetaFileName)
{
    ensureDirectory();
    loadLastCheckpointedSequence();
}

void ReplayableSourceStorage::ensureDirectory()
{
    if (!std::filesystem::exists(storageDir))
    {
        std::filesystem::create_directories(storageDir);
    }
}

void ReplayableSourceStorage::persistBuffer(const TupleBuffer& buffer)
{
    std::scoped_lock lock(mutex);
    ensureDirectory();

    FileHeader header;
    header.sequence = buffer.getSequenceNumber().getRawValue();
    header.chunk = buffer.getChunkNumber().getRawValue();
    header.lastChunk = buffer.isLastChunk() ? 1 : 0;
    header.dataSize = buffer.getNumberOfTuples();

    const auto filePath = storageDir / fmt::format("{:020}{}", header.sequence, FileExtension);
    std::ofstream out(filePath, std::ios::binary | std::ios::trunc);
    if (!out.is_open())
    {
        NES_WARNING("ReplayableSourceStorage: Cannot open {} for writing", filePath.string());
        return;
    }

    out.write(reinterpret_cast<const char*>(&header), sizeof(header));

    auto data = buffer.getAvailableMemoryArea<std::byte>();
    const auto bytesToWrite = std::min<uint64_t>(header.dataSize, data.size());
    out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(bytesToWrite));
}

void ReplayableSourceStorage::writeCheckpointMeta(SequenceNumber sequenceNumber)
{
    std::ofstream out(metaFilePath, std::ios::binary | std::ios::trunc);
    if (!out.is_open())
    {
        NES_WARNING("ReplayableSourceStorage: Cannot open {} for writing", metaFilePath.string());
        return;
    }
    const auto value = sequenceNumber.getRawValue();
    out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

std::optional<SequenceNumber::Underlying> ReplayableSourceStorage::loadLastCheckpointedSequence()
{
    std::scoped_lock lock(mutex);
    if (!std::filesystem::exists(metaFilePath))
    {
        lastCheckpointed.reset();
        return std::nullopt;
    }

    std::ifstream in(metaFilePath, std::ios::binary);
    if (!in.is_open())
    {
        NES_WARNING("ReplayableSourceStorage: Cannot open {} for reading", metaFilePath.string());
        lastCheckpointed.reset();
        return std::nullopt;
    }

    SequenceNumber::Underlying value{0};
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    if (in.gcount() == static_cast<std::streamsize>(sizeof(value)))
    {
        lastCheckpointed = value;
    }
    else
    {
        lastCheckpointed.reset();
    }
    return lastCheckpointed;
}

void ReplayableSourceStorage::pruneCoveredFiles(SequenceNumber::Underlying checkpointSequence)
{
    auto files = listPersistedFiles();
    for (const auto& [seq, path] : files)
    {
        if (seq <= checkpointSequence)
        {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
    }
}

std::vector<std::pair<SequenceNumber::Underlying, std::filesystem::path>> ReplayableSourceStorage::listPersistedFiles()
{
    std::vector<std::pair<SequenceNumber::Underlying, std::filesystem::path>> files;
    if (!std::filesystem::exists(storageDir))
    {
        return files;
    }

    for (const auto& entry : std::filesystem::directory_iterator(storageDir))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }
        if (entry.path().extension() != FileExtension)
        {
            continue;
        }

        std::ifstream in(entry.path(), std::ios::binary);
        if (!in.is_open())
        {
            continue;
        }

        FileHeader header{};
        in.read(reinterpret_cast<char*>(&header), sizeof(header));
        if (in.gcount() != static_cast<std::streamsize>(sizeof(header)))
        {
            continue;
        }

        files.emplace_back(header.sequence, entry.path());
    }

    std::sort(files.begin(), files.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    return files;
}

void ReplayableSourceStorage::markCheckpoint(SequenceNumber sequenceNumber)
{
    std::scoped_lock lock(mutex);
    lastCheckpointed = sequenceNumber.getRawValue();
    writeCheckpointMeta(sequenceNumber);
    pruneCoveredFiles(sequenceNumber.getRawValue());
}

void ReplayableSourceStorage::replayPending(
    AbstractBufferProvider& bufferProvider,
    const detail::EmitFn& emit,
    const std::stop_token& stopToken)
{
    std::scoped_lock lock(mutex);

    const auto checkpointSequence = lastCheckpointed.value_or(INVALID_SEQ_NUMBER.getRawValue());
    pruneCoveredFiles(checkpointSequence);

    auto files = listPersistedFiles();
    for (const auto& [seq, path] : files)
    {
        if (seq <= checkpointSequence || stopToken.stop_requested())
        {
            continue;
        }

        std::ifstream in(path, std::ios::binary);
        if (!in.is_open())
        {
            continue;
        }

        FileHeader header{};
        in.read(reinterpret_cast<char*>(&header), sizeof(header));
        if (in.gcount() != static_cast<std::streamsize>(sizeof(header)))
        {
            continue;
        }

        auto buffer = bufferProvider.getBufferBlocking();
        auto data = buffer.getAvailableMemoryArea<std::byte>();
        const auto bytesToCopy = std::min<uint64_t>(header.dataSize, data.size());
        in.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(bytesToCopy));

        buffer.setNumberOfTuples(bytesToCopy);
        buffer.setSequenceNumber(SequenceNumber(header.sequence));
        buffer.setChunkNumber(ChunkNumber(header.chunk));
        buffer.setLastChunk(header.lastChunk != 0);
        buffer.setOriginId(originId);

        emit(std::move(buffer), false);
    }
}

}
