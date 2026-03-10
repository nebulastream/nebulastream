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

ReplayableSourceStorage::ReplayableSourceStorage(OriginId originId, std::filesystem::path storageDirectory, PhysicalSourceId physicalSourceId)
    : originId(originId)
    , physicalSourceId(physicalSourceId)
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

    FileHeader header{};
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

void ReplayableSourceStorage::writeCheckpointMeta(const SequenceNumber sequenceNumber, const uint64_t byteOffset)
{
    std::ofstream out(metaFilePath, std::ios::binary | std::ios::trunc);
    if (!out.is_open())
    {
        NES_WARNING("ReplayableSourceStorage: Cannot open {} for writing", metaFilePath.string());
        return;
    }
    const CheckpointMeta meta{.sequence = sequenceNumber.getRawValue(), .byteOffset = byteOffset};
    out.write(reinterpret_cast<const char*>(&meta), sizeof(meta));
}

std::optional<SequenceNumber::Underlying> ReplayableSourceStorage::loadLastCheckpointedSequence()
{
    std::scoped_lock lock(mutex);
    if (!std::filesystem::exists(metaFilePath))
    {
        lastCheckpointed.reset();
        lastCheckpointedByteOffset.reset();
        return std::nullopt;
    }

    std::ifstream in(metaFilePath, std::ios::binary);
    if (!in.is_open())
    {
        NES_WARNING("ReplayableSourceStorage: Cannot open {} for reading", metaFilePath.string());
        lastCheckpointed.reset();
        lastCheckpointedByteOffset.reset();
        return std::nullopt;
    }

    CheckpointMeta meta{};
    in.read(reinterpret_cast<char*>(&meta), sizeof(meta));
    if (in.gcount() == static_cast<std::streamsize>(sizeof(meta)))
    {
        lastCheckpointed = meta.sequence;
        lastCheckpointedByteOffset = meta.byteOffset;
    }
    else if (in.gcount() == static_cast<std::streamsize>(sizeof(SequenceNumber::Underlying)))
    {
        lastCheckpointed = meta.sequence;
        lastCheckpointedByteOffset = 0;
    }
    else
    {
        lastCheckpointed.reset();
        lastCheckpointedByteOffset.reset();
    }
    return lastCheckpointed;
}

std::optional<uint64_t> ReplayableSourceStorage::loadLastCheckpointedByteOffset()
{
    std::scoped_lock lock(mutex);
    if (!lastCheckpointedByteOffset.has_value() && std::filesystem::exists(metaFilePath))
    {
        std::ifstream in(metaFilePath, std::ios::binary);
        if (!in.is_open())
        {
            return std::nullopt;
        }

        CheckpointMeta meta{};
        in.read(reinterpret_cast<char*>(&meta), sizeof(meta));
        if (in.gcount() == static_cast<std::streamsize>(sizeof(meta)))
        {
            lastCheckpointed = meta.sequence;
            lastCheckpointedByteOffset = meta.byteOffset;
        }
        else if (in.gcount() == static_cast<std::streamsize>(sizeof(SequenceNumber::Underlying)))
        {
            lastCheckpointed = meta.sequence;
            lastCheckpointedByteOffset = 0;
        }
    }
    return lastCheckpointedByteOffset;
}

std::optional<uint64_t> ReplayableSourceStorage::loadRecoveryByteOffset()
{
    std::scoped_lock lock(mutex);
    if (!lastCheckpointedByteOffset.has_value() && std::filesystem::exists(metaFilePath))
    {
        std::ifstream in(metaFilePath, std::ios::binary);
        if (!in.is_open())
        {
            return std::nullopt;
        }

        CheckpointMeta meta{};
        in.read(reinterpret_cast<char*>(&meta), sizeof(meta));
        if (in.gcount() == static_cast<std::streamsize>(sizeof(meta)))
        {
            lastCheckpointed = meta.sequence;
            lastCheckpointedByteOffset = meta.byteOffset;
        }
        else if (in.gcount() == static_cast<std::streamsize>(sizeof(SequenceNumber::Underlying)))
        {
            lastCheckpointed = meta.sequence;
            lastCheckpointedByteOffset = 0;
        }
    }

    const auto files = listPersistedFiles();
    if (!lastCheckpointedByteOffset.has_value() && files.empty())
    {
        return std::nullopt;
    }

    uint64_t byteOffset = lastCheckpointedByteOffset.value_or(0);
    for (const auto& file : files)
    {
        if (lastCheckpointed && file.sequence <= *lastCheckpointed)
        {
            continue;
        }
        byteOffset += file.dataSize;
    }
    return byteOffset;
}

void ReplayableSourceStorage::pruneCoveredFiles(SequenceNumber::Underlying checkpointSequence)
{
    auto files = listPersistedFiles();
    for (const auto& file : files)
    {
        if (file.sequence <= checkpointSequence)
        {
            std::error_code ec;
            std::filesystem::remove(file.path, ec);
        }
    }
}

std::vector<ReplayableSourceStorage::PersistedFileInfo> ReplayableSourceStorage::listPersistedFiles()
{
    std::vector<PersistedFileInfo> files;
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

        files.push_back(PersistedFileInfo{.sequence = header.sequence, .dataSize = header.dataSize, .path = entry.path()});
    }

    std::sort(files.begin(), files.end(), [](const auto& lhs, const auto& rhs) { return lhs.sequence < rhs.sequence; });
    return files;
}

void ReplayableSourceStorage::markCheckpoint(SequenceNumber sequenceNumber)
{
    std::scoped_lock lock(mutex);
    const auto checkpointValue = sequenceNumber.getRawValue();
    if (lastCheckpointed && checkpointValue < *lastCheckpointed)
    {
        return;
    }
    uint64_t byteOffset = lastCheckpointedByteOffset.value_or(0);
    for (const auto& file : listPersistedFiles())
    {
        if (lastCheckpointed && file.sequence <= *lastCheckpointed)
        {
            continue;
        }
        if (file.sequence > checkpointValue)
        {
            break;
        }
        byteOffset += file.dataSize;
    }
    lastCheckpointed = checkpointValue;
    lastCheckpointedByteOffset = byteOffset;
    writeCheckpointMeta(sequenceNumber, byteOffset);
    pruneCoveredFiles(checkpointValue);
}

std::optional<SequenceNumber> ReplayableSourceStorage::replayPending(
    AbstractBufferProvider& bufferProvider,
    const detail::EmitFn& emit,
    const std::stop_token& stopToken)
{
    std::scoped_lock lock(mutex);

    auto files = listPersistedFiles();
    const auto checkpointSequence = lastCheckpointed;
    std::optional<SequenceNumber> lastReplayed;
    for (const auto& file : files)
    {
        if (stopToken.stop_requested())
        {
            continue;
        }

        if (checkpointSequence && file.sequence <= *checkpointSequence)
        {
            continue;
        }

        std::ifstream in(file.path, std::ios::binary);
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
        lastReplayed = SequenceNumber(header.sequence);
    }
    return lastReplayed;
}

}
