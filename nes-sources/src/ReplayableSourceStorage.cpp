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
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <ios>
#include <map>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
constexpr std::string_view PruneMetaFileName = "pruned.meta";
constexpr std::string_view BufferFileExtension = ".bin";

std::optional<SequenceNumber::Underlying> parseBufferFileName(const std::filesystem::path& file)
{
    if (file.extension() != BufferFileExtension)
    {
        return std::nullopt;
    }
    const auto stem = file.stem().string();
    SequenceNumber::Underlying sequence = 0;
    const auto [end, error] = std::from_chars(stem.data(), stem.data() + stem.size(), sequence);
    if (error != std::errc{} || end != stem.data() + stem.size())
    {
        return std::nullopt;
    }
    return sequence;
}
}

ReplayableSourceStorage::ReplayableSourceStorage(std::filesystem::path directory) : directory(std::move(directory))
{
    std::filesystem::create_directories(this->directory);
    if (std::ifstream meta(this->directory / PruneMetaFileName, std::ios::binary); meta.is_open())
    {
        meta.read(reinterpret_cast<char*>(&pruned), sizeof(pruned)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        if (!meta)
        {
            throw CheckpointError("Corrupt source replay storage in {}", this->directory.string());
        }
    }
    std::vector<std::filesystem::path> leftovers;
    for (const auto& entry : std::filesystem::directory_iterator(this->directory))
    {
        const auto sequence = parseBufferFileName(entry.path());
        if (sequence && *sequence > pruned.sequence)
        {
            bufferSizes.emplace(*sequence, entry.file_size());
        }
        else if (sequence || entry.path().extension() == ".tmp")
        {
            leftovers.push_back(entry.path());
        }
    }
    /// A crash while pruning or persisting leaves these files behind.
    for (const auto& leftover : leftovers)
    {
        std::error_code errorCode;
        std::filesystem::remove(leftover, errorCode);
    }
}

std::filesystem::path ReplayableSourceStorage::getBufferFile(const SequenceNumber::Underlying persistedSequence) const
{
    return directory / fmt::format("{:020}{}", persistedSequence, BufferFileExtension);
}

void ReplayableSourceStorage::writePruneMeta() const
{
    const auto metaFile = directory / PruneMetaFileName;
    const auto temporaryMetaFile = std::filesystem::path(metaFile.string() + ".tmp");
    {
        std::ofstream out(temporaryMetaFile, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(&pruned), sizeof(pruned)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        out.close();
        if (!out)
        {
            throw CheckpointError("Cannot write source replay storage {}", temporaryMetaFile.string());
        }
    }
    std::filesystem::rename(temporaryMetaFile, metaFile);
}

void ReplayableSourceStorage::persist(const SequenceNumber::Underlying persistedSequence, const TupleBuffer& buffer)
{
    const auto payload = buffer.getAvailableMemoryArea<char>().first(buffer.getNumberOfTuples());
    const auto bufferFile = getBufferFile(persistedSequence);
    /// A crash while writing must not leave a partial buffer behind, which a recovered source would replay.
    const auto temporaryBufferFile = std::filesystem::path(bufferFile.string() + ".tmp");
    {
        std::ofstream out(temporaryBufferFile, std::ios::binary | std::ios::trunc);
        out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
        out.close();
        if (!out)
        {
            throw CheckpointError("Cannot persist source buffer {} in {}", persistedSequence, directory.string());
        }
    }
    std::filesystem::rename(temporaryBufferFile, bufferFile);
    const std::scoped_lock lock(mutex);
    bufferSizes.insert_or_assign(persistedSequence, payload.size());
}

void ReplayableSourceStorage::prune(const SequenceNumber::Underlying persistedSequence)
{
    const std::scoped_lock lock(mutex);
    if (persistedSequence <= pruned.sequence)
    {
        return;
    }
    const auto prunedEnd = bufferSizes.upper_bound(persistedSequence);
    for (auto buffer = bufferSizes.begin(); buffer != prunedEnd; ++buffer)
    {
        pruned.byteOffset += buffer->second;
    }
    pruned.sequence = persistedSequence;
    writePruneMeta();
    for (auto buffer = bufferSizes.begin(); buffer != prunedEnd; ++buffer)
    {
        std::error_code errorCode;
        std::filesystem::remove(getBufferFile(buffer->first), errorCode);
    }
    bufferSizes.erase(bufferSizes.begin(), prunedEnd);
}

uint64_t ReplayableSourceStorage::getByteOffsetAfter(const SequenceNumber::Underlying persistedSequence) const
{
    const std::scoped_lock lock(mutex);
    if (persistedSequence < pruned.sequence)
    {
        throw CheckpointError("Source buffers up to {} were already pruned from {}", pruned.sequence, directory.string());
    }
    uint64_t byteOffset = pruned.byteOffset;
    for (auto buffer = bufferSizes.begin(); buffer != bufferSizes.upper_bound(persistedSequence); ++buffer)
    {
        byteOffset += buffer->second;
    }
    return byteOffset;
}

uint64_t ReplayableSourceStorage::replayAfter(
    const SequenceNumber::Underlying persistedSequence,
    AbstractBufferProvider& bufferProvider,
    const std::function<void(SequenceNumber::Underlying persistedSequence, TupleBuffer&& buffer)>& emit,
    const std::stop_token& stopToken) const
{
    std::map<SequenceNumber::Underlying, uint64_t> buffers;
    {
        const std::scoped_lock lock(mutex);
        if (persistedSequence < pruned.sequence)
        {
            throw CheckpointError("Source buffers up to {} were already pruned from {}", pruned.sequence, directory.string());
        }
        buffers.insert(bufferSizes.upper_bound(persistedSequence), bufferSizes.end());
    }
    uint64_t replayedBuffers = 0;
    for (const auto& [sequence, size] : buffers)
    {
        if (stopToken.stop_requested())
        {
            break;
        }
        auto buffer = bufferProvider.getBufferBlocking();
        auto payload = buffer.getAvailableMemoryArea<char>();
        if (size > payload.size())
        {
            throw CheckpointError(
                "Persisted source buffer {} of {} bytes does not fit into a buffer of {} bytes", sequence, size, payload.size());
        }
        std::ifstream in(getBufferFile(sequence), std::ios::binary);
        in.read(payload.data(), static_cast<std::streamsize>(size));
        if (!in)
        {
            throw CheckpointError("Cannot read persisted source buffer {} in {}", sequence, directory.string());
        }
        buffer.setNumberOfTuples(size);
        emit(sequence, std::move(buffer));
        ++replayedBuffers;
    }
    return replayedBuffers;
}

}
