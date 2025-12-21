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

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <istream>
#include <ostream>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <endian.h>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Util/Ranges.hpp>
#include <netinet/in.h>
#include <ErrorHandling.hpp>

namespace NES
{

template <typename T>
void binaryRead(std::istream& istream, T& value)
{
    static_assert(std::is_trivially_copyable_v<T>, "Type must be safe to copy via bytes");
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    istream.read(reinterpret_cast<char*>(&value), sizeof(T));
}

template <typename T>
std::ostream& binaryWrite(std::ostream& os, const T& value)
{
    static_assert(std::is_trivially_copyable_v<T>, "Type must be safe to copy via bytes");
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    os.write(reinterpret_cast<const char*>(&value), sizeof(T));
    return os;
}

struct BinaryFileHeader
{
    uint64_t numberOfBuffers = 0;

    std::ostream& serialize(std::ostream& stream) const { return binaryWrite(stream, htobe64(numberOfBuffers)); }

    std::istream& deserialize(std::istream& stream)
    {
        uint64_t value{};
        binaryRead(stream, value);
        numberOfBuffers = be64toh(value);
        return stream;
    }
};

struct BinaryBufferHeader
{
    uint64_t numberOfTuples = 0;
    uint64_t bufferSize = 0;
    uint64_t numberChildBuffers = 0;
    SequenceNumber::Underlying sequenceNumber = 0;
    ChunkNumber::Underlying chunkNumber = 0;

    std::ostream& serialize(std::ostream& stream) const
    {
        return binaryWrite(
            stream,
            std::to_array(
                {htobe64(numberOfTuples),
                 htobe64(bufferSize),
                 htobe64(numberChildBuffers),
                 htobe64(sequenceNumber),
                 htobe64(chunkNumber)}));
    }

    std::istream& deserialize(std::istream& stream)
    {
        std::array<uint64_t, 5> values{};
        binaryRead(stream, values);
        numberOfTuples = be64toh(values[0]);
        bufferSize = be64toh(values[1]);
        numberChildBuffers = be64toh(values[2]);
        sequenceNumber = be64toh(values[3]);
        chunkNumber = be64toh(values[4]);
        return stream;
    }
};

struct BinaryChildBufferHeader
{
    uint64_t bufferSize = 0;

    std::ostream& serialize(std::ostream& stream) const { return binaryWrite(stream, htobe64(bufferSize)); }

    std::istream& deserialize(std::istream& stream)
    {
        uint64_t value{};
        binaryRead(stream, value);
        bufferSize = be64toh(value);
        return stream;
    }
};

enum class FileOpenMode : uint8_t
{
    APPEND,
    TRUNCATE,
};

inline std::optional<std::filesystem::path>
findFileByName(const std::string& fileNameWithoutExtension, const std::filesystem::path& directory)
{
    if (!std::filesystem::exists(directory) || !std::filesystem::is_directory(directory))
    {
        return std::nullopt;
    }

    for (const auto& entry : std::filesystem::directory_iterator(directory))
    {
        if (entry.is_regular_file())
        {
            if (std::string stem = entry.path().stem().string(); stem == fileNameWithoutExtension)
            {
                return entry.path();
            }
        }
    }

    return std::nullopt;
}

inline std::ofstream createFile(const std::filesystem::path& filepath, const FileOpenMode fileOpenMode)
{
    /// Ensure the directory exists
    if (const auto parentPath = filepath.parent_path(); not parentPath.empty())
    {
        create_directories(parentPath);
    }

    /// Set up file open mode flags
    std::ios_base::openmode openMode = std::ios::binary;
    switch (fileOpenMode)
    {
        case FileOpenMode::APPEND:
            openMode |= std::ios::app;
            break;
        case FileOpenMode::TRUNCATE:
            openMode |= std::ios::trunc;
            break;
    }

    /// Open the file with appropriate mode
    std::ofstream file(filepath, openMode);
    if (not file)
    {
        throw InvalidConfigParameter("Could not open file: {}", filepath.string());
    }
    return file;
}

inline void sortTupleBuffers(std::vector<TupleBuffer>& buffers)
{
    std::ranges::sort(
        buffers.begin(),
        buffers.end(),
        [](const TupleBuffer& left, const TupleBuffer& right)
        {
            return SequenceData{left.getSequenceNumber(), left.getChunkNumber(), left.isLastChunk()}
            < SequenceData{right.getSequenceNumber(), right.getChunkNumber(), right.isLastChunk()};
        });
}

inline void writeTupleBuffersToFile(std::vector<TupleBuffer>& resultBufferVec, const std::filesystem::path& actualResultFilePath)
{
    sortTupleBuffers(resultBufferVec);
    auto file = createFile(actualResultFilePath, FileOpenMode::TRUNCATE);
    BinaryFileHeader{.numberOfBuffers = resultBufferVec.size()}.serialize(file);
    for (const auto& buffer : resultBufferVec)
    {
        BinaryBufferHeader{
            .numberOfTuples = buffer.getNumberOfTuples(),
            .bufferSize = buffer.getAvailableMemoryArea().size(),
            .numberChildBuffers = buffer.getNumberOfChildBuffers(),
            .sequenceNumber = buffer.getSequenceNumber().getRawValue(),
            .chunkNumber = buffer.getChunkNumber().getRawValue()}
            .serialize(file);
        file.write(buffer.getAvailableMemoryArea<const char>().data(), buffer.getAvailableMemoryArea().size());
        for (size_t childIndex = 0; childIndex < buffer.getNumberOfChildBuffers(); childIndex++)
        {
            auto child = buffer.loadChildBuffer(childIndex);
            BinaryChildBufferHeader{.bufferSize = child.getAvailableMemoryArea().size()}.serialize(file);
            file.write(child.getAvailableMemoryArea<const char>().data(), child.getAvailableMemoryArea().size());
        }
    }

    file.close();
}

inline std::vector<TupleBuffer> loadTupleBuffersFromFile(AbstractBufferProvider& bufferProvider, const std::filesystem::path& filepath)
{
    auto allocateBuffer = [&](size_t bufferSize)
    {
        if (bufferSize == bufferProvider.getBufferSize())
        {
            return bufferProvider.getBufferBlocking();
        }
        return bufferProvider.getUnpooledBuffer(bufferSize).value();
    };

    std::ifstream file(filepath);
    if (!file)
    {
        return {};
    }

    auto buffers = std::vector<TupleBuffer>();
    BinaryFileHeader header{};
    header.deserialize(file);

    buffers.reserve(header.numberOfBuffers);

    for (size_t bufferIdx = 0; bufferIdx < header.numberOfBuffers; bufferIdx++)
    {
        BinaryBufferHeader bufferHeader{};
        bufferHeader.deserialize(file);

        auto buffer = allocateBuffer(bufferHeader.bufferSize);
        buffer.setChunkNumber(ChunkNumber(bufferHeader.chunkNumber));
        buffer.setSequenceNumber(SequenceNumber(bufferHeader.sequenceNumber));
        buffer.setNumberOfTuples(bufferHeader.numberOfTuples);
        file.read(buffer.getAvailableMemoryArea<char>().data(), bufferHeader.bufferSize);
        for (size_t childBufferIndex = 0; childBufferIndex < bufferHeader.numberChildBuffers; childBufferIndex++)
        {
            BinaryChildBufferHeader childBufferHeader{};
            childBufferHeader.deserialize(file);
            auto child = allocateBuffer(childBufferHeader.bufferSize);
            file.read(child.getAvailableMemoryArea<char>().data(), childBufferHeader.bufferSize);
            [[maybe_unused]] auto index = buffer.storeChildBuffer(child);
        }
        buffers.emplace_back(std::move(buffer));
    }

    return buffers;
}
}
