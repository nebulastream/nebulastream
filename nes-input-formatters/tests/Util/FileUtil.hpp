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

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <istream>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <Util/Ranges.hpp>
#include <ErrorHandling.hpp>
#include "Util/Files.hpp"

namespace NES
{

template <typename T>
requires std::integral<T> || std::floating_point<T>
void write_le(std::ostream& os, T val)
{
    if constexpr (std::endian::native == std::endian::little)
    {
        os.write(reinterpret_cast<const char*>(&val), sizeof(T));
        return;
    }

    if constexpr (std::integral<T>)
    {
        auto swapped = std::byteswap(val);
        os.write(reinterpret_cast<const char*>(&swapped), sizeof(T));
    }
    else if constexpr (sizeof(T) == 4)
    {
        auto swapped = std::bit_cast<uint32_t>(std::byteswap(std::bit_cast<uint32_t>(val)));
        os.write(reinterpret_cast<const char*>(&swapped), sizeof(T));
    }
    else if constexpr (sizeof(T) == 8)
    {
        auto swapped = std::bit_cast<uint64_t>(std::byteswap(std::bit_cast<uint64_t>(val)));
        os.write(reinterpret_cast<const char*>(&swapped), sizeof(T));
    }
}

template <typename T>
requires std::integral<T> || std::floating_point<T>
T read_le(std::istream& is)
{
    std::array<char, sizeof(T)> buf;
    is.read(buf.data(), sizeof(T));

    if constexpr (std::endian::native == std::endian::little)
    {
        return std::bit_cast<T>(buf);
    }

    if constexpr (std::integral<T>)
    {
        auto swapped = std::bit_cast<T>(std::byteswap(std::bit_cast<T>(buf)));
        return swapped;
    }
    else if constexpr (sizeof(T) == 4)
    {
        auto swapped = std::bit_cast<uint32_t>(std::byteswap(std::bit_cast<uint32_t>(buf)));
        return std::bit_cast<T>(swapped);
    }
    else if constexpr (sizeof(T) == 8)
    {
        auto swapped = std::bit_cast<uint64_t>(std::byteswap(std::bit_cast<uint64_t>(buf)));
        return std::bit_cast<T>(swapped);
    }
}

void writeBuffersToDisk(const std::filesystem::path& filePath, const std::vector<NES::TupleBuffer>& buffers, size_t schemaSizeInBytes)
{
    std::ofstream file(filePath, std::ios::binary);
    if (not file)
    {
        throw std::runtime_error(fmt::format("Could not open file: {} while writing: {}", filePath, getErrorMessageFromERRNO()));
    }

    write_le<size_t>(file, buffers.size());
    for (const auto& buffer : buffers)
    {
        write_le<size_t>(file, buffer.getNumberOfTuples() * schemaSizeInBytes);
        write_le<size_t>(file, buffer.getSequenceRange().start[0]);
        write_le<size_t>(file, buffer.getSequenceRange().start[1]);
        write_le<size_t>(file, buffer.getSequenceRange().start[2]);
        write_le<size_t>(file, buffer.getSequenceRange().end[0]);
        write_le<size_t>(file, buffer.getSequenceRange().end[1]);
        write_le<size_t>(file, buffer.getSequenceRange().end[2]);
        write_le<size_t>(file, buffer.getNumberOfTuples());
        write_le<size_t>(file, buffer.getNumberOfChildBuffers());
        file.write(buffer.getAvailableMemoryArea<char>().data(), buffer.getNumberOfTuples() * schemaSizeInBytes);

        for (size_t i = 0; i < buffer.getNumberOfChildBuffers(); ++i)
        {
            auto child = buffer.loadChildBuffer(NES::VariableSizedAccess::Index(i));
            write_le<size_t>(file, child.getAvailableMemoryArea().size());
            write_le<size_t>(file, child.getNumberOfTuples());
            file.write(child.getAvailableMemoryArea<char>().data(), child.getAvailableMemoryArea().size());
        }
    }
}

std::vector<NES::TupleBuffer> readBuffersToDisk(const std::filesystem::path& filePath, NES::AbstractBufferProvider& bufferProvider)
{
    std::ifstream file(filePath, std::ios::binary);
    if (not file)
    {
        throw std::runtime_error(fmt::format("Could not open file: {} while reading: {}", filePath, getErrorMessageFromERRNO()));
    }

    const auto numberOfBuffers = read_le<size_t>(file);
    std::vector<NES::TupleBuffer> result;
    result.reserve(numberOfBuffers);

    for (size_t i = 0; i < numberOfBuffers; ++i)
    {
        const auto bufferSize = read_le<size_t>(file);
        INVARIANT(bufferProvider.getBufferSize() >= bufferSize, "Buffer size mismatch");

        const auto sequenceStart0 = read_le<size_t>(file);
        const auto sequenceStart1 = read_le<size_t>(file);
        const auto sequenceStart2 = read_le<size_t>(file);
        const auto sequenceEnd0 = read_le<size_t>(file);
        const auto sequenceEnd1 = read_le<size_t>(file);
        const auto sequenceEnd2 = read_le<size_t>(file);
        const auto numberOfTuples = read_le<size_t>(file);
        const auto numberOfChildren = read_le<size_t>(file);

        auto buffer = bufferProvider.getBufferBlocking();
        file.read(buffer.getAvailableMemoryArea<char>().data(), bufferSize);
        buffer.setSequenceRange(NES::SequenceRange(
            NES::FracturedNumber({sequenceStart0, sequenceStart1, sequenceStart2}),
            NES::FracturedNumber({sequenceEnd0, sequenceEnd1, sequenceEnd2})));
        buffer.setNumberOfTuples(numberOfTuples);

        for (size_t j = 0; j < numberOfChildren; ++j)
        {
            const auto childBufferSize = read_le<size_t>(file);
            const auto numberOfTuples = read_le<size_t>(file);
            auto childBuffer = bufferProvider.getUnpooledBuffer(childBufferSize).value();
            childBuffer.setNumberOfTuples(numberOfTuples);
            file.read(childBuffer.getAvailableMemoryArea<char>().data(), childBufferSize);
            std::ignore = buffer.storeChildBuffer(childBuffer);
        }
        result.emplace_back(std::move(buffer));
    }

    return result;
}

struct BinaryFileHeader
{
    uint64_t numberOfBuffers = 0;
    uint64_t sizeOfBuffersInBytes = 4096;
};

struct BinaryBufferHeader
{
    uint64_t numberOfTuples = 0;
    uint64_t numberChildBuffers = 0;
    uint64_t sequenceRangeStart = 0;
    uint64_t sequenceRangeStartFracture = 0;
    uint64_t sequenceRangeEnd = 0;
    uint64_t sequenceRangeEndFracture = 0;
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

inline void writeFileHeaderToFile(const BinaryFileHeader& binaryFileHeader, const std::filesystem::path& filepath)
{
    /// create new file and write file header
    auto file = createFile(filepath, FileOpenMode::TRUNCATE);
    file.write(std::bit_cast<const char*>(&binaryFileHeader), sizeof(BinaryFileHeader));
    file.close();
}

inline void writePagedSizeBufferChunkToFile(
    const BinaryBufferHeader& binaryHeader,
    const size_t sizeOfSchemaInBytes,
    const std::span<TupleBuffer> pagedSizeBufferChunk,
    std::ofstream& file)
{
    file.write(std::bit_cast<const char*>(&binaryHeader), sizeof(BinaryBufferHeader));

    for (const auto& buffer : pagedSizeBufferChunk)
    {
        const auto sizeOfBufferInBytes = buffer.getNumberOfTuples() * sizeOfSchemaInBytes;
        file.write(buffer.getAvailableMemoryArea<const char>().data(), static_cast<std::streamsize>(sizeOfBufferInBytes));
    }
}

/// @return Variable sized data as a string
inline std::string readVarSizedDataAsString(const TupleBuffer& tupleBuffer, VariableSizedAccess variableSizedAccess)
{
    /// Retrieve the variable sized value as span over its bytes
    const auto varSizedSpan = TupleBufferRef::loadAssociatedVarSizedValue(tupleBuffer, variableSizedAccess);
    const auto* const strPtrContent = reinterpret_cast<const char*>(varSizedSpan.data());
    return std::string{strPtrContent, variableSizedAccess.getSize().getRawSize()};
}

inline void writePagedSizeTupleBufferChunkToFile(
    std::span<TupleBuffer> pagedSizeBufferChunk,
    const SequenceRange& sequenceRange,
    const size_t numTuplesInChunk,
    std::ofstream& appendFile,
    const size_t sizeOfSchemaInBytes,
    const std::vector<size_t>& varSizedFieldOffsets)
{
    const auto numChildBuffers = numTuplesInChunk * varSizedFieldOffsets.size();
    writePagedSizeBufferChunkToFile(
        {
            .numberOfTuples = numTuplesInChunk,
            .numberChildBuffers = numChildBuffers,
            .sequenceRangeStart = sequenceRange.start[0],
            .sequenceRangeStartFracture = sequenceRange.start[1],
            .sequenceRangeEnd = sequenceRange.end[0],
            .sequenceRangeEndFracture = sequenceRange.end[1],
        },
        sizeOfSchemaInBytes,
        pagedSizeBufferChunk,
        appendFile);

    if (not varSizedFieldOffsets.empty())
    {
        for (auto& buffer : pagedSizeBufferChunk)
        {
            for (size_t tupleIdx = 0; tupleIdx < buffer.getNumberOfTuples(); ++tupleIdx)
            {
                for (const auto& varSizedFieldOffset : varSizedFieldOffsets)
                {
                    const auto currentTupleOffset = tupleIdx * sizeOfSchemaInBytes;
                    const auto currentTupleVarSizedFieldOffset = currentTupleOffset + varSizedFieldOffset;
                    const VariableSizedAccess varSizedAccess{
                        *reinterpret_cast<VariableSizedAccess*>(buffer.getAvailableMemoryArea().data() + currentTupleVarSizedFieldOffset)};
                    const auto variableSizedData = readVarSizedDataAsString(buffer, varSizedAccess);
                    appendFile.write(variableSizedData.data(), static_cast<std::streamsize>(variableSizedData.size()));
                }
            }
        }
    }
}

struct TupleBufferChunk
{
    size_t lastBufferInChunkIdx;
    size_t numTuplesInChunk = 0;
};

inline void sortTupleBuffers(std::vector<TupleBuffer>& buffers)
{
    std::ranges::sort(
        buffers.begin(),
        buffers.end(),
        [](const TupleBuffer& left, const TupleBuffer& right) { return left.getSequenceRange() < right.getSequenceRange(); });
}

inline void writeTupleBuffersToFile(
    std::vector<TupleBuffer>& resultBufferVec,
    const Schema& schema,
    const std::filesystem::path& actualResultFilePath,
    const std::vector<size_t>& varSizedFieldOffsets)
{
    sortTupleBuffers(resultBufferVec);
    const auto sizeOfSchemaInBytes = schema.getSizeOfSchemaInBytes();

    const std::vector<TupleBufferChunk> pagedSizedChunkOffsets
        = [](const std::vector<TupleBuffer>& resultBufferVec, const size_t sizeOfSchemaInBytes)
    {
        size_t numBytesInNextChunk = 0;
        size_t numTuplesInNextChunk = 0;
        std::vector<TupleBufferChunk> offsets;
        for (const auto& [bufferIdx, buffer] : resultBufferVec | NES::views::enumerate)
        {
            if (const auto sizeOfCurrentBufferInBytes = buffer.getNumberOfTuples() * sizeOfSchemaInBytes;
                numBytesInNextChunk + sizeOfCurrentBufferInBytes > 4096)
            {
                offsets.emplace_back(bufferIdx, numTuplesInNextChunk);
                numBytesInNextChunk = buffer.getNumberOfTuples() * sizeOfSchemaInBytes;
                numTuplesInNextChunk = buffer.getNumberOfTuples();
            }
            else
            {
                numBytesInNextChunk += sizeOfCurrentBufferInBytes;
                numTuplesInNextChunk += buffer.getNumberOfTuples();
            }
        }
        offsets.emplace_back(resultBufferVec.size(), numTuplesInNextChunk);
        return offsets;
    }(resultBufferVec, sizeOfSchemaInBytes);

    writeFileHeaderToFile({.numberOfBuffers = pagedSizedChunkOffsets.size()}, actualResultFilePath);
    auto appendFile = createFile(actualResultFilePath, FileOpenMode::APPEND);

    size_t nextChunkStart = 0;
    uint64_t nextSeq = 1;
    for (const auto& [lastBufferIdxInChunk, numTuplesInChunk] : pagedSizedChunkOffsets)
    {
        const auto nextChunk = std::span(resultBufferVec).subspan(nextChunkStart, lastBufferIdxInChunk - nextChunkStart);
        const auto range = SequenceRange(FracturedNumber(nextSeq), FracturedNumber(nextSeq + 1));
        writePagedSizeTupleBufferChunkToFile(nextChunk, range, numTuplesInChunk, appendFile, sizeOfSchemaInBytes, varSizedFieldOffsets);
        nextChunkStart = lastBufferIdxInChunk;
        ++nextSeq;
    }
    appendFile.close();
}

inline void updateChildBufferIdx(
    TupleBuffer& parentBuffer,
    const VariableSizedAccess varSizedAccess,
    const std::vector<size_t>& varSizedFieldOffsets,
    const size_t sizeOfSchemaInBytes)
{
    /// Write index of child buffer that 'parentBuffer' returned to the corresponding place in the parent buffer
    const auto tupleIdx = varSizedAccess.getIndex() / varSizedFieldOffsets.size();
    const auto tupleByteOffset = tupleIdx * sizeOfSchemaInBytes;
    const size_t varSizedOffsetIdx = varSizedAccess.getIndex() % varSizedFieldOffsets.size();
    const auto varSizedOffset = varSizedFieldOffsets.at(varSizedOffsetIdx) + tupleByteOffset;
    const auto combinedIndexOffsetBytes = std::as_bytes(std::span{&varSizedAccess, 1});
    std::ranges::copy(combinedIndexOffsetBytes, parentBuffer.getAvailableMemoryArea().begin() + varSizedOffset);
}

inline std::vector<TupleBuffer> loadTupleBuffersFromFile(
    AbstractBufferProvider& bufferProvider,
    const Schema& schema,
    const std::filesystem::path& filepath,
    const std::vector<size_t>& varSizedFieldOffsets)
{
    const auto sizeOfSchemaInBytes = schema.getSizeOfSchemaInBytes();
    if (std::ifstream file(filepath, std::ifstream::binary); file.is_open())
    {
        const auto fileHeader = [](std::ifstream& file, const std::filesystem::path& filepath)
        {
            BinaryFileHeader tmpFileHeader;
            if (file.read(reinterpret_cast<char*>(&tmpFileHeader), sizeof(BinaryFileHeader)))
            {
                return tmpFileHeader;
            }
            throw FormattingError("Failed to read file header from file: {}", filepath.string());
        }(file, filepath);

        std::vector<TupleBuffer> expectedResultBuffers(fileHeader.numberOfBuffers);
        for (size_t bufferIdx = 0; bufferIdx < fileHeader.numberOfBuffers; ++bufferIdx)
        {
            const auto bufferHeader = [](std::ifstream& file)
            {
                BinaryBufferHeader header;
                file.read(std::bit_cast<char*>(&header), sizeof(BinaryBufferHeader));
                return header;
            }(file);

            auto parentBuffer = bufferProvider.getBufferBlocking();
            const auto numBytesInBuffer = bufferHeader.numberOfTuples * sizeOfSchemaInBytes;
            file.read(parentBuffer.getAvailableMemoryArea<char>().data(), static_cast<std::streamsize>(numBytesInBuffer));

            for (size_t tupleIdx = 0; tupleIdx < bufferHeader.numberOfTuples; ++tupleIdx)
            {
                for (const auto& varSizedOffset : varSizedFieldOffsets)
                {
                    const auto childBufferOffset = (tupleIdx * sizeOfSchemaInBytes) + varSizedOffset;
                    const auto varSizedAccess
                        = reinterpret_cast<VariableSizedAccess*>(parentBuffer.getAvailableMemoryArea().data() + childBufferOffset);
                    if (auto nextChildBuffer = bufferProvider.getUnpooledBuffer(varSizedAccess->getSize().getRawSize()))
                    {
                        file.read(nextChildBuffer.value().getAvailableMemoryArea<char>().data(), varSizedAccess->getSize().getRawSize());

                        const auto newChildBufferIdx = parentBuffer.storeChildBuffer(nextChildBuffer.value());
                        *varSizedAccess = VariableSizedAccess{newChildBufferIdx, VariableSizedAccess::Offset(0), varSizedAccess->getSize()};
                        continue;
                    }
                    throw BufferAllocationFailure("Failed to get unpooled buffer");
                }
            }
            parentBuffer.setNumberOfTuples(bufferHeader.numberOfTuples);
            parentBuffer.setSequenceRange(SequenceRange(
                FracturedNumber({bufferHeader.sequenceRangeStart, bufferHeader.sequenceRangeStartFracture}),
                FracturedNumber({bufferHeader.sequenceRangeEnd, bufferHeader.sequenceRangeEndFracture})));
            expectedResultBuffers.at(bufferIdx) = std::move(parentBuffer);
        }
        file.close();
        return expectedResultBuffers;
    }
    throw InvalidConfigParameter("Invalid filepath: {}", filepath.string());
}
}
