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
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Util/Ranges.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct BinaryFileHeader
{
    uint64_t numberOfBuffers = 0;
    uint64_t sizeOfBuffersInBytes = 4096;
};

struct BinaryBufferHeader
{
    uint64_t numberOfTuples = 0;
    uint64_t numberChildBuffers = 0;
    uint64_t sequenceNumber = 0;
    uint64_t chunkNumber = 0;
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

inline void writePagedSizeTupleBufferChunkToFile(
    std::span<TupleBuffer> pagedSizeBufferChunk,
    const SequenceNumber::Underlying sequenceNumber,
    const size_t numTuplesInChunk,
    std::ofstream& appendFile,
    const size_t sizeOfSchemaInBytes,
    const std::vector<size_t>& varSizedFieldOffsets)
{
    const auto numChildBuffers = numTuplesInChunk * varSizedFieldOffsets.size();
    writePagedSizeBufferChunkToFile(
        {.numberOfTuples = numTuplesInChunk,
         .numberChildBuffers = numChildBuffers,
         .sequenceNumber = sequenceNumber,
         .chunkNumber = ChunkNumber::INITIAL},
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
                        *reinterpret_cast<uint64_t*>(buffer.getAvailableMemoryArea().data() + currentTupleVarSizedFieldOffset)};
                    const auto variableSizedData = MemoryLayout::readVarSizedDataAsString(buffer, varSizedAccess);
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
        [](const TupleBuffer& left, const TupleBuffer& right)
        {
            return SequenceData{left.getSequenceNumber(), left.getChunkNumber(), left.isLastChunk()}
            < SequenceData{right.getSequenceNumber(), right.getChunkNumber(), right.isLastChunk()};
        });
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
    size_t nextChunkSequenceNumber = SequenceNumber::INITIAL;
    for (const auto& [lastBufferIdxInChunk, numTuplesInChunk] : pagedSizedChunkOffsets)
    {
        const auto nextChunk = std::span(resultBufferVec).subspan(nextChunkStart, lastBufferIdxInChunk - nextChunkStart);
        writePagedSizeTupleBufferChunkToFile(
            nextChunk, nextChunkSequenceNumber, numTuplesInChunk, appendFile, sizeOfSchemaInBytes, varSizedFieldOffsets);
        nextChunkStart = lastBufferIdxInChunk;
        ++nextChunkSequenceNumber;
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
    const auto combinedIndexOffset = varSizedAccess.getCombinedIdxOffset();
    const auto combinedIndexOffsetBytes = std::as_bytes(std::span{&combinedIndexOffset, 1});
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


            for (size_t childBufferIdx = 0; childBufferIdx < bufferHeader.numberChildBuffers; ++childBufferIdx)
            {
                const auto childBufferSize = [](std::ifstream& file)
                {
                    uint32_t childSize = 0;
                    file.read(std::bit_cast<char*>(&childSize), sizeof(uint32_t));
                    return childSize;
                }(file);

                if (auto nextChildBuffer = bufferProvider.getUnpooledBuffer(childBufferSize + sizeof(uint32_t)))
                {
                    nextChildBuffer.value().getAvailableMemoryArea<uint32_t>()[0] = childBufferSize;
                    file.read(nextChildBuffer.value().getAvailableMemoryArea<char>().data() + sizeof(uint32_t), childBufferSize);

                    const auto newChildBufferIdx = parentBuffer.storeChildBuffer(nextChildBuffer.value());
                    updateChildBufferIdx(parentBuffer, VariableSizedAccess{newChildBufferIdx}, varSizedFieldOffsets, sizeOfSchemaInBytes);
                    continue;
                }
                throw BufferAllocationFailure("Failed to get unpooled buffer");
            }
            parentBuffer.setNumberOfTuples(bufferHeader.numberOfTuples);
            parentBuffer.setSequenceNumber(SequenceNumber(bufferHeader.sequenceNumber));
            parentBuffer.setChunkNumber(ChunkNumber(bufferHeader.chunkNumber));
            expectedResultBuffers.at(bufferIdx) = std::move(parentBuffer);
        }
        file.close();
        return expectedResultBuffers;
    }
    throw InvalidConfigParameter("Invalid filepath: {}", filepath.string());
}
}
