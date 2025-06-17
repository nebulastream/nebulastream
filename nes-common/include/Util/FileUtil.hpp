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
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::Util
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

static std::ofstream createFile(const std::filesystem::path& filepath, const FileOpenMode fileOpenMode)
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

static void writeFileHeaderToFile(const BinaryFileHeader& binaryFileHeader, const std::filesystem::path& filepath)
{
    /// create new file and write file header
    auto file = createFile(filepath, FileOpenMode::TRUNCATE);
    file.write(std::bit_cast<const char*>(&binaryFileHeader), sizeof(BinaryFileHeader));
    file.close();
}

static void writePagedSizeBufferChunkToFile(
    const BinaryBufferHeader& binaryHeader,
    const size_t sizeOfSchemaInBytes,
    const std::span<Memory::TupleBuffer> pagedSizeBufferChunk,
    std::ofstream& file)
{
    file.write(std::bit_cast<const char*>(&binaryHeader), sizeof(BinaryBufferHeader));

    for (const auto& buffer : pagedSizeBufferChunk)
    {
        const auto sizeOfBufferInBytes = buffer.getNumberOfTuples() * sizeOfSchemaInBytes;
        file.write(buffer.getBuffer<const char>(), static_cast<std::streamsize>(sizeOfBufferInBytes));
    }
}

static void writePagedSizeTupleBufferChunkToFile(
    std::span<Memory::TupleBuffer> pagedSizeBufferChunk,
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
                    const auto childBufferIdx = *reinterpret_cast<uint32_t*>(buffer.getBuffer() + currentTupleVarSizedFieldOffset);
                    const auto childBufferSize = buffer.loadChildBuffer(childBufferIdx).getBuffer<uint32_t>()[0] + sizeof(uint32_t);

                    const auto childBufferSpan = std::span(buffer.loadChildBuffer(childBufferIdx).getBuffer<const char>(), childBufferSize);
                    appendFile.write(childBufferSpan.data(), static_cast<std::streamsize>(childBufferSpan.size_bytes()));
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

inline void sortTupleBuffers(std::vector<Memory::TupleBuffer>& buffers)
{
    std::ranges::sort(
        buffers.begin(),
        buffers.end(),
        [](const Memory::TupleBuffer& left, const Memory::TupleBuffer& right)
        {
            if (left.getSequenceNumber() == right.getSequenceNumber())
            {
                return left.getChunkNumber() < right.getChunkNumber();
            }
            return left.getSequenceNumber() < right.getSequenceNumber();
        });
}

inline void writeTupleBuffersToFile(
    std::vector<Memory::TupleBuffer>& resultBufferVec,
    const Schema& schema,
    const std::filesystem::path& actualResultFilePath,
    const std::vector<size_t>& varSizedFieldOffsets)
{
    sortTupleBuffers(resultBufferVec);
    const auto sizeOfSchemaInBytes = schema.getSizeOfSchemaInBytes();

    const std::vector<TupleBufferChunk> pagedSizedChunkOffsets
        = [](const std::vector<Memory::TupleBuffer>& resultBufferVec, const size_t sizeOfSchemaInBytes)
    {
        size_t numBytesInNextChunk = 0;
        size_t numTuplesInNextChunk = 0;
        std::vector<TupleBufferChunk> offsets;
        for (const auto& [bufferIdx, buffer] : resultBufferVec | std::views::enumerate)
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

static void updateChildBufferIdx(
    Memory::TupleBuffer& parentBuffer,
    const size_t newChildBufferIdx,
    const std::vector<size_t>& varSizedFieldOffsets,
    const size_t sizeOfSchemaInBytes)
{
    /// Write index of child buffer that 'parentBuffer' returned to the corresponding place in the parent buffer
    const auto tupleIdx = newChildBufferIdx / varSizedFieldOffsets.size();
    const auto tupleByteOffset = tupleIdx * sizeOfSchemaInBytes;
    const size_t varSizedOffsetIdx = newChildBufferIdx % varSizedFieldOffsets.size();
    const auto varSizedOffset = varSizedFieldOffsets.at(varSizedOffsetIdx) + tupleByteOffset;
    *reinterpret_cast<uint32_t*>(parentBuffer.getBuffer() + varSizedOffset) = newChildBufferIdx;
}

static std::vector<Memory::TupleBuffer> loadTupleBuffersFromFile(
    Memory::AbstractBufferProvider& bufferProvider,
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
            throw std::runtime_error("Failed to read file header from file: " + filepath.string());
        }(file, filepath);

        std::vector<Memory::TupleBuffer> expectedResultBuffers(fileHeader.numberOfBuffers);
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
            file.read(parentBuffer.getBuffer<char>(), static_cast<std::streamsize>(numBytesInBuffer));


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
                    nextChildBuffer.value().getBuffer<uint32_t>()[0] = childBufferSize;
                    file.read(nextChildBuffer.value().getBuffer<char>() + sizeof(uint32_t), childBufferSize);

                    const auto newChildBufferIdx = parentBuffer.storeChildBuffer(nextChildBuffer.value());
                    updateChildBufferIdx(parentBuffer, newChildBufferIdx, varSizedFieldOffsets, sizeOfSchemaInBytes);
                    ;
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
