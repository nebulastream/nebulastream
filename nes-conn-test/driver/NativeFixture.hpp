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

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <optional>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <ErrorHandling.hpp>

namespace NES::ConnTest
{

/// On-disk headers of a `.nes` native-TupleBuffer container. Copied verbatim from
/// the writer/loader in `nes-input-formatters/tests/Util/FileUtil.hpp` (the source
/// of the committed `.nes` fixtures). We copy rather than link because that header
/// lives in a GTest-only test-utils target and its *writer* half pulls in Nautilus;
/// the loader below needs only host-only Schema / TupleBuffer / buffer-provider types.
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

/// Byte offset, within a tuple, of every VARSIZED field. The engine's row layout is
/// a no-padding prefix-sum of `getSizeInBytesWithNull()`, so this is the same stride
/// walk the loader uses to find each `VariableSizedAccess` slot. Nullable-aware via
/// `getSizeInBytesWithNull()`, so a future nullable fixture needs no change here.
inline std::vector<std::size_t> getVarSizedFieldOffsets(const Schema& schema)
{
    std::size_t priorFieldOffset = 0;
    std::vector<std::size_t> varSizedFieldOffsets;
    for (const auto& field : schema)
    {
        if (field.dataType.isType(DataType::Type::VARSIZED))
        {
            varSizedFieldOffsets.emplace_back(priorFieldOffset);
        }
        priorFieldOffset += field.dataType.getSizeInBytesWithNull();
    }
    return varSizedFieldOffsets;
}

/// Load native row-layout TupleBuffers from a `.nes` container under `schema`.
///
/// Host-only port of `loadTupleBuffersFromFile` (FileUtil.hpp), with two deliberate
/// changes for the harness:
///   - parent buffers come from `getUnpooledBuffer` (sized to the baked buffer)
///     rather than `getBufferBlocking`, so the load does not depend on the pool's
///     buffer size: the `concurrent` scenario tunes the pool to 128-byte buffers,
///     far below a baked `.nes` buffer, which `getBufferBlocking` would overflow.
///   - a VARSIZED slot whose size is 0 (a NULL / empty varsized field) is left
///     untouched instead of requesting a 0-byte child buffer, so a future nullable
///     fixture loads unchanged.
inline std::vector<TupleBuffer> loadNativeTupleBuffers(
    AbstractBufferProvider& bufferProvider,
    const Schema& schema,
    const std::filesystem::path& filepath,
    const std::vector<std::size_t>& varSizedFieldOffsets)
{
    const auto sizeOfSchemaInBytes = schema.getSizeOfSchemaInBytes();
    std::ifstream file(filepath, std::ifstream::binary);
    if (!file.is_open())
    {
        throw InvalidConfigParameter("Invalid filepath: {}", filepath.string());
    }

    BinaryFileHeader fileHeader;
    if (!file.read(std::bit_cast<char*>(&fileHeader), sizeof(BinaryFileHeader)))
    {
        throw FormattingError("Failed to read file header from file: {}", filepath.string());
    }

    std::vector<TupleBuffer> resultBuffers(fileHeader.numberOfBuffers);
    for (std::size_t bufferIdx = 0; bufferIdx < fileHeader.numberOfBuffers; ++bufferIdx)
    {
        BinaryBufferHeader bufferHeader;
        if (!file.read(std::bit_cast<char*>(&bufferHeader), sizeof(BinaryBufferHeader)))
        {
            throw FormattingError("Failed to read buffer header from file: {}", filepath.string());
        }

        const auto numBytesInBuffer = bufferHeader.numberOfTuples * sizeOfSchemaInBytes;
        auto parentBufferOpt = bufferProvider.getUnpooledBuffer(std::max<std::size_t>(numBytesInBuffer, 1));
        if (!parentBufferOpt)
        {
            throw BufferAllocationFailure("Failed to get unpooled buffer for {} bytes", numBytesInBuffer);
        }
        auto parentBuffer = std::move(parentBufferOpt.value());
        file.read(parentBuffer.getAvailableMemoryArea<char>().data(), static_cast<std::streamsize>(numBytesInBuffer));

        for (std::size_t tupleIdx = 0; tupleIdx < bufferHeader.numberOfTuples; ++tupleIdx)
        {
            for (const auto& varSizedOffset : varSizedFieldOffsets)
            {
                const auto childBufferOffset = (tupleIdx * sizeOfSchemaInBytes) + varSizedOffset;
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic): the slot at this offset IS a VariableSizedAccess in the engine's row layout; reading it back is the loader's whole job.
                auto* const varSizedAccess
                    = reinterpret_cast<VariableSizedAccess*>(parentBuffer.getAvailableMemoryArea().data() + childBufferOffset);
                const auto varSizedSize = varSizedAccess->getSize().getRawSize();
                if (varSizedSize == 0)
                {
                    continue; /// NULL / empty varsized field: no child payload to rebuild.
                }
                auto childBufferOpt = bufferProvider.getUnpooledBuffer(varSizedSize);
                if (!childBufferOpt)
                {
                    throw BufferAllocationFailure("Failed to get unpooled child buffer");
                }
                file.read(childBufferOpt.value().getAvailableMemoryArea<char>().data(), static_cast<std::streamsize>(varSizedSize));
                const auto newChildBufferIdx = parentBuffer.storeChildBuffer(childBufferOpt.value());
                *varSizedAccess = VariableSizedAccess{newChildBufferIdx, VariableSizedAccess::Offset(0), varSizedAccess->getSize()};
            }
        }
        parentBuffer.setNumberOfTuples(bufferHeader.numberOfTuples);
        parentBuffer.setSequenceNumber(SequenceNumber(bufferHeader.sequenceNumber));
        parentBuffer.setChunkNumber(ChunkNumber(bufferHeader.chunkNumber));
        resultBuffers.at(bufferIdx) = std::move(parentBuffer);
    }
    return resultBuffers;
}

}
