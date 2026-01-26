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

#include <NetworkBindings.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include <Decoders/Decoder.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <network/lib.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>
#include <Thread.hpp>

void initReceiverService(const std::string& connectionAddr, const NES::WorkerId& workerId) /// NOLINT(misc-use-internal-linkage)
{
    init_receiver_service(rust::String(connectionAddr), rust::String(workerId.getRawValue()));
}

void initSenderService(const std::string& connectionAddr, const NES::WorkerId& workerId) /// NOLINT(misc-use-internal-linkage)
{
    init_sender_service(rust::String(connectionAddr), rust::String(workerId.getRawValue()));
}

void TupleBufferBuilder::setMetadata(const SerializedTupleBufferHeader& metaData)
{
    buffer.setSequenceNumber(NES::SequenceNumber(metaData.sequence_number));
    buffer.setChunkNumber(NES::ChunkNumber(metaData.chunk_number));
    buffer.setOriginId(NES::OriginId(metaData.origin_id));
    buffer.setLastChunk(metaData.last_chunk);
    buffer.setWatermark(NES::Timestamp(metaData.watermark));
    buffer.setNumberOfTuples(metaData.number_of_tuples);
}

void TupleBufferBuilder::setData(rust::Slice<const uint8_t> data, bool encoded)
{
    if (encoded)
    {
        /// Decode the buffer
        /// Create a vector and allocate as much size as a tuple buffer can hold
        std::vector<char> decodedData{};
        decodedData.reserve(buffer.getBufferSize());
        /// Create a byte span out of the data slice and decode the data into decodedData
        std::span byteSpan = std::as_bytes(std::span(data));
        NES::Decoder::DecodingResult decodingResult = decoder->decodeBuffer(byteSpan, decodedData);
        INVARIANT(
            decodingResult.status == NES::Decoder::DecodingResultStatus::SUCCESSFULLY_DECODED,
            "Decoding of transmitted tuple buffer caused error");

        std::memcpy(buffer.getAvailableMemoryArea<>().data(), decodedData.data(), decodingResult.decompressedSize);
    }
    else
    {
        INVARIANT(
            buffer.getBufferSize() >= data.length(),
            "Buffer size missmatch. Internal BufferSize: {} vs. External {}",
            buffer.getBufferSize(),
            data.length());
        std::ranges::copy(data, buffer.getAvailableMemoryArea<uint8_t>().begin());
    }
}

void TupleBufferBuilder::addChildBuffer(const rust::Slice<const uint8_t> child, bool encoded, uint64_t bufferSize)
{
    if (encoded)
    {
        /// Decode the child
        /// Create a vector and allocate as much size as the child buffer needs. This size is captured in the bufferSize arg
        std::vector<char> decodedData{};
        decodedData.reserve(bufferSize);
        /// Create a byte span out of the child slice and decode the data into decodedData
        std::span byteSpan = std::as_bytes(std::span(child));
        NES::Decoder::DecodingResult decodingResult = decoder->decodeBuffer(byteSpan, decodedData);
        INVARIANT(
            decodingResult.status == NES::Decoder::DecodingResultStatus::SUCCESSFULLY_DECODED,
            "Decoding of transmitted child buffer caused error");

        auto childBuffer = bufferProvider.getUnpooledBuffer(decodingResult.decompressedSize);

        if (!childBuffer)
        {
            throw NES::CannotAllocateBuffer("allocating child buffer");
        }

        INVARIANT(
            childBuffer->getBufferSize() >= decodingResult.decompressedSize,
            "Unpooled Buffer size missmatch. Internal BufferSize: {} vs. External {}",
            childBuffer->getBufferSize(),
            decodingResult.decompressedSize);
        std::memcpy(childBuffer->getAvailableMemoryArea<>().data(), decodedData.data(), decodingResult.decompressedSize);
        [[maybe_unused]] auto childIndex = buffer.storeChildBuffer(*childBuffer);
    }
    else
    {
        auto childBuffer = bufferProvider.getUnpooledBuffer(child.size());
        if (!childBuffer)
        {
            throw NES::CannotAllocateBuffer("allocating child buffer");
        }

        INVARIANT(
            childBuffer->getBufferSize() >= child.length(),
            "Unpooled Buffer size missmatch. Internal BufferSize: {} vs. External {}",
            childBuffer->getBufferSize(),
            child.length());

        std::ranges::copy(child, childBuffer->getAvailableMemoryArea<uint8_t>().begin());
        [[maybe_unused]] auto childIndex
            = buffer.storeChildBuffer(*childBuffer); /// index should already be present in the owning parent buffer
    }
}

void identifyThread(const rust::str threadName, const rust::str workerId) /// NOLINT(misc-use-internal-linkage)
{
    NES::Thread::ThreadName = static_cast<std::string>(threadName);
    NES::Thread::WorkerNodeId = NES::WorkerId(static_cast<std::string>(workerId));
}
