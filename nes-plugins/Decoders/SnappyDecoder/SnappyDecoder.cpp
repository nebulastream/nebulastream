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
#include <SnappyDecoder.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>
#include <snappy.h>
#include <Runtime/TupleBuffer.hpp>
#include <crc32c/crc32c.h>
#include <DecoderRegistry.hpp>

namespace NES
{

SnappyDecoder::SnappyDecoder() = default;

void SnappyDecoder::decodeAndEmit(
    TupleBuffer& encodedBuffer,
    TupleBuffer& emptyDecodedBuffer,
    const std::function<std::optional<TupleBuffer>(TupleBuffer&, DecodeStatusType)>& emitAndProvide)
{
    /// Add the data of the encoded buffer to the vector.
    encodedBufferStorage.insert(
        encodedBufferStorage.end(),
        encodedBuffer.getAvailableMemoryArea<std::istream::char_type>().data(),
        encodedBuffer.getAvailableMemoryArea<std::istream::char_type>().data() + encodedBuffer.getNumberOfTuples());

    auto currentDecodedBuffer = emptyDecodedBuffer;

    /// As long as enough data to read out the next frame header is staged, we possibly have more data to decode
    while (encodedBufferStorage.size() >= 4)
    {
        /// Type of frame (compressed, uncompressed, stream identifier)
        const int8_t type = encodedBufferStorage[0];
        uint32_t length = 0;
        /// 3 bytes for content length
        std::memcpy(&length, &encodedBufferStorage[1], 3);

        switch (type)
        {
            case 0: { /// Compressed frame
                /// Is the whole frame already stored (4 bytes header + length of payload)?
                if (encodedBufferStorage.size() < 4 + length)
                {
                    /// We need to obtain more buffers before being able to decode
                    return;
                }
                /// Get checksum
                uint32_t checksum = 0;
                std::memcpy(&checksum, &encodedBufferStorage[4], 4);

                /// Pure payload without header & checksum
                const char* encodedData = &encodedBufferStorage[8];
                /// We use a string as decoded data sink, since the decompress function expects this.
                std::string decodedString;
                /// Length of the body, without the checksum
                const uint32_t payloadLength = length - 4;

                if (!snappy::Uncompress(encodedData, payloadLength, &decodedString))
                {
                    NES_ERROR("Decompression of snappy encoded frame failed!")
                    return;
                }
                /// Calculate the checksum of the decompressed content & compare
                const uint32_t unmaskedChecksum = crc32c::Crc32c(decodedString);
                const uint32_t maskedChecksum = ((unmaskedChecksum >> 15) | (unmaskedChecksum << 17)) + 0xa282ead8;
                if (checksum != maskedChecksum)
                {
                    NES_ERROR("Decompressed snappy payload checksum does not match. Possible data corruption");
                    return;
                }
                size_t emittedBytes = 0;
                const size_t amountOfDecodedBytes = decodedString.size();

                /// Emitting loop: While not every decoded byte has been emitted, write as much of the raw decoded data in the current decoded tuple
                /// buffer and emit it. Provide new, empty tuple buffer, if required.
                while (emittedBytes < amountOfDecodedBytes)
                {
                    const auto numBytesForBuffer = std::min(currentDecodedBuffer.getBufferSize(), amountOfDecodedBytes - emittedBytes);
                    std::memcpy(
                        currentDecodedBuffer.getAvailableMemoryArea<std::istream::char_type>().data(),
                        decodedString.data() + emittedBytes,
                        numBytesForBuffer);
                    currentDecodedBuffer.setNumberOfTuples(numBytesForBuffer);
                    emittedBytes += numBytesForBuffer;

                    /// As long as either a buffer is needed for this decompressed batch or the next potential batch in this call, we must allocate a new tuple buffer
                    if (emittedBytes < amountOfDecodedBytes || encodedBufferStorage.size() - (4 + length) >= 4)
                    {
                        auto newBuffer = emitAndProvide(currentDecodedBuffer, DecodeStatusType::DECODING_REQUIRES_ANOTHER_BUFFER);
                        currentDecodedBuffer = newBuffer.value();
                    }
                    else
                    {
                        auto _ = emitAndProvide(currentDecodedBuffer, DecodeStatusType::FINISHED_DECODING_CURRENT_BUFFER);
                    }
                }
                /// Remove the frame from our temporary storage
                encodedBufferStorage.erase(encodedBufferStorage.begin(), encodedBufferStorage.begin() + length + 4);
                break;
            }
            case 1: { /// Decompressed frame. No decoding needed.
                if (encodedBufferStorage.size() < 4 + length)
                {
                    /// We need to obtain more buffers before being able to decode
                    return;
                }

                /// Send the uncompressed frame
                size_t emittedBytes = 0;
                /// The decoded bytes amount is equal to length without the 4 byte checksum
                const size_t amountOfDecodedBytes = length - 4;

                /// Emitting loop: While not every decoded byte has been emitted, write as much of the raw decoded data in the current decoded tuple
                /// buffer and emit it. Provide new, empty tuple buffer, if required.
                while (emittedBytes < amountOfDecodedBytes)
                {
                    const auto numBytesForBuffer = std::min(currentDecodedBuffer.getBufferSize(), amountOfDecodedBytes - emittedBytes);
                    /// We skip the first 8 bytes of the buffer, which include the header and checksum, which we are not interested in
                    std::memcpy(
                        currentDecodedBuffer.getAvailableMemoryArea<std::istream::char_type>().data(),
                        encodedBufferStorage.data() + 8 + emittedBytes,
                        numBytesForBuffer);
                    currentDecodedBuffer.setNumberOfTuples(numBytesForBuffer);
                    emittedBytes += numBytesForBuffer;
                    if (emittedBytes < amountOfDecodedBytes || encodedBufferStorage.size() - (4 + length) >= 4)
                    {
                        auto newBuffer = emitAndProvide(currentDecodedBuffer, DecodeStatusType::DECODING_REQUIRES_ANOTHER_BUFFER);
                        currentDecodedBuffer = newBuffer.value();
                    }
                    else
                    {
                        auto _ = emitAndProvide(currentDecodedBuffer, DecodeStatusType::FINISHED_DECODING_CURRENT_BUFFER);
                    }
                }

                encodedBufferStorage.erase(encodedBufferStorage.begin(), encodedBufferStorage.begin() + length + 4);
                break;
            }
            case -1: { /// Format identifier
                if (encodedBufferStorage.size() < 4 + length)
                {
                    /// We need to obtain more buffers before being able to decode
                    return;
                }
                /// Only has header, not a checksum
                encodedBufferStorage.erase(encodedBufferStorage.begin(), encodedBufferStorage.begin() + length + 4);
                break;
            }
            default: {
                NES_ERROR("Unknown snappy frame type encountered!")
            }
        }
    }
}

Decoder::DecodingResult SnappyDecoder::decodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const
{
    size_t currentSrcPosition = 0;
    size_t currentDstPosition = 0;

    /// Decompress src frame by frame. It is to be expected that src only contains full, valid frames
    while (currentSrcPosition < src.size_bytes())
    {
        /// Check, if enough bytes to contain the header are available
        if (src.size_bytes() - currentSrcPosition < 8)
        {
            NES_ERROR("Error during snappy buffer decoding: Src buffer contains incomplete frame.");
            return DecodingResult(DecodingResultStatus::DECODING_ERROR, 0);
        }

        /// Currently, this is only used to decompress snappy buffers produced by this systems encodeBuffer method for snappy.
        /// This method only produces compressed chunks. Therefore, the chunk identifier should always be 0.
        /// We assume that this is the case to avoid a branch here and skip to the length.
        uint32_t chunkLength = 0;
        std::memcpy(&chunkLength, src.data() + currentSrcPosition + 1, 3);
        /// Get checksum
        uint32_t checksum = 0;
        std::memcpy(&checksum, src.data() + currentSrcPosition + 4, 4);

        /// Check if frame contains the whole payload
        if (src.size_bytes() - (currentSrcPosition + 4) < chunkLength)
        {
            NES_ERROR("Error during snappy buffer decoding: Src buffer contains incomplete frame.");
            return DecodingResult(DecodingResultStatus::DECODING_ERROR, 0);
        }
        std::string decompressedString;
        /// Decompress buffer
        if (!snappy::Uncompress(reinterpret_cast<const char*>(src.data() + currentSrcPosition + 8), chunkLength - 4, &decompressedString))
        {
            NES_ERROR("Decompression of snappy frame failed.");
            return DecodingResult(DecodingResultStatus::DECODING_ERROR, 0);
        }

        /// Calculate checksum
        const uint32_t unmaskedChecksum = crc32c::Crc32c(decompressedString);
        const uint32_t maskedChecksum = ((unmaskedChecksum >> 15) | (unmaskedChecksum << 17)) + 0xa282ead8;
        if (maskedChecksum != checksum)
        {
            NES_ERROR("Checksum of decompressed snappy data does not match frame checksum. Possible corruption of payload.");
            return DecodingResult(DecodingResultStatus::DECODING_ERROR, 0);
        }

        /// Write the decompressed content into dst. Dst is assumed to have allocated enough memory
        std::memcpy(dst.data() + currentDstPosition, decompressedString.c_str(), decompressedString.size());

        /// Update positions
        currentSrcPosition += chunkLength + 4;
        currentDstPosition += decompressedString.size();
    }
    return DecodingResult(DecodingResultStatus::SUCCESSFULLY_DECODED, currentDstPosition);
}

std::ostream& SnappyDecoder::toString(std::ostream& str) const
{
    str << "SnappyDecoder: Currently storing " << encodedBufferStorage.size() << " bytes of encoded frame data.";
    return str;
}

DecoderRegistryReturnType DecoderGeneratedRegistrar::RegisterSnappyDecoder(DecoderRegistryArguments)
{
    return std::make_unique<SnappyDecoder>();
}
}
