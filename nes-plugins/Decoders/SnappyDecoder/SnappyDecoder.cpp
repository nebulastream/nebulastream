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
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <snappy.h>
#include <Runtime/TupleBuffer.hpp>
#include <DecoderRegistry.hpp>

namespace NES
{

SnappyDecoder::SnappyDecoder() = default;

void SnappyDecoder::decodeAndEmit(
    TupleBuffer& encodedBuffer,
    TupleBuffer& emptyDecodedBuffer,
    const std::function<std::optional<TupleBuffer>(TupleBuffer&, const DecodeStatusType)>& emitAndProvide)
{
    /// Add the data of the encoded buffer to the vector.
    encodedBufferStorage.insert(
        encodedBufferStorage.end(),
        encodedBuffer.getAvailableMemoryArea<std::istream::char_type>().data(),
        encodedBuffer.getAvailableMemoryArea<std::istream::char_type>().data() + encodedBuffer.getNumberOfTuples());

    /// Check if we can already extract the header of the next frame (4 bytes)
    if (encodedBufferStorage.size() < 4)
    {
        /// Did not obtain enough data to extract length of frame
        return;
    }
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
            /// Pure payload without header. We need to skip the 4 byte header and the 4 byte checksum at the start of the payload
            const char* encodedData = &encodedBufferStorage[8];
            /// We use a string as decoded data sink, since the decompress function expects this.
            std::string decodedString;
            /// Length of the body, without the checksum
            uint32_t payloadLength = length - 4;

            if (!snappy::Uncompress(encodedData, payloadLength, &decodedString))
            {
                NES_ERROR("Decompression of snappy encoded frame failed!")
                return;
            }
            size_t emittedBytes = 0;
            const size_t amountOfDecodedBytes = decodedString.size();

            auto currentDecodedBuffer = emptyDecodedBuffer;

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

                if (emittedBytes < amountOfDecodedBytes)
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
            auto currentDecodedBuffer = emptyDecodedBuffer;

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
                if (emittedBytes < amountOfDecodedBytes)
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
