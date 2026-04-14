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

#include <LZ4Decoder.hpp>

#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <utility>

#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <DecoderRegistry.hpp>
#include <ErrorHandling.hpp>
#include <lz4.h>
#include <lz4frame.h>
#include <Decoders/Decoder.hpp>

namespace NES
{

LZ4Decoder::LZ4Decoder()
{
    /// Create the decompression context that will be used for decoding. Throws error if creation fails.
    if (const auto errorCode = LZ4F_createDecompressionContext(&decompCtx, LZ4F_VERSION); LZ4F_isError(errorCode))
    {
        NES_ERROR("Failed to create LZ4Decompression context.");
    }
}

LZ4Decoder::~LZ4Decoder()
{
    if (decompCtx)
    {
        LZ4F_freeDecompressionContext(decompCtx);
        decompCtx = nullptr;
    }
}

void LZ4Decoder::decodeAndEmit(
    TupleBuffer& encodedBuffer,
    TupleBuffer& emptyDecodedBuffer,
    const std::function<std::optional<TupleBuffer>(TupleBuffer&, const DecodeStatusType)>& emitAndProvide)
{
    TupleBuffer currentDecodedBuffer = emptyDecodedBuffer;

    /// Create variables to hold the encoded bytes in the tuple buffer and the already decoded bytes
    const size_t encodedBytes = encodedBuffer.getNumberOfTuples();
    size_t amountOfDecodedBytes = 0;

    while (amountOfDecodedBytes < encodedBytes)
    {
        /// Create size_t variables of the buffer capacities, which will be overwritten by LZ4F_decompress
        size_t remainingEncodedBytes = encodedBytes - amountOfDecodedBytes;
        size_t tupleBufferSize = currentDecodedBuffer.getBufferSize();

        /// Decode data from the encoded buffer starting from amountOfDecodedBytes and write the result into currentDecodedBuffer
        /// until we either decoded all of the data or the decodedBuffer is completely filled.
        const auto errorCode = LZ4F_decompress(
            decompCtx,
            currentDecodedBuffer.getAvailableMemoryArea<std::istream::char_type>().data(),
            &tupleBufferSize,
            encodedBuffer.getAvailableMemoryArea<std::istream::char_type>().data() + amountOfDecodedBytes,
            &remainingEncodedBytes,
            nullptr);

        if (LZ4F_isError(errorCode))
        {
            NES_ERROR("Failed to decompress lz4-encoded buffer.");
            return;
        }

        /// TupleBufferSize now yields the amount of bytes that were written in the currentDecodedBuffer
        currentDecodedBuffer.setNumberOfTuples(tupleBufferSize);

        /// RemainingEncodedBytes now yields the amount of bytes that were decoded
        amountOfDecodedBytes += remainingEncodedBytes;

        if (amountOfDecodedBytes < encodedBytes)
        {
            /// We still have encoded bytes left. We emit the filled decodedBuffer and obtain a new, empty buffer
            auto emptyBuffer = emitAndProvide(currentDecodedBuffer, DecodeStatusType::DECODING_REQUIRES_ANOTHER_BUFFER);
            PRECONDITION(emptyBuffer.has_value(), "emitAndProvide did not provide a new empty tuple buffer!");
            currentDecodedBuffer = emptyBuffer.value();
        }
        else
        {
            auto _ = emitAndProvide(currentDecodedBuffer, DecodeStatusType::FINISHED_DECODING_CURRENT_BUFFER);
        }
    }
}

Decoder::DecodingResult LZ4Decoder::decodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const
{
    /// The src span holds the size of the compressed block
    const size_t encodedBytes = src.size_bytes();
    /// Another assumtion in this function is, that dst allocated enough memory to hold the whole decoded contents
    /// This is because this function is only called for encoded tuple buffers, which have a maximum size in their unencoded form
    const size_t dstCapacity = dst.capacity() * sizeof(char);
    const int32_t decompressedBytes = LZ4_decompress_safe(reinterpret_cast<const char*>(src.data()), dst.data(), encodedBytes, dstCapacity);
    if (decompressedBytes < 0)
    {
        /// We also return an error if the decoding operation returned an error
        NES_ERROR("Error during LZ4 decoding of whole buffer");
        return DecodingResult{.status=DecodingResultStatus::DECODING_ERROR, .decompressedSize=0};
    }
    /// decompressedBytes holds the amount of bytes that were written by the decompression operation
    return DecodingResult{.status=DecodingResultStatus::SUCCESSFULLY_DECODED, .decompressedSize=static_cast<size_t>(decompressedBytes)};
}

std::ostream& LZ4Decoder::toString(std::ostream& str) const
{
    /// Simply returns name due to lack of other meaningful debug information.
    str << "LZ4Decoder";
    return str;
}

DecoderRegistryReturnType DecoderGeneratedRegistrar::RegisterLZ4Decoder(DecoderRegistryArguments)
{
    return std::make_unique<LZ4Decoder>();
}
}
