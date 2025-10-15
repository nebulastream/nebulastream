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
#include <lz4frame.h>

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
    const std::function<std::optional<TupleBuffer>(const TupleBuffer&, const DecodeStatusType)>& emitAndProvide)
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
            auto result = emitAndProvide(currentDecodedBuffer, DecodeStatusType::FINISHED_DECODING_CURRENT_BUFFER);
        }
    }
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
