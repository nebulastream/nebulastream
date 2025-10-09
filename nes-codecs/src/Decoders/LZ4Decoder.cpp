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
#include <memory>
#include <utility>

#include <../../../nes-common/include/ErrorHandling.hpp>
#include <../../../nes-common/include/Util/Logger/Logger.hpp>
#include <../../../nes-memory/include/Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>
#include <DecoderRegistry.hpp>
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

Decoder::DecodeReturnType LZ4Decoder::decode(TupleBuffer& encodedBuffer, TupleBuffer& emptyDecodedBuffer)
{
    /// The amount of bytes in the encoded buffer
    const size_t encodedBytes = encodedBuffer.getNumberOfTuples();
    PRECONDITION(positionInCurrentBuffer < encodedBuffer, "PositionInCurrentBuffer is out of bounds.");

    /// Create size_t variables of the buffer capacities, which will be overwritten by LZ4F_decompress
    size_t remainingEncodedBytes = encodedBytes - positionInCurrentBuffer;
    size_t tupleBufferSize = emptyDecodedBuffer.getBufferSize();

    /// Decode data from the encoded buffer starting from positionInCurrentBuffer and write the result into emptyDecodedBuffer
    /// until we either decoded all of the data (return true) or emptyDecodedBuffer was filled up completely (return false).
    const auto errorCode = LZ4F_decompress(
        decompCtx,
        emptyDecodedBuffer.getMemArea(),
        &tupleBufferSize,
        encodedBuffer.getMemArea() + positionInCurrentBuffer,
        &remainingEncodedBytes,
        nullptr);

    if (LZ4F_isError(errorCode))
    {
        NES_ERROR("Failed to decompress lz4-encoded buffer.");
    }

    /// TupleBufferSize now yields the amount of bytes that were written in the emptyDecodedBuffer
    emptyDecodedBuffer.setNumberOfTuples(tupleBufferSize);

    /// RemainingEncodedBytes now yields the amount of bytes that were decoded
    positionInCurrentBuffer += remainingEncodedBytes;

    if (positionInCurrentBuffer >= encodedBytes)
    {
        /// We decoded all bytes in encodedTupleBuffer and signalize this by returning the corresponding DecodeReturnType
        /// Reset positionInCurrentBuffer
        positionInCurrentBuffer = 0;
        return DecodeReturnType::FINISHED_ENCODING_CURRENT_BUFFER;
    }
    /// There are still bytes in the encoded buffer that were not decoded yet.
    return DecodeReturnType::REQUIRES_CURRENT_BUFFER_AGAIN;
}

std::ostream& LZ4Decoder::toString(std::ostream& str) const
{
    /// Simply returns name due to lack of other meaningful debug information.
    str << "LZ4Decoder: Current position in encoded buffer: " << positionInCurrentBuffer;
    return str;
}

DecoderRegistryReturnType DecoderGeneratedRegistrar::RegisterLZ4Decoder(DecoderRegistryArguments)
{
    return std::make_unique<LZ4Decoder>();
}
}
