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
#include <Util/Logger/Logger.hpp>
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

std::vector<TupleBuffer> LZ4Decoder::decode(TupleBuffer& encodedBuffer, AbstractBufferProvider& bufferProvider)
{
    /// Vector to encapsule all buffers with decoded data
    std::vector<TupleBuffer> decodedBuffers;
    /// The maximum amount of bytes that we can decode at once corresponds to the size of a tuple buffer
    const ssize_t tupleBufferSize = bufferProvider.getBufferSize();
    /// The amount of bytes in the encoded buffer
    const ssize_t encodedBytes = encodedBuffer.getNumberOfTuples();

    /// Keeps track on how many bytes we already decoded
    ssize_t decodedBytes = 0;

    /// The decoding loop: Generate a new tuple buffers and fill with decoded data until encoded buffer was fully decoded
    while (decodedBytes < encodedBytes)
    {
        /// Create size_t variables of the buffer sizes which will be overwritten by LZ4F_decompress
        size_t bufferCapacity = tupleBufferSize;
        size_t remainingEncodedBytes = encodedBytes - decodedBytes;
        TupleBuffer decodedBuffer = bufferProvider.getBufferBlocking();

        const auto errorCode = LZ4F_decompress(
            decompCtx,
            decodedBuffer.getMemArea(),
            &bufferCapacity,
            encodedBuffer.getMemArea() + decodedBytes,
            &remainingEncodedBytes,
            nullptr);

        if (LZ4F_isError(errorCode))
        {
            NES_ERROR("Failed to decompress lz4-encoded buffer.");
        }

        /// Update number of decoded bytes and set number of tuples for the decodedBuffer
        /// RemainingEncodedBytes now yields the amount of bytes that were decoded
        decodedBytes += remainingEncodedBytes;
        /// BufferCapacity now yields the amount of bytes that were written in the decodedBuffer
        decodedBuffer.setNumberOfTuples(bufferCapacity);

        decodedBuffers.emplace_back(std::move(decodedBuffer));
    }
    return decodedBuffers;
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
