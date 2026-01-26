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

#include <ZstdDecoder.hpp>

#include <cstdio>
#include <iostream>
#include <memory>
#include <ostream>
#include <utility>
#include <DecoderRegistry.hpp>

namespace NES
{

ZstdDecoder::ZstdDecoder() : decompCtx(ZSTD_createDStream())
{
    /// Create the decompression context
    if (!decompCtx)
    {
        NES_ERROR("Failed to create ZstdDecompression context.");
    }
}

ZstdDecoder::~ZstdDecoder()
{
    if (decompCtx)
    {
        ZSTD_freeDStream(decompCtx);
        decompCtx = nullptr;
    }
}

void ZstdDecoder::decodeAndEmit(
    TupleBuffer& encodedBuffer,
    TupleBuffer& emptyDecodedBuffer,
    const std::function<std::optional<TupleBuffer>(TupleBuffer&, const DecodeStatusType)>& emitAndProvide)
{
    TupleBuffer currentDecodedBuffer = emptyDecodedBuffer;

    /// Create Zstd input buffer of the encoded buffer.
    ZSTD_inBuffer src{
        .src = encodedBuffer.getAvailableMemoryArea<std::istream::char_type>().data(), .size = encodedBuffer.getNumberOfTuples(), .pos = 0};

    size_t returnedCode = -1;
    while (returnedCode != 0)
    {
        /// We create a new output buffer for the empty decoded buffer. We use get buffer size for the size since we aim to fill the whole
        /// buffer, if possible.
        ZSTD_outBuffer dst{
            .dst = currentDecodedBuffer.getAvailableMemoryArea<std::istream::char_type>().data(),
            .size = currentDecodedBuffer.getBufferSize(),
            .pos = 0};
        /// Decode as much data as possible
        returnedCode = ZSTD_decompressStream(decompCtx, &dst, &src);
        if (ZSTD_isError(returnedCode))
        {
            NES_ERROR("Failed to decompress zstd-encoded buffer.");
            /// Sometimes, we get a decompression error right at the end of the data stream. I have no idea why this is happening, but just
            /// returning without emitting anything resolved the endless loop issue and did not lead to data loss for the large systests.
            return;
        }
        /// Set number of tuples using the output buffer pos
        currentDecodedBuffer.setNumberOfTuples(dst.pos);

        /// Emit decoded buffer. If the encoded buffer was not fully encoded yet, provide a new, empty buffer.
        if (returnedCode == 0)
        {
            auto _ = emitAndProvide(currentDecodedBuffer, DecodeStatusType::FINISHED_DECODING_CURRENT_BUFFER);
        }
        else
        {
            auto emptyBuffer = emitAndProvide(currentDecodedBuffer, DecodeStatusType::DECODING_REQUIRES_ANOTHER_BUFFER);
            currentDecodedBuffer = emptyBuffer.value();
        }
    }
}

Decoder::DecodingResult ZstdDecoder::decodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const
{
    /// Zstd natively provides a method for "stateless" decoding of the whole buffer
    /// We assume that dst allocated enough memory to hold the whole decoded buffer
    const size_t srcSize = src.size_bytes();
    const size_t dstSize = dst.capacity() * sizeof(char);
    const size_t decompressedBytes = ZSTD_decompress(dst.data(), dstSize, src.data(), srcSize);
    if (ZSTD_isError(decompressedBytes))
    {
        return DecodingResult{DecodingResultStatus::DECODING_ERROR, 0};
    }
    return DecodingResult{DecodingResultStatus::SUCCESSFULLY_DECODED, decompressedBytes};
}

std::ostream& ZstdDecoder::toString(std::ostream& str) const
{
    str << "ZstdDecoder";
    return str;
}

DecoderRegistryReturnType DecoderGeneratedRegistrar::RegisterZstdDecoder(DecoderRegistryArguments)
{
    return std::make_unique<ZstdDecoder>();
}
}
