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
    const std::function<std::optional<TupleBuffer>(const TupleBuffer&, const DecodeStatusType)>& emitAndProvide)
{
    TupleBuffer currentDecodedBuffer = emptyDecodedBuffer;

    /// Create Zstd input buffer of the encoded buffer.
    ZSTD_inBuffer src{.src = encodedBuffer.getMemArea(), .size = encodedBuffer.getNumberOfTuples(), .pos = 0};

    size_t returnedCode = -1;
    while (returnedCode != 0)
    {
        /// We create a new output buffer for the empty decoded buffer. We use get buffer size for the size since we aim to fill the whole
        /// buffer, if possible.
        ZSTD_outBuffer dst{.dst = currentDecodedBuffer.getMemArea(), .size = currentDecodedBuffer.getBufferSize(), .pos = 0};
        /// Decode as much data as possible
        returnedCode = ZSTD_decompressStream(decompCtx, &dst, &src);
        if (ZSTD_isError(returnedCode))
        {
            NES_ERROR("Failed to decompress zstd-encoded buffer.");
        }
        /// Set number of tuples using the output buffer pos
        currentDecodedBuffer.setNumberOfTuples(dst.pos);

        /// Emit decoded buffer. If the encoded buffer was not fully encoded yet, provide a new, empty buffer.
        if (returnedCode == 0)
        {
            auto result = emitAndProvide(currentDecodedBuffer, DecodeStatusType::FINISHED_DECODING_CURRENT_BUFFER);
        }
        else
        {
            auto emptyBuffer = emitAndProvide(currentDecodedBuffer, DecodeStatusType::DECODING_REQUIRES_ANOTHER_BUFFER);
            currentDecodedBuffer = emptyBuffer.value();
        }
    }
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
