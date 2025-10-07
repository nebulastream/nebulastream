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

Decoder::DecodeReturnType ZstdDecoder::decode(TupleBuffer& encodedBuffer, TupleBuffer& emptyDecodedBuffer)
{
    /// Create Zstd input buffer of the encoded buffer. We take into consideration, how many bytes already have been decoded.
    /// Its a bit hacky to create a new input buffer for every call. It would be better, if we would provide a "submit encoded buffer"
    /// method for decoders, which can set up all the necessary prerequisits once per encoded buffer.
    ZSTD_inBuffer src{.src = encodedBuffer.getMemArea(), .size = encodedBuffer.getNumberOfTuples(), .pos = positionInCurrentBuffer};

    /// We create a new output buffer for the empty decoded buffer. We use get buffer size for the size since we aim to fill the whole
    /// buffer, if possible. Pos is always 0, because the method should be called with a fresh decoded buffer each time.
    ZSTD_outBuffer dst{.dst = emptyDecodedBuffer.getMemArea(), .size = emptyDecodedBuffer.getBufferSize(), .pos = 0};

    /// Decode as much data as possible
    const size_t returnedCode = ZSTD_decompressStream(decompCtx, &dst, &src);
    if (ZSTD_isError(returnedCode))
    {
        NES_ERROR("Failed to decompress zstd-encoded buffer.");
    }

    /// Set number of tuples using the output buffer pos
    emptyDecodedBuffer.setNumberOfTuples(dst.pos);

    /// If the returned code is 0, the whole buffer was decoded. We reset our position member. Otherwise, we adapt the updated pos value
    /// of the input buffer.
    if (returnedCode == 0)
    {
        positionInCurrentBuffer = 0;
        return DecodeReturnType::FINISHED_ENCODING_CURRENT_BUFFER;
    }
    positionInCurrentBuffer = src.pos;
    return DecodeReturnType::REQUIRES_CURRENT_BUFFER_AGAIN;
}

std::ostream& ZstdDecoder::toString(std::ostream& str) const
{
    str << "ZstdDecoder: Current position in encoded buffer: " << positionInCurrentBuffer;
    return str;
}

DecoderRegistryReturnType DecoderGeneratedRegistrar::RegisterZstdDecoder(DecoderRegistryArguments)
{
    return std::make_unique<ZstdDecoder>();
}
}
