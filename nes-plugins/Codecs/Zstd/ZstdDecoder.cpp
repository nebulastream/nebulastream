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

#include <cstddef>
#include <expected>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include <zstd.h>
#include <Configurations/ConfigField.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

ZstdDecoder::ZstdDecoder(const ZstdDecoderConfig& /*config*/) : context(ZSTD_createDCtx())
{
    if (context == nullptr)
    {
        throw CannotDecodeSourceData("Cannot create a Zstd decompression context");
    }
}

ZstdDecoder::~ZstdDecoder()
{
    ZSTD_freeDCtx(context);
}

Schema<QualifiedErasedConfigField, Ordered> ZstdDecoder::getConfigSchema()
{
    return Schema<QualifiedErasedConfigField, Ordered>{std::vector<QualifiedErasedConfigField>{}};
}

std::expected<std::vector<TupleBuffer>, Exception> ZstdDecoder::decode(const TupleBuffer& encoded, AbstractBufferProvider& bufferProvider)
{
    const auto input = encoded.getAvailableMemoryArea<std::byte>().first(encoded.getNumberOfTuples());
    std::vector<TupleBuffer> decoded;
    if (input.empty())
    {
        return decoded;
    }

    ZSTD_inBuffer source{.src = input.data(), .size = input.size(), .pos = 0};
    std::optional<TupleBuffer> current;
    ZSTD_outBuffer target{};
    while (true)
    {
        if (not current)
        {
            current = bufferProvider.getBufferBlocking();
            const auto memory = current->getAvailableMemoryArea<std::byte>();
            target = ZSTD_outBuffer{.dst = memory.data(), .size = memory.size(), .pos = 0};
        }
        /// Reads until the input is used up or the target is full; returns 0 when a frame is complete and flushed.
        const auto inputBefore = source.pos;
        const auto outputBefore = target.pos;
        const auto hint = ZSTD_decompressStream(context, &target, &source);
        if (ZSTD_isError(hint) != 0U)
        {
            return std::unexpected{CannotDecodeSourceData("Corrupt Zstd input: {}", ZSTD_getErrorName(hint))};
        }
        if (source.pos > inputBefore || target.pos > outputBefore)
        {
            /// A call without progress reports the header size of a next frame, not that a frame is open.
            insideFrame = hint != 0;
        }

        if (target.pos == target.size)
        {
            /// The decompressor may still hold output for this input, so the loop continues with a new buffer.
            current->setNumberOfTuples(target.pos);
            decoded.push_back(std::move(*current));
            current.reset();
            continue;
        }
        if (source.pos == source.size)
        {
            /// Target not full and input used up: everything decodable so far has been written.
            break;
        }
    }
    if (current && target.pos > 0)
    {
        current->setNumberOfTuples(target.pos);
        decoded.push_back(std::move(*current));
    }
    return decoded;
}

std::expected<std::vector<TupleBuffer>, Exception> ZstdDecoder::flush(AbstractBufferProvider& /*bufferProvider*/)
{
    /// decode returns all output it can produce, so the only check left is whether the stream stopped inside a frame.
    if (insideFrame)
    {
        return std::unexpected{CannotDecodeSourceData("Zstd input ended inside a frame (truncated stream)")};
    }
    return std::vector<TupleBuffer>{};
}

}
