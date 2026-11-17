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
#include <vector>

#include <zstd.h>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::expected<ZstdDecoderConfig, Exception> ZstdDecoderConfig::fromConfig(const InstantiatedConfig&)
{
    return ZstdDecoderConfig{};
}

ZstdDecoder::ZstdDecoder(const ZstdDecoderConfig&) : context(ZSTD_createDCtx())
{
    if (context == nullptr)
    {
        throw CannotDecodeSourceData("Cannot create Zstd decompression context");
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

void ZstdDecoder::process(const std::span<const std::byte> input, const BufferProvider& provideBuffer, std::vector<TupleBuffer>& output)
{
    if (input.empty())
    {
        /// Nothing can be pending: every call drains the decompressor until it stops filling buffers.
        return;
    }

    ZSTD_inBuffer source{.src = input.data(), .size = input.size(), .pos = 0};
    std::optional<TupleBuffer> current;
    ZSTD_outBuffer target{};
    while (true)
    {
        if (!current)
        {
            current = provideBuffer();
            if (!current)
            {
                return; /// The source is stopping.
            }
            const auto memory = current->getAvailableMemoryArea<std::byte>();
            target = ZSTD_outBuffer{.dst = memory.data(), .size = memory.size(), .pos = 0};
        }
        /// Reads until the input is used up or the target is full; returns 0 when a frame is complete and flushed.
        const auto hint = ZSTD_decompressStream(context, &target, &source);
        if (ZSTD_isError(hint))
        {
            throw CannotDecodeSourceData("Corrupt Zstd input: {}", ZSTD_getErrorName(hint));
        }
        insideFrame = hint != 0;

        if (target.pos == target.size)
        {
            /// Full: pass it on. The decompressor may still hold output for this input, so go on.
            current->setNumberOfTuples(target.pos);
            output.push_back(std::move(*current));
            current.reset();
            continue;
        }
        if (source.pos == source.size)
        {
            break; /// Target not full and input used up: everything decodable so far is flushed.
        }
    }
    if (current && target.pos > 0)
    {
        current->setNumberOfTuples(target.pos);
        output.push_back(std::move(*current));
    }
}

void ZstdDecoder::flush(const BufferProvider&, std::vector<TupleBuffer>&)
{
    /// process() leaves nothing behind, so the only thing to check is that the stream did not stop mid-frame.
    if (insideFrame)
    {
        throw CannotDecodeSourceData("Zstd input ended inside a frame (truncated stream)");
    }
}

}
