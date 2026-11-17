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

#include <cstddef>
#include <expected>
#include <optional>
#include <span>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <ErrorHandling.hpp>
#include <lz4frame.h>

namespace NES
{

std::expected<LZ4DecoderConfig, Exception> LZ4DecoderConfig::fromConfig(const InstantiatedConfig&)
{
    return LZ4DecoderConfig{};
}

LZ4Decoder::LZ4Decoder(const LZ4DecoderConfig&)
{
    if (const auto result = LZ4F_createDecompressionContext(&context, LZ4F_VERSION); LZ4F_isError(result))
    {
        throw CannotDecodeSourceData("Cannot create LZ4 decompression context: {}", LZ4F_getErrorName(result));
    }
}

LZ4Decoder::~LZ4Decoder()
{
    LZ4F_freeDecompressionContext(context);
}

Schema<QualifiedErasedConfigField, Ordered> LZ4Decoder::getConfigSchema()
{
    return Schema<QualifiedErasedConfigField, Ordered>{std::vector<QualifiedErasedConfigField>{}};
}

void LZ4Decoder::process(const std::span<const std::byte> input, const BufferProvider& provideBuffer, std::vector<TupleBuffer>& output)
{
    if (input.empty())
    {
        /// Nothing can be pending: every call drains the decompressor until it stops filling buffers.
        return;
    }

    std::optional<TupleBuffer> current;
    size_t filled = 0;
    size_t consumed = 0;
    while (true)
    {
        if (!current)
        {
            current = provideBuffer();
            if (!current)
            {
                return; /// The source is stopping.
            }
            filled = 0;
        }
        const auto target = current->getAvailableMemoryArea<std::byte>();
        size_t written = target.size() - filled;
        size_t read = input.size() - consumed;
        /// Reads until the input is used up, the target is full, or a frame ends (it then returns 0).
        const auto hint = LZ4F_decompress(context, target.data() + filled, &written, input.data() + consumed, &read, nullptr);
        if (LZ4F_isError(hint))
        {
            throw CannotDecodeSourceData("Corrupt LZ4 input: {}", LZ4F_getErrorName(hint));
        }
        INVARIANT(written > 0 || read > 0 || consumed == input.size(), "LZ4F_decompress made no progress");
        consumed += read;
        filled += written;
        if (read > 0 || written > 0)
        {
            /// A call without progress (no input left, e.g. after a frame that ended exactly at a full buffer) reports
            /// the header size of a next frame; it says nothing about the stream being inside a frame.
            insideFrame = hint != 0;
        }

        if (filled == target.size())
        {
            /// Full: pass it on. The decompressor may still hold output for this input, so go on.
            current->setNumberOfTuples(filled);
            output.push_back(std::move(*current));
            current.reset();
            continue;
        }
        if (consumed == input.size())
        {
            break; /// Target not full and input used up: nothing left to decode for now.
        }
    }
    if (current && filled > 0)
    {
        current->setNumberOfTuples(filled);
        output.push_back(std::move(*current));
    }
}

void LZ4Decoder::flush(const BufferProvider&, std::vector<TupleBuffer>&)
{
    /// process() leaves nothing behind, so the only thing to check is that the stream did not stop mid-frame.
    if (insideFrame)
    {
        throw CannotDecodeSourceData("LZ4 input ended inside a frame (truncated stream)");
    }
}

}
