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
#include <utility>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <ErrorHandling.hpp>
#include <lz4frame.h>

namespace NES
{

LZ4Decoder::LZ4Decoder(const LZ4DecoderConfig& /*config*/)
{
    if (LZ4F_isError(LZ4F_createDecompressionContext(&context, LZ4F_VERSION)) != 0U)
    {
        throw CannotDecodeSourceData("Cannot create an LZ4 decompression context");
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

std::expected<std::vector<TupleBuffer>, Exception> LZ4Decoder::decode(const TupleBuffer& encoded, AbstractBufferProvider& bufferProvider)
{
    auto input = encoded.getAvailableMemoryArea<std::byte>().first(encoded.getNumberOfTuples());
    std::vector<TupleBuffer> decoded;
    std::optional<TupleBuffer> current;
    std::span<std::byte> target;
    size_t written = 0;
    while (not input.empty())
    {
        if (not current)
        {
            current = bufferProvider.getBufferBlocking();
            target = current->getAvailableMemoryArea<std::byte>();
            written = 0;
        }
        size_t consumed = input.size();
        size_t produced = target.size() - written;
        /// Returns 0 when a frame is complete, otherwise a hint for the size of the next input.
        const auto hint = LZ4F_decompress(context, target.data() + written, &produced, input.data(), &consumed, nullptr);
        if (LZ4F_isError(hint) != 0U)
        {
            return std::unexpected{CannotDecodeSourceData("Corrupt LZ4 input: {}", LZ4F_getErrorName(hint))};
        }
        input = input.subspan(consumed);
        written += produced;
        if (consumed > 0 || produced > 0)
        {
            insideFrame = hint != 0;
        }
        if (written == target.size())
        {
            current->setNumberOfTuples(written);
            decoded.push_back(std::move(*current));
            current.reset();
        }
    }
    if (current && written > 0)
    {
        current->setNumberOfTuples(written);
        decoded.push_back(std::move(*current));
    }
    return decoded;
}

std::expected<std::vector<TupleBuffer>, Exception> LZ4Decoder::flush(AbstractBufferProvider& /*bufferProvider*/)
{
    if (insideFrame)
    {
        return std::unexpected{CannotDecodeSourceData("LZ4 input ended inside a frame (truncated stream)")};
    }
    return std::vector<TupleBuffer>{};
}

}
