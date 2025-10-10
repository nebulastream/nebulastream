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

#pragma once

#include <cstddef>
#include <vector>
#include <Decoders/Decoder.hpp>
#include <lz4frame.h>

namespace NES
{
/// Decoder for LZ4frame encoded data.
/// The buffers of encoded data must arrive sequentially in the right order.
class LZ4Decoder final : public Decoder
{
public:
    explicit LZ4Decoder();
    ~LZ4Decoder() override;

    LZ4Decoder(const LZ4Decoder&) = delete;
    LZ4Decoder& operator=(const LZ4Decoder&) = delete;
    LZ4Decoder(LZ4Decoder&&) = delete;
    LZ4Decoder& operator=(LZ4Decoder&&) = delete;

    void decodeAndEmit(
        TupleBuffer& encodedBuffer,
        TupleBuffer& emptyDecodedBuffer,
        const std::function<std::optional<TupleBuffer>(const TupleBuffer&, const DecodeStatusType)>& emitAndProvide) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// Decompression context to keep track of decompression metadata during buffer decoding
    LZ4F_decompressionContext_t decompCtx;
};
}
