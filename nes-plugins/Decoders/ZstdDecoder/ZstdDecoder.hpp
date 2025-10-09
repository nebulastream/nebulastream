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
#include <zstd.h>
#include <Decoders/Decoder.hpp>

namespace NES
{
/// Decoder for Zstd encoded data.
/// The buffers of encoded data must arrive sequentially in the right order.
class ZstdDecoder final : public Decoder
{
public:
    explicit ZstdDecoder();
    ~ZstdDecoder() override;

    ZstdDecoder(const ZstdDecoder&) = delete;
    ZstdDecoder& operator=(const ZstdDecoder&) = delete;
    ZstdDecoder(ZstdDecoder&&) = delete;
    ZstdDecoder& operator=(ZstdDecoder&&) = delete;

    DecodeReturnType decode(TupleBuffer& encodedBuffer, TupleBuffer& emptyDecodedBuffer) override;
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// Decompression context to keep track of decompression metadata during buffer decoding.
    /// We use the stream decompression functions because we do not know the size of the original data in the encoded
    /// buffers, which is required to use the frame decompression methods
    ZSTD_DStream* decompCtx = nullptr;
    /// Position of next byte that has to be decoded in the expected encoded buffer
    size_t positionInCurrentBuffer = 0;
};

};
