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

#include <ZstdEncoder.hpp>

#include <cstddef>
#include <cstdio>
#include <memory>
#include <ostream>
#include <span>
#include <vector>
#include <zstd.h>
#include <Encoders/Encoder.hpp>
#include <Util/Logger/Logger.hpp>
#include <EncoderRegistry.hpp>

namespace NES
{
ZstdEncoder::ZstdEncoder() = default;

ZstdEncoder::~ZstdEncoder() = default;

Encoder::EncodingResult ZstdEncoder::encodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const
{
    /// Get maximum size of compressed buffer and allocate the amount in the dst vector
    const size_t maxCompressedSize = ZSTD_compressBound(src.size_bytes());
    dst.resize(maxCompressedSize);
    /// Compress src
    const size_t writtenBytes = ZSTD_compress(dst.data(), maxCompressedSize, src.data(), src.size_bytes(), ZSTD_CLEVEL_DEFAULT);
    if (ZSTD_isError(writtenBytes))
    {
        NES_ERROR("Error occurred during ZSTD Encoding operation: {}.", ZSTD_getErrorName(writtenBytes));
        return EncodingResult(EncodeStatusType::ENCODING_ERROR, 0);
    }
    return EncodingResult(EncodeStatusType::SUCCESSFULLY_ENCODED, writtenBytes);
}

std::ostream& ZstdEncoder::toString(std::ostream& str) const
{
    str << "ZstdEncoder";
    return str;
}

EncoderRegistryReturnType EncoderGeneratedRegistrar::RegisterZstdEncoder(EncoderRegistryArguments)
{
    return std::make_unique<ZstdEncoder>();
}
}
