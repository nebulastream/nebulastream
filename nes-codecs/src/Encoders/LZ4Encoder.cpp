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

#include <LZ4Encoder.hpp>

#include <cstdio>
#include <functional>
#include <memory>
#include <ostream>
#include <utility>

#include <Encoders/Encoder.hpp>
#include <EncoderRegistry.hpp>
#include <lz4frame.h>

namespace NES
{

/// Initialise compression preferences with default
LZ4Encoder::LZ4Encoder() : preferences(LZ4F_INIT_PREFERENCES)
{
}

LZ4Encoder::~LZ4Encoder()
{
}

Encoder::EncodingResult LZ4Encoder::encodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const
{
    /// First, we must allocate enough memory in the dst vector. LZ4 provides an upper bound for the compressed size that we can use.
    const size_t worstCaseSize = LZ4F_compressFrameBound(src.size_bytes(), &preferences);
    dst.resize(worstCaseSize);

    /// We call the compression function that compresses all of src and writes the result into dst
    /// It returns the compressed size or an error code.
    const size_t compressedSize = LZ4F_compressFrame(dst.data(), worstCaseSize, src.data(), src.size_bytes(), &preferences);
    if (LZ4F_isError(compressedSize))
    {
        NES_ERROR("Error occurred during LZ4 Encoding operation: {}", LZ4F_getErrorName(compressedSize));
        return EncodingResult(EncodeStatusType::ENCODING_ERROR, 0);
    }
    return EncodingResult(EncodeStatusType::SUCCESSFULLY_ENCODED, compressedSize);
}

std::ostream& LZ4Encoder::toString(std::ostream& str) const
{
    /// Simply returns name due to lack of other meaningful debug information.
    str << "LZ4Encoder";
    return str;
}

EncoderRegistryReturnType EncoderGeneratedRegistrar::RegisterLZ4Encoder(EncoderRegistryArguments)
{
    return std::make_unique<LZ4Encoder>();
}
}
