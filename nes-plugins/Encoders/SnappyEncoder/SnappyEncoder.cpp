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

#include <SnappyEncoder.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <span>
#include <string>
#include <vector>
#include <snappy.h>
#include <Encoders/Encoder.hpp>
#include <crc32c/crc32c.h>
#include <EncoderRegistry.hpp>

namespace NES
{
SnappyEncoder::SnappyEncoder() = default;
SnappyEncoder::~SnappyEncoder() = default;

Encoder::EncodingResult SnappyEncoder::encodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const
{
    size_t currentSrcPosition = 0;
    size_t currentDstPosition = 0;
    /// Pack the src buffer into frames until it is completely compressed
    while (currentSrcPosition < src.size_bytes())
    {
        const size_t bytesToCompress = std::min(static_cast<uint64_t>(65536), src.size_bytes() - currentSrcPosition);
        /// Create subspan of data for frame
        const std::span subspan = src.subspan(currentSrcPosition, bytesToCompress);
        /// Compress subspan into the string
        std::string compressedData;
        const size_t compressedLength = snappy::Compress(reinterpret_cast<const char*>(subspan.data()), bytesToCompress, &compressedData);
        /// Calculate the checksum
        const uint32_t unmaskedChecksum = crc32c::Crc32c(reinterpret_cast<const char*>(subspan.data()), bytesToCompress);
        const uint32_t maskedChecksum = ((unmaskedChecksum >> 15) | (unmaskedChecksum << 17)) + 0xa282ead8;

        /// Resize dst to fit the new frame
        /// The size of the frame is the compressedDataLength and 8 header bytes (1 chunktype, 3 length, 4 checksum)
        const size_t totalFrameLength = 8 + compressedLength;
        dst.reserve(currentDstPosition + totalFrameLength);
        /// Write header data
        /// Chunk identifier (0 for compressed chunk)
        std::memcpy(dst.data() + currentDstPosition, "0", 1);
        /// Chunk length -> always fits into 3 bytes, take lowest 3 bytes of chunk length variable
        const uint32_t chunkLength = compressedLength + 4;
        std::memcpy(dst.data() + currentDstPosition + 1, &chunkLength, 3);
        /// Checksum
        std::memcpy(dst.data() + currentDstPosition + 4, &maskedChecksum, 4);
        /// Payload
        std::memcpy(dst.data() + currentDstPosition + 8, compressedData.c_str(), compressedLength);

        /// Update src and dst pos
        currentSrcPosition += bytesToCompress;
        currentDstPosition += totalFrameLength;
    }
    return EncodingResult(EncodeStatusType::SUCCESSFULLY_ENCODED, currentDstPosition);
}

std::ostream& SnappyEncoder::toString(std::ostream& str) const
{
    str << "SnappyEncoder";
    return str;
}

EncoderRegistryReturnType EncoderGeneratedRegistrar::RegisterSnappyEncoder(EncoderRegistryArguments)
{
    return std::make_unique<SnappyEncoder>();
}
}
