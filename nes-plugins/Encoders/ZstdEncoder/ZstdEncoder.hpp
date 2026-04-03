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
#include <ostream>
#include <span>
#include <vector>
#include <Encoders/Encoder.hpp>

namespace NES
{
/// Encoder that encodes a batch of data into Zstd compressed data
/// A single batch is encoded into a whole frame
class ZstdEncoder final : public Encoder
{
public:
    explicit ZstdEncoder();
    ~ZstdEncoder() override;

    ZstdEncoder(const ZstdEncoder&) = delete;
    ZstdEncoder& operator=(const ZstdEncoder&) = delete;
    ZstdEncoder(ZstdEncoder&&) = delete;
    ZstdEncoder& operator=(ZstdEncoder&&) = delete;

    [[nodiscard]] EncodingResult encodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const override;
protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;
};
}
