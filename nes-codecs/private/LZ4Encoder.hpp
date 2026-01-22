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
#include <memory>
#include <string>
#include <Encoders/Encoder.hpp>
#include <lz4frame.h>

namespace NES
{
/// Encoder to encode a batch of data into the framing-LZ4 format
/// Will create a full frame for every method call.

class LZ4Encoder final : public Encoder
{
public:
    explicit LZ4Encoder();
    ~LZ4Encoder() override;

    LZ4Encoder(const LZ4Encoder&) = delete;
    LZ4Encoder& operator=(const LZ4Encoder&) = delete;
    LZ4Encoder(LZ4Encoder&&) = delete;
    LZ4Encoder& operator=(LZ4Encoder&&) = delete;

    [[nodiscard]] EncodingResult encodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const override;
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// Includes all compression options like block size, linkage, compression level etc.
    /// Will currently be set to default settings but has potential for future customization
    LZ4F_preferences_t preferences;
};
}
