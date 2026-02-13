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
/// Transforms a batch of data into the snappy framing format. Might produce multiple frames, since only 65536 bytes can be compressed into one frame.
class SnappyEncoder final : public Encoder
{
public:
    explicit SnappyEncoder();
    ~SnappyEncoder() override;
    
    SnappyEncoder(const SnappyEncoder&) = delete;
    SnappyEncoder& operator=(const SnappyEncoder&) = delete;
    SnappyEncoder(SnappyEncoder&&) = delete;
    SnappyEncoder& operator=(SnappyEncoder&&) = delete;

    [[nodiscard]] EncodingResult encodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const override;
protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;
};
}
