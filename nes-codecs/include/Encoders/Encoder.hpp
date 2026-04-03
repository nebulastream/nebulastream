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
#include <cstdint>
#include <ostream>
#include <span>
#include <vector>

namespace NES
{
/// Encoder is an interface for all specific encoder classes that encode data according to the rules
/// of a specific codec, for example, the compression codec LZ4.

class Encoder
{
public:
    /// This enum is used to signalize whether the encoding operation was able to write the whole encoded content in the single provided buffer
    enum class EncodeStatusType : uint8_t
    {
        SUCCESSFULLY_ENCODED,
        ENCODING_ERROR
    };

    struct EncodingResult
    {
        EncodeStatusType status;
        size_t compressedSize;
    };

    Encoder() = default;
    virtual ~Encoder() = default;

    /// Will encode the contents of src and write the result into dst. Any previous contents in dst will be overwritten
    /// Function must at first resize the dst vector so that it may encapsulate all the encoded data.
    [[nodiscard]] virtual EncodingResult encodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const = 0;

    friend std::ostream& operator<<(std::ostream& out, const Encoder& encoder);

protected:
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};


}
