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
#include <string>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// Decoder is an interface for all specific decoder classes that obtain a tuple buffer filled with data that has been encoded following
/// a certain codec, for example the compression codec LZ4. A decoder can decode and emit the contents of the buffer with decodeAndEmit().

class Decoder
{
public:
    /// Enum used to signalize if the decode method needs a new, empty buffer.
    enum class DecodeStatusType : uint8_t
    {
        FINISHED_DECODING_CURRENT_BUFFER,
        DECODING_REQUIRES_ANOTHER_BUFFER
    };

    enum class DecodingResultStatus : uint8_t
    {
        SUCCESSFULLY_DECODED,
        DECODING_ERROR
    };

    struct DecodingResult
    {
        DecodingResultStatus status;
        size_t decompressedSize;
    };

    Decoder() = default;
    virtual ~Decoder() = default;

    /// Decodes the data in encodedBuffer and writes the decoded data in the provided emptyDecodedBuffer.
    /// If the space in the decodedBuffer does not suffice to decode all the data, the emitAndProvide function will emit the decodedBuffer
    /// and provide a new, empty buffer to further write decoded data to.
    /// If called with the FINISHED_DECODING_CURRENT_BUFFER enum, emitAndProvide should only emit and not provide a new buffer.
    virtual void decodeAndEmit(
        TupleBuffer& encodedBuffer,
        TupleBuffer& emptyDecodedBuffer,
        const std::function<std::optional<TupleBuffer>(TupleBuffer&, const DecodeStatusType)>& emitAndProvide) = 0;

    /// Stateless function that will decode the whole data in src into the dst vector
    /// It is assumed that dst already allocated enough memory to hold the output
    [[nodiscard]] virtual DecodingResult decodeBuffer(std::span<const std::byte> src, std::vector<char>& dst) const = 0;

    friend std::ostream& operator<<(std::ostream& out, const Decoder& decoder);

protected:
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};
}

FMT_OSTREAM(NES::Decoder);
