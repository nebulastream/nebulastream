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
/// a certain codec, for example the compression codec LZ4. A decoder can decode the contents of the buffer with decode() and return
/// a vector of tuple buffers filled with the decoded data
/// (Vector, since the size of the decoded data rarely corresponds to the size of the encoded data).

class Decoder
{
public:
    /// Helper enum class which contains all codecs for which we provide a decoder. None is a special case and does not have a
    /// decoder.
    enum class Codec : uint8_t
    {
        None
    };

    Decoder() = default;
    virtual ~Decoder() = default;

    /// Decode the data in encodedBuffer starting from positionInCurrentBuffer and write the decoded data in the provided emptyDecodedBuffer.
    /// Returns true, if the whole encodedBuffer was decoded.
    /// Returns false, if the emptyDecodedBuffer did not provide enough space to decode all the remaining data.
    /// In this case, decode should be called again with the same encoded buffer and a new emptyDecodedBuffer.
    /// Will update positionInCurrentBuffer to the index of the first byte in encodedBuffer that was not decoded yet.
    virtual bool decode(TupleBuffer& encodedBuffer, TupleBuffer& emptyDecodedBuffer) = 0;

    [[nodiscard]] static std::string getCodecOptionsAsString();

    friend std::ostream& operator<<(std::ostream& out, const Decoder& decoder);

protected:
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
    /// Index of first not-decoded byte in the tuple buffer that is being decoded currently
    /// Will be set back to 0 if decode returns with true
    size_t positionInCurrentBuffer = 0;
};
}

FMT_OSTREAM(NES::Decoder);
