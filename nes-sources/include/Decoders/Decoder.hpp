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

#include <vector>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// Decoder is an interface for all specific decoder classes that obtain a tuple buffer filled with data that has been encoded following
/// a certain codec, for example the compression codec LZ4. A decoder can decode the contents of the buffer with decode() and return
/// a vector of tuple buffers filled with the decoded data
/// (Vector, since the size of the decoded data rarely corresponds to the size of the decoded data).

class Decoder
{
    public:
    Decoder() = default;
    virtual ~Decoder() = default;

    /// Decode the data in encodedBuffer and write the decoded data in new tuple buffers.
    /// @return Vector of tuple buffers contained the decoded data.
    virtual std::vector<TupleBuffer> decode(TupleBuffer& encodedBuffer, AbstractBufferProvider& bufferProvider) = 0;

    friend std::ostream& operator<<(std::ostream& out, const Decoder& decoder);

protected:
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};
}

FMT_OSTREAM(NES::Decoder);
