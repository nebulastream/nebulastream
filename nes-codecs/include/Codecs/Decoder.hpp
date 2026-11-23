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

#include <expected>
#include <vector>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Turns the bytes a source reads (e.g. zstd-compressed CSV) into the bytes its input formatter parses.
/// Called only from its source's thread, in stream order, so it may keep state across calls.
/// Buffers store their byte count as their tuple count.
class Decoder
{
public:
    Decoder() = default;
    virtual ~Decoder() = default;
    Decoder(const Decoder&) = delete;
    Decoder& operator=(const Decoder&) = delete;
    Decoder(Decoder&&) = delete;
    Decoder& operator=(Decoder&&) = delete;

    /// Returns any number of decoded buffers, taken from the buffer provider, or CannotDecodeSourceData.
    virtual std::expected<std::vector<TupleBuffer>, Exception> decode(const TupleBuffer& encoded, AbstractBufferProvider& bufferProvider)
        = 0;

    /// The encoded stream has ended: returns the decoded buffers the decoder still held back.
    virtual std::expected<std::vector<TupleBuffer>, Exception> flush(AbstractBufferProvider& /*bufferProvider*/)
    {
        return std::vector<TupleBuffer>{};
    }
};

}
