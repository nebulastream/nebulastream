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
#include <functional>
#include <optional>
#include <span>
#include <vector>

#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// One step of a source's Decoder, e.g. decryption, decompression or deserialization. A stage turns the
/// bytes it gets into output buffers for the next stage (or, for the last one, for the input formatter).
///
/// Stages have no fixed kind or position: the chain is the ordered list in SOURCE.DECODERS, the reverse of
/// the order in which the producer encoded the data (e.g. 'AES, LZ4, IntelliVue' for data that was
/// serialized, compressed, then encrypted). The only constraint is that the last stage's output is a
/// format the source's input formatter parses.
///
/// The Decoder runs every stage on the source thread, in order, so a stage sees all bytes of its source
/// exactly once and in the order they were read, and may keep state across calls: a message that spans
/// two buffers, context that arrived earlier, data it holds back. Input and output are not 1:1: one call
/// may produce no, one or several output buffers. An output buffer carries its number of bytes as its
/// number of tuples, like the raw buffers a source fills.
///
/// A stage signals data it cannot decode by throwing; the source then fails like on a read error.
class DecoderStage
{
public:
    /// Hands out an empty buffer for output. nullopt if the source is stopping.
    using BufferProvider = std::function<std::optional<TupleBuffer>()>;

    DecoderStage() = default;
    virtual ~DecoderStage() = default;
    DecoderStage(const DecoderStage&) = delete;
    DecoderStage& operator=(const DecoderStage&) = delete;
    DecoderStage(DecoderStage&&) = delete;
    DecoderStage& operator=(DecoderStage&&) = delete;

    /// Processes the next input bytes and appends finished output buffers to output. Also called with no
    /// bytes when the source returned without data, so stages that hold data back can release it by time.
    virtual void process(std::span<const std::byte> input, const BufferProvider& provideBuffer, std::vector<TupleBuffer>& output) = 0;

    /// The source reached its end of stream: append everything still held back to output.
    virtual void flush(const BufferProvider& /*provideBuffer*/, std::vector<TupleBuffer>& /*output*/) { }
};

}
