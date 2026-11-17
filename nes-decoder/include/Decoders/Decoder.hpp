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
#include <span>
#include <stop_token>
#include <vector>

#include <Decoders/DecoderStage.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// Everything that happens to a source's bytes between reading them (Source::fillTupleBuffer) and
/// handing them to the query (emit): an ordered list of DecoderStages, e.g. decryption, decompression,
/// deserialization, each feeding its output to the next, in the order given by SOURCE.DECODERS. The
/// source thread only calls decode() with what the source read and emits the buffers that come back, in
/// order; how many there are depends on the stages.
class Decoder
{
public:
    explicit Decoder(std::vector<std::unique_ptr<DecoderStage>> stages);

    /// Runs the bytes the source read through all stages; returns the buffers to emit, in order.
    /// Output buffers come from bufferProvider; waiting for one ends once stopToken is triggered.
    [[nodiscard]] std::vector<TupleBuffer>
    decode(std::span<const std::byte> encoded, AbstractBufferProvider& bufferProvider, const std::stop_token& stopToken);

    /// End of stream: flushes every stage, first to last, so what one stage still held back passes
    /// through the stages after it. Returns the buffers to emit, in order.
    [[nodiscard]] std::vector<TupleBuffer> flush(AbstractBufferProvider& bufferProvider, const std::stop_token& stopToken);

private:
    std::vector<std::unique_ptr<DecoderStage>> stages;

    /// Feeds buffers through the stages from 'first' on.
    std::vector<TupleBuffer> runFrom(size_t first, std::vector<TupleBuffer> buffers, const DecoderStage::BufferProvider& provideBuffer);
};

}
