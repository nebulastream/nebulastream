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

#include <Decoders/Decoder.hpp>

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <stop_token>
#include <utility>
#include <vector>
#include <Decoders/DecoderStage.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
DecoderStage::BufferProvider bufferProviderFor(AbstractBufferProvider& bufferProvider, const std::stop_token& stopToken)
{
    /// Same wait as the source thread's own: a timeout per attempt, so a stop request is noticed.
    return [&bufferProvider, stopToken]() -> std::optional<TupleBuffer>
    {
        std::optional<TupleBuffer> buffer;
        while (!buffer && !stopToken.stop_requested())
        {
            buffer = bufferProvider.getBufferWithTimeout(std::chrono::milliseconds(25));
        }
        return buffer;
    };
}

std::span<const std::byte> bytesOf(const TupleBuffer& buffer)
{
    return buffer.getAvailableMemoryArea<std::byte>().first(buffer.getNumberOfTuples());
}
}

Decoder::Decoder(std::vector<std::unique_ptr<DecoderStage>> stages) : stages(std::move(stages))
{
    PRECONDITION(!this->stages.empty(), "A decoder needs at least one stage");
}

std::vector<TupleBuffer>
Decoder::decode(const std::span<const std::byte> encoded, AbstractBufferProvider& bufferProvider, const std::stop_token& stopToken)
{
    const auto provideBuffer = bufferProviderFor(bufferProvider, stopToken);
    std::vector<TupleBuffer> output;
    stages.front()->process(encoded, provideBuffer, output);
    return runFrom(1, std::move(output), provideBuffer);
}

std::vector<TupleBuffer> Decoder::flush(AbstractBufferProvider& bufferProvider, const std::stop_token& stopToken)
{
    const auto provideBuffer = bufferProviderFor(bufferProvider, stopToken);
    std::vector<TupleBuffer> result;
    for (size_t stage = 0; stage < stages.size(); ++stage)
    {
        std::vector<TupleBuffer> flushed;
        stages[stage]->flush(provideBuffer, flushed);
        for (auto& buffer : runFrom(stage + 1, std::move(flushed), provideBuffer))
        {
            result.push_back(std::move(buffer));
        }
    }
    return result;
}

std::vector<TupleBuffer>
Decoder::runFrom(const size_t first, std::vector<TupleBuffer> buffers, const DecoderStage::BufferProvider& provideBuffer)
{
    for (size_t stage = first; stage < stages.size(); ++stage)
    {
        std::vector<TupleBuffer> next;
        for (const auto& buffer : buffers)
        {
            stages[stage]->process(bytesOf(buffer), provideBuffer, next);
        }
        buffers = std::move(next);
    }
    return buffers;
}

}
