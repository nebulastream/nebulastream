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

#include <chrono>
#include <future>
#include <memory>

#include <Async/AsyncSourceExecutor.hpp>
#include <Blocking/BlockingSourceRunner.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/BlockingSource.hpp>

namespace NES::Sources
{

struct SourceExecutionContext
{
    SourceExecutionContext() = delete;

    SourceExecutionContext(
        const OriginId originId,
        std::variant<std::unique_ptr<BlockingSource>, std::unique_ptr<AsyncSource>> sourceImpl,
        std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
        std::unique_ptr<InputFormatters::InputFormatter> inputFormatter)
        : originId(originId)
        , sourceImpl(std::move(sourceImpl))
        , bufferProvider(std::move(bufferProvider))
        , inputFormatter(std::move(inputFormatter))
        , currSequenceNumber(SequenceNumber::INITIAL)
    {
    }

    void addBufferMetadata(IOBuffer& buffer)
    {
        buffer.setOriginId(originId);
        buffer.setCreationTimestampInMS(
            Runtime::Timestamp(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                    .count()));
        buffer.setSequenceNumber(SequenceNumber{currSequenceNumber++});
        buffer.setChunkNumber(ChunkNumber{1});
        buffer.setLastChunk(true);

        NES_TRACE(
            "Setting buffer metadata with originId={} sequenceNumber={} chunkNumber={} lastChunk={}",
            buffer.getOriginId(),
            buffer.getSequenceNumber(),
            buffer.getChunkNumber(),
            buffer.isLastChunk());
    }

    const OriginId originId;
    std::variant<std::unique_ptr<BlockingSource>, std::unique_ptr<AsyncSource>> sourceImpl;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter;
    size_t currSequenceNumber;
};

}
