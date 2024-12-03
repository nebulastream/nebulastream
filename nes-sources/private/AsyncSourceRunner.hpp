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

#include <Sources/AsyncSourceExecutor.hpp>

#include <cstdint>
#include <iostream>
#include <memory>

#include <Util/Logger/Logger.hpp>
#include <boost/asio/awaitable.hpp>

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>


namespace NES::Sources
{

namespace asio = boost::asio;

class AsyncSourceRunner : public std::enable_shared_from_this<AsyncSourceRunner>
{
public:
    explicit AsyncSourceRunner(
        OriginId originId,
        std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
        SourceReturnType::EmitFunction&& emitFn,
        size_t numSourceLocalBuffers,
        std::unique_ptr<Source> sourceImpl,
        std::unique_ptr<InputFormatters::InputFormatter> inputFormatter,
        std::shared_ptr<AsyncSourceExecutor> executor);

    AsyncSourceRunner() = delete;
    virtual ~AsyncSourceRunner() = default;

    AsyncSourceRunner(const AsyncSourceRunner&) = delete;
    AsyncSourceRunner& operator=(const AsyncSourceRunner&) = delete;
    AsyncSourceRunner(AsyncSourceRunner&&) = delete;
    AsyncSourceRunner& operator=(AsyncSourceRunner&&) = delete;

    void start();
    void stop();
    asio::awaitable<void> coroutine();

    [[nodiscard]] OriginId getOriginId() const { return originId; }


protected:
    enum class RunnerState
    {
        Initial,
        Running,
        Stopped,
    };

    const OriginId originId;
    uint64_t maxSequenceNumber;
    std::shared_ptr<NES::Memory::AbstractBufferProvider> bufferProvider;
    const SourceReturnType::EmitFunction emitFn;
    std::unique_ptr<Source> sourceImpl;
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter;
    std::shared_ptr<AsyncSourceExecutor> executor;
    RunnerState state{RunnerState::Initial};

    void addBufferMetadata(ByteBuffer& buffer, SequenceNumber sequenceNumber)
    {
        buffer.setOriginId(originId);
        buffer.setCreationTimestampInMS(Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
        buffer.setSequenceNumber(sequenceNumber);
        buffer.setChunkNumber(ChunkNumber(1));
        buffer.setLastChunk(true);

        NES_TRACE(
            "Setting buffer metadata with originId={} sequenceNumber={} chunkNumber={} lastChunk={}",
            buffer.getOriginId(),
            buffer.getSequenceNumber(),
            buffer.getChunkNumber(),
            buffer.isLastChunk());
    }


    friend std::ostream& operator<<(std::ostream& out, const AsyncSourceRunner& sourceRunner)
    {
        out << "\nAsyncSourceRunner(";
        out << "\n  originId: " << sourceRunner.originId;
        out << "\n  maxSequenceNumber: " << sourceRunner.maxSequenceNumber;
        out << "\n  source implementation:" << *sourceRunner.sourceImpl;
        out << ")\n";
        return out;
    }
};

}
