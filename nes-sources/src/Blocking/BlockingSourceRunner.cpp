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
#include <Blocking/BlockingSourceRunner.hpp>

#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

BlockingSourceRunner::BlockingSourceRunner(
    OriginId originId,
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
    std::unique_ptr<BlockingSource> sourceImplementation,
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter)
    : originId(originId)
    , bufferProvider(bufferProvider)
    , sourceImpl(std::move(sourceImplementation))
    , inputFormatter(std::move(inputFormatter))
{
    NES_ASSERT(this->bufferProvider, "Invalid buffer manager");
}

namespace detail
{
void addBufferMetaData(OriginId originId, SequenceNumber sequenceNumber, Memory::TupleBuffer& buffer)
{
    /// set the origin id for this source
    buffer.setOriginId(originId);
    /// set the creation timestamp
    buffer.setCreationTimestampInMS(
        Runtime::Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    /// Set the sequence number of this buffer.
    /// A data source generates a monotonic increasing sequence number
    buffer.setSequenceNumber(sequenceNumber);
    buffer.setChunkNumber(ChunkNumber(1));
    buffer.setLastChunk(true);
    NES_TRACE(
        "Setting the buffer metadata for source {} with originId={} sequenceNumber={} chunkNumber={} lastChunk={}",
        buffer.getOriginId(),
        buffer.getOriginId(),
        buffer.getSequenceNumber(),
        buffer.getChunkNumber(),
        buffer.isLastChunk());
}

using EmitFn = std::function<void(Memory::TupleBuffer)>;
void threadSetup(OriginId originId)
{
    setThreadName(fmt::format("DataSrc-{}", originId));
}

/// RAII-Wrapper around source open and close
struct SourceHandle
{
    explicit SourceHandle(BlockingSource& source) : source(source) { source.open(); }
    SourceHandle(const SourceHandle& other) = delete;
    SourceHandle(SourceHandle&& other) noexcept = delete;
    SourceHandle& operator=(const SourceHandle& other) = delete;
    SourceHandle& operator=(SourceHandle&& other) noexcept = delete;

    ~SourceHandle()
    {
        /// Throwing in a destructor would terminate the application
        try
        {
            source.close();
        }
        catch (...)
        {
            tryLogCurrentException();
        }
    }
    BlockingSource& source; ///NOLINT Source handle should never outlive the source
};

BlockingSourceTermination dataSourceThreadRoutine(
    const std::stop_token& stopToken,
    BlockingSource& source,
    Memory::AbstractBufferProvider& bufferProvider,
    InputFormatters::InputFormatter& inputFormatter,
    const EmitFn& emit)
{
    SourceHandle const sourceHandle(source);
    while (!stopToken.stop_requested())
    {
        /// 4 Things that could happen:
        /// 1. Happy Path: Source produces a tuple buffer and emit is called. The loop continues.
        /// 2. Stop was requested by the owner of the data source. Stop is propagated to the source implementation.
        ///    fillTupleBuffer will return false, however this is not a EndOfStream, the source simply could not produce a buffer.
        ///    The thread exits with `StopRequested`
        /// 3. EndOfStream was signaled by the source implementation. It returned false, but the Stop Token was not triggered.
        ///    The thread exits with `EndOfStream`
        /// 4. Failure. The fillTupleBuffer method will throw an exception, the exception is propagted to the SourceThread via the return promise.
        ///    The thread exists with an exception
        auto emptyBuffer = bufferProvider.getBufferBlocking();
        auto numReadBytes = source.fillBuffer(emptyBuffer, stopToken);

        if (numReadBytes != 0)
        {
            inputFormatter.parseTupleBufferRaw(emptyBuffer, bufferProvider, numReadBytes, emit);
        }

        if (stopToken.stop_requested())
        {
            return {BlockingSourceTermination::StopRequested};
        }

        if (numReadBytes == 0)
        {
            return {BlockingSourceTermination::EndOfStream};
        }
    }
    return {BlockingSourceTermination::StopRequested};
}

void dataSourceThread(
    const std::stop_token& stopToken,
    std::promise<BlockingSourceTermination> result,
    BlockingSource* source,
    SourceReturnType::EmitFunction emit,
    OriginId originId,
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter,
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
{
    threadSetup(originId);

    size_t sequenceNumberGenerator = SequenceNumber::INITIAL;
    EmitFn const dataEmit = [&](Memory::TupleBuffer&& buffer)
    {
        addBufferMetaData(originId, SequenceNumber(sequenceNumberGenerator++), buffer);
        emit(originId, SourceReturnType::Data{std::move(buffer)});
    };

    try
    {
        result.set_value(dataSourceThreadRoutine(stopToken, *source, *bufferProvider, *inputFormatter, dataEmit));
        if (!stopToken.stop_requested())
        {
            emit(originId, SourceReturnType::EoS{});
        }
    }
    catch (std::exception const& e)
    {
        auto ingestionException = RunningRoutineFailure(e.what());
        result.set_exception(std::make_exception_ptr(ingestionException));
        emit(originId, SourceReturnType::Error{std::move(ingestionException)});
    }
}
}

bool BlockingSourceRunner::start(SourceReturnType::EmitFunction&& emitFunction)
{
    NES_ASSERT(this->originId != INVALID_ORIGIN_ID, "The id of the source is not set properly");
    if (started.exchange(true))
    {
        return false;
    }

    NES_DEBUG("BlockingSourceThread  {} : start source", originId);
    std::promise<BlockingSourceTermination> terminationPromise;
    this->terminationFuture = terminationPromise.get_future();

    std::jthread sourceThread(
        detail::dataSourceThread,
        std::move(terminationPromise),
        sourceImpl.get(),
        std::move(emitFunction),
        originId,
        std::move(inputFormatter),
        std::move(bufferProvider));
    thread = std::move(sourceThread);
    return true;
}

bool BlockingSourceRunner::stop()
{
    PRECONDITION(thread.get_id() != std::this_thread::get_id(), "BlockingSourceRunner should never request the source termination");
    if (!started.exchange(false))
    {
        return false;
    }

    NES_DEBUG("BlockingSourceThread  {} : stop source", originId);
    thread.request_stop();

    try
    {
        this->terminationFuture.get();
        auto deletedOnScopeExit = std::move(thread);
    }
    catch (const Exception& exception)
    {
        NES_ERROR("BlockingSource encountered an error: {}", exception.what());
    }
    NES_DEBUG("BlockingSourceThread  {} : stopped", originId);
    return true;
}

std::ostream& BlockingSourceRunner::toString(std::ostream& str) const override
{
    str << "\nBlockingSourceThread(";
    str << "\n  originId: " << originId;
    str << "\n  bufferProvider: " << bufferProvider;
    str << "\n  sourceImplementation:" << sourceImpl;
    str << ")\n";
    return str;
}

}