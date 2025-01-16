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
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SourceThread.hpp>
#include <magic_enum.hpp>

namespace NES::Sources
{

SourceThread::SourceThread(
    OriginId originId,
    std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
    size_t numSourceLocalBuffers,
    std::unique_ptr<Source> sourceImplementation,
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter)
    : originId(originId)
    , localBufferManager(std::move(poolProvider))
    , numSourceLocalBuffers(numSourceLocalBuffers)
    , sourceImplementation(std::move(sourceImplementation))
    , inputFormatter(std::move(inputFormatter))
{
    NES_ASSERT(this->localBufferManager, "Invalid buffer manager");
}

namespace detail
{
void addBufferMetaData(OriginId originId, SequenceNumber sequenceNumber, Memory::TupleBuffer& buffer)
{
    /// set the origin id for this source
    buffer.setOriginId(originId);
    /// set the creation timestamp
    buffer.setCreationTimestampInMS(Runtime::Timestamp(
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

using EmitFn = std::function<void(Memory::TupleBuffer, bool addBufferMetadata)>;
void threadSetup(OriginId originId)
{
    setThreadName(fmt::format("DataSrc-{}", originId));
}

/// RAII-Wrapper around source open and close
struct SourceHandle
{
    explicit SourceHandle(Source& source) : source(source) { source.open(); }
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
    Source& source; ///NOLINT Source handle should never outlive the source
};

SourceImplementationTermination dataSourceThreadRoutine(
    const std::stop_token& stopToken,
    Source& source,
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
        auto numReadBytes = source.fillTupleBuffer(emptyBuffer, stopToken);

        if (numReadBytes != 0)
        {
            inputFormatter.parseTupleBufferRaw(emptyBuffer, bufferProvider, numReadBytes, emit);
        }

        if (stopToken.stop_requested())
        {
            return {SourceImplementationTermination::StopRequested};
        }

        if (numReadBytes == 0)
        {
            return {SourceImplementationTermination::EndOfStream};
        }
    }
    return {SourceImplementationTermination::StopRequested};
}

void dataSourceThread(
    const std::stop_token& stopToken,
    std::promise<SourceImplementationTermination> result,
    Source* source,
    SourceReturnType::EmitFunction emit,
    OriginId originId,
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter,
    std::optional<std::shared_ptr<Memory::AbstractBufferProvider>> bufferProvider)
{
    threadSetup(originId);
    if (!bufferProvider)
    {
        emit(originId, SourceReturnType::Error(CannotAllocateBuffer()));
        result.set_exception(std::make_exception_ptr(CannotAllocateBuffer()));
        return;
    }

    size_t sequenceNumberGenerator = SequenceNumber::INITIAL;
    EmitFn const dataEmit = [&](Memory::TupleBuffer&& buffer, bool shouldAddMetadata)
    {
        if (shouldAddMetadata)
        {
            addBufferMetaData(originId, SequenceNumber(sequenceNumberGenerator++), buffer);
        }
        emit(originId, SourceReturnType::Data{std::move(buffer)});
    };

    try
    {
        result.set_value(dataSourceThreadRoutine(stopToken, *source, **bufferProvider, *inputFormatter, dataEmit));
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

bool SourceThread::start(SourceReturnType::EmitFunction&& emitFunction)
{
    NES_ASSERT(this->originId != INVALID_ORIGIN_ID, "The id of the source is not set properly");
    if (started.exchange(true))
    {
        return false;
    }

    NES_DEBUG("SourceThread  {} : start source", originId);
    std::promise<SourceImplementationTermination> terminationPromise;
    this->terminationFuture = terminationPromise.get_future();

    std::jthread sourceThread(
        detail::dataSourceThread,
        std::move(terminationPromise),
        sourceImplementation.get(),
        std::move(emitFunction),
        originId,
        std::move(inputFormatter),
        localBufferManager->createFixedSizeBufferPool(numSourceLocalBuffers));
    thread = std::move(sourceThread);
    return true;
}

bool SourceThread::stop()
{
    PRECONDITION(thread.get_id() != std::this_thread::get_id(), "DataSrc Thread should never request the source termination");
    if (!started.exchange(false))
    {
        return false;
    }

    NES_DEBUG("SourceThread  {} : stop source", originId);
    thread.request_stop();

    try
    {
        this->terminationFuture.get();
        auto deletedOnScopeExit = std::move(thread);
    }
    catch (const Exception& exception)
    {
        NES_ERROR("Source encountered an error: {}", exception.what());
    }
    NES_DEBUG("SourceThread  {} : stopped", originId);
    return true;
}

OriginId SourceThread::getOriginId() const
{
    return this->originId;
}

std::ostream& operator<<(std::ostream& out, const SourceThread& sourceThread)
{
    out << "\nSourceThread(";
    out << "\n  originId: " << sourceThread.originId;
    out << "\n  numSourceLocalBuffers: " << sourceThread.numSourceLocalBuffers;
    out << "\n  source implementation:" << *sourceThread.sourceImplementation;
    out << ")\n";
    return out;
}

}
