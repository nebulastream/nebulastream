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

#include <SourceThread.hpp>

#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

SourceThread::SourceThread(
    Ingestion ingestion,
    OriginId originId,
    std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
    size_t numOfLocalBuffers,
    std::unique_ptr<Source> sourceImplementation)
    : originId(originId)
    , localBufferManager(std::move(poolProvider))
    , numOfLocalBuffers(numOfLocalBuffers)
    , sourceImplementation(std::move(sourceImplementation))
    , ingestion(std::move(ingestion))
{
    PRECONDITION(this->localBufferManager, "Invalid buffer manager");
}

namespace detail
{
void addBufferMetaData(OriginId originId, SequenceNumber sequenceNumber, Memory::TupleBuffer& buffer)
{
    /// set the origin id for this source
    buffer.setOriginId(originId);
    /// set the creation timestamp
    buffer.setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    /// Set the sequence number of this buffer.
    /// A data source generates a monotonic increasing sequence number
    buffer.setSequenceNumber(sequenceNumber);
    buffer.setChunkNumber(INITIAL_CHUNK_NUMBER);
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
    explicit SourceHandle(Source& source, std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider) : source(source)
    {
        source.open(std::move(bufferProvider));
    }
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
    Ingestion ingestion,
    Source& source,
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
    const EmitFn& emit)
{
    const SourceHandle sourceHandle(source, bufferProvider);
    while (ingestion.wait(stopToken), !stopToken.stop_requested())
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

        std::optional<Memory::TupleBuffer> emptyBuffer;
        while (!emptyBuffer && !stopToken.stop_requested())
        {
            emptyBuffer = bufferProvider->getBufferWithTimeout(std::chrono::milliseconds(25));
        }
        if (stopToken.stop_requested())
        {
            return {SourceImplementationTermination::StopRequested};
        }

        auto numReadBytes = source.fillTupleBuffer(*emptyBuffer, stopToken);

        if (numReadBytes != 0)
        {
            emptyBuffer->setNumberOfTuples(numReadBytes);
            emit(*emptyBuffer, true);
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

struct DestroyOnExit
{
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    ~DestroyOnExit() { bufferProvider->destroy(); }
};

void dataSourceThread(
    const std::stop_token& stopToken,
    Ingestion ingestion,
    std::promise<SourceImplementationTermination> result,
    Source* source,
    SourceReturnType::EmitFunction emit,
    OriginId originId,
    std::optional<std::shared_ptr<Memory::AbstractBufferProvider>> bufferProvider)
{
    threadSetup(originId);
    if (!bufferProvider)
    {
        emit(originId, SourceReturnType::Error(BufferAllocationFailure()));
        result.set_exception(std::make_exception_ptr(BufferAllocationFailure()));
        return;
    }

    const DestroyOnExit onExit{bufferProvider.value()};
    size_t sequenceNumberGenerator = SequenceNumber::INITIAL;
    const EmitFn dataEmit = [&](Memory::TupleBuffer&& buffer, bool shouldAddMetadata)
    {
        if (shouldAddMetadata)
        {
            addBufferMetaData(originId, SequenceNumber(sequenceNumberGenerator++), buffer);
        }
        emit(originId, SourceReturnType::Data{std::move(buffer)});
    };

    try
    {
        result.set_value_at_thread_exit(dataSourceThreadRoutine(stopToken, std::move(ingestion), *source, *bufferProvider, dataEmit));
        if (!stopToken.stop_requested())
        {
            emit(originId, SourceReturnType::EoS{});
        }
    }
    catch (const std::exception& e)
    {
        auto ingestionException = RunningRoutineFailure(e.what());
        result.set_exception_at_thread_exit(std::make_exception_ptr(ingestionException));
        emit(originId, SourceReturnType::Error{std::move(ingestionException)});
    }
}
}

bool SourceThread::start(SourceReturnType::EmitFunction&& emitFunction)
{
    INVARIANT(this->originId != INVALID_ORIGIN_ID, "The id of the source is not set properly");
    if (started.exchange(true))
    {
        return false;
    }

    NES_DEBUG("Starting source with originId: {}", originId);
    std::promise<SourceImplementationTermination> terminationPromise;
    this->terminationFuture = terminationPromise.get_future();

    std::jthread sourceThread(
        detail::dataSourceThread,
        ingestion,
        std::move(terminationPromise),
        sourceImplementation.get(),
        std::move(emitFunction),
        originId,
        localBufferManager->createFixedSizeBufferPool(numOfLocalBuffers));
    thread = std::move(sourceThread);
    return true;
}

void SourceThread::stop()
{
    PRECONDITION(thread.get_id() != std::this_thread::get_id(), "DataSrc Thread should never request the source termination");

    NES_DEBUG("SourceThread  {} : stop source", originId);
    thread.request_stop();
    {
        auto deletedOnScopeExit = std::move(thread);
    }
    NES_DEBUG("SourceThread  {} : stopped", originId);

    try
    {
        this->terminationFuture.get();
    }
    catch (const Exception& exception)
    {
        NES_ERROR("Source encountered an error: {}", exception.what());
    }
}

SourceReturnType::TryStopResult SourceThread::tryStop(std::chrono::milliseconds timeout)
{
    PRECONDITION(thread.get_id() != std::this_thread::get_id(), "DataSrc Thread should never request the source termination");
    NES_DEBUG("SourceThread  {} : attempting to stop source", originId);
    thread.request_stop();

    try
    {
        auto result = this->terminationFuture.wait_for(timeout);
        if (result == std::future_status::timeout)
        {
            NES_DEBUG("SourceThread  {} : source was not stopped during timeout", originId);
            return SourceReturnType::TryStopResult::TIMEOUT;
        }
        auto deletedOnScopeExit = std::move(thread);
    }
    catch (const Exception& exception)
    {
        NES_ERROR("Source encountered an error: {}", exception.what());
    }

    NES_DEBUG("SourceThread  {} : stopped", originId);
    return SourceReturnType::TryStopResult::SUCCESS;
}

OriginId SourceThread::getOriginId() const
{
    return this->originId;
}

std::ostream& operator<<(std::ostream& out, const SourceThread& sourceThread)
{
    out << "\nSourceThread(";
    out << "\n  originId: " << sourceThread.originId;
    out << "\n  numOfLocalBuffers: " << sourceThread.numOfLocalBuffers;
    out << "\n  source implementation:" << *sourceThread.sourceImplementation;
    out << ")\n";
    return out;
}

}
