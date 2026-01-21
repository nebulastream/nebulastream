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
#include <memory>
#include <stop_token>
#include <string>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <Thread.hpp>
#include <scope_guard.hpp>

namespace NES
{

SourceThread::SourceThread(
    BackpressureListener backpressureListener,
    OriginId originId,
    std::shared_ptr<AbstractBufferProvider> poolProvider,
    std::unique_ptr<Source> sourceImplementation,
    std::optional<std::unique_ptr<Decoder>> decoderImplementation)
    : originId(originId)
    , localBufferManager(std::move(poolProvider))
    , sourceImplementation(std::move(sourceImplementation))
    , backpressureListener(std::move(backpressureListener))
    , decoderImplementation(std::move(decoderImplementation))
{
    PRECONDITION(this->localBufferManager, "Invalid buffer manager");
}

namespace
{
void addBufferMetaData(OriginId originId, SequenceNumber sequenceNumber, TupleBuffer& buffer)
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

using EmitFn = std::function<void(TupleBuffer&&, bool addBufferMetadata)>;

SourceImplementationTermination dataSourceThreadRoutine(
    const std::stop_token& stopToken,
    BackpressureListener backpressureListener,
    Source& source,
    std::shared_ptr<AbstractBufferProvider> bufferProvider,
    const EmitFn& emit,
    std::optional<Decoder*> decoder)
{
    source.open(bufferProvider);
    SCOPE_EXIT
    {
        source.close();
    };

    const bool requiresMetadata = !source.addsMetadata();
    while (backpressureListener.wait(stopToken), !stopToken.stop_requested())
    {
        /// 4 Things that could happen:
        /// 1. Happy Path: Source produces a tuple buffer, we optionally decode the tuple buffer and emit is called. The loop continues.
        /// 2. Stop was requested by the owner of the data source. Stop is propagated to the source implementation.
        ///    The thread exits with `StopRequested`
        /// 3. EndOfStream was signaled by the source implementation. It returned 0 bytes, but the Stop Token was not triggered.
        ///    The thread exits with `EndOfStream`
        /// 4. Failure. The fillTupleBuffer method will throw an exception, the exception is propagted to the SourceThread via the return promise.
        ///    The thread exists with an exception

        std::optional<TupleBuffer> emptyBuffer;
        while (!emptyBuffer && !stopToken.stop_requested())
        {
            emptyBuffer = bufferProvider->getBufferWithTimeout(std::chrono::milliseconds(25));
        }
        if (stopToken.stop_requested())
        {
            return {SourceImplementationTermination::StopRequested};
        }

        const auto fillTupleResult = source.fillTupleBuffer(*emptyBuffer, stopToken);

        if (!fillTupleResult.isEoS())
        {
            /// The source read in raw bytes, thus we don't know the number of tuples yet.
            /// The InputFormatterTask expects that the source set the number of bytes this way and uses it to determine the number of tuples.
            emptyBuffer->setNumberOfTuples(fillTupleResult.getNumberOfBytes());

            /// If a decoder implementation is given, the source produces encoded data. We must decode the empty buffer and emit the result.
            if (decoder.has_value())
            {
                /// Lambda function to emit a decoded buffer and optionally provide an empty, new one.
                auto emitAndProvide
                    = [&emit, &bufferProvider](
                          TupleBuffer& filledDecodedBuffer, const Decoder::DecodeStatusType decodeStatus) -> std::optional<TupleBuffer>
                {
                    /// Emit the filled buffer.
                    emit(std::move(filledDecodedBuffer), true);
                    /// If required, provide a new, empty buffer.
                    return decodeStatus == Decoder::DecodeStatusType::DECODING_REQUIRES_ANOTHER_BUFFER
                        ? std::optional(bufferProvider->getBufferBlocking())
                        : std::nullopt;
                };

                /// Get an initial empty buffer for the decoded data
                auto decodedBuffer = bufferProvider->getBufferBlocking();
                decoder.value()->decodeAndEmit(*emptyBuffer, decodedBuffer, emitAndProvide);
            }
            else
            {
                emit(std::move(*emptyBuffer), true);
            }
        }
        else
        {
            if (stopToken.stop_requested())
            {
                return {SourceImplementationTermination::StopRequested};
            }

            return {SourceImplementationTermination::EndOfStream};
        }
    }
    return {SourceImplementationTermination::StopRequested};
}

void dataSourceThread(
    const std::stop_token& stopToken,
    BackpressureListener backpressureListener,
    std::promise<SourceImplementationTermination> result,
    Source* source,
    std::optional<Decoder*> decoder,
    SourceReturnType::EmitFunction emit,
    const OriginId originId,
    ///NOLINTNEXTLINE(performance-unnecessary-value-param) `jthread` does not allow references
    std::shared_ptr<AbstractBufferProvider> bufferProvider)
{
    size_t sequenceNumberGenerator = SequenceNumber::INITIAL;
    const EmitFn dataEmit = [&](TupleBuffer&& buffer, bool shouldAddMetadata)
    {
        if (shouldAddMetadata)
        {
            addBufferMetaData(originId, SequenceNumber(sequenceNumberGenerator++), buffer);
        }
        emit(originId, SourceReturnType::Data{std::move(buffer)}, stopToken);
    };

    try
    {
        result.set_value_at_thread_exit(
            dataSourceThreadRoutine(stopToken, std::move(backpressureListener), *source, std::move(bufferProvider), dataEmit, decoder));
        if (!stopToken.stop_requested())
        {
            emit(originId, SourceReturnType::EoS{}, stopToken);
        }
    }
    catch (const std::exception& e)
    {
        auto backpressureListenerException = RunningRoutineFailure(e.what());
        result.set_exception_at_thread_exit(std::make_exception_ptr(backpressureListenerException));
        emit(originId, SourceReturnType::Error{std::move(backpressureListenerException)}, stopToken);
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

    /// If we have a decoder implementation, we pass a pointer to it to the sourceThread. Otherwise we pass nullopt
    std::optional<Decoder*> optionalDecoderPtr = decoderImplementation ? std::optional(decoderImplementation->get()) : std::nullopt;

    Thread sourceThread(
        fmt::format("DataSrc-{}", originId),
        dataSourceThread,
        backpressureListener,
        std::move(terminationPromise),
        sourceImplementation.get(),
        optionalDecoderPtr,
        std::move(emitFunction),
        originId,
        localBufferManager);

    thread = std::move(sourceThread);
    return true;
}

void SourceThread::stop()
{
    PRECONDITION(!thread.isCurrentThread(), "DataSrc Thread should never request the source termination");

    NES_DEBUG("SourceThread  {} : stop source", originId);
    thread.requestStop();
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
    if (!thread.joinable())
    {
        NES_DEBUG("SourceThread {}: thread is not running", originId);
        return SourceReturnType::TryStopResult::NOT_RUNNING;
    }
    PRECONDITION(!thread.isCurrentThread(), "DataSrc Thread should never request the source termination");
    NES_DEBUG("SourceThread {}: attempting to stop source", originId);
    thread.requestStop();

    try
    {
        auto result = this->terminationFuture.wait_for(timeout);
        if (result == std::future_status::timeout)
        {
            NES_DEBUG("SourceThread {}: source was not stopped during timeout", originId);
            return SourceReturnType::TryStopResult::TIMEOUT;
        }
        auto deletedOnScopeExit = std::move(thread);
    }
    catch (const Exception& exception)
    {
        NES_ERROR("Source encountered an error: {}", exception.what());
    }

    NES_DEBUG("SourceThread {}: stopped", originId);
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
    out << "\n  source implementation:" << *sourceThread.sourceImplementation;
    out << "\n decoder implementation:" << *sourceThread.decoderImplementation;
    out << ")\n";
    return out;
}

}
