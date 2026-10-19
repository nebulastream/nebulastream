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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/ReplayableSourceStorage.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceCheckpointProgress.hpp>
#include <Sources/SourceCheckpointing.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
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
    std::optional<SourceCheckpointing> checkpointing)
    : originId(originId)
    , localBufferManager(std::move(poolProvider))
    , sourceImplementation(std::move(sourceImplementation))
    , backpressureListener(std::move(backpressureListener))
    , checkpointing(std::move(checkpointing))
{
    PRECONDITION(this->localBufferManager, "Invalid buffer manager");
    if (this->checkpointing and this->checkpointing->checkpointManager->isCheckpointingEnabled())
    {
        checkpointProgress = std::make_shared<SourceCheckpointProgress>();
    }
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
    const EmitFn& emit)
{
    source.open(bufferProvider);
    SCOPE_SUCCESS
    {
        source.close();
    };

    /// If a source fails during fillTupleBuffer we still want to call close. However This will be handled during stack unwinding.
    /// If the close method throws again the system would terminate, we have to resort to swalling and logging the exception.
    /// The actual fillTupleBuffer failure is handled in the parent exception handler.
    SCOPE_FAIL
    {
        cpptrace::try_catch(
            [&]() { source.close(); }, []() { NES_ERROR("Source threw during failure shutdown: ", wrapExternalException().what()); });
    };

    const bool requiresMetadata = !source.addsMetadata();
    while (backpressureListener.wait(stopToken), !stopToken.stop_requested())
    {
        /// 4 Things that could happen:
        /// 1. Happy Path: Source produces a tuple buffer and emit is called. The loop continues.
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
            /// The InputFormatter expects that the source set the number of bytes this way and uses it to determine the number of tuples.
            emptyBuffer->setNumberOfTuples(fillTupleResult.getNumberOfBytes());
            emit(std::move(*emptyBuffer), requiresMetadata);
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
    SourceReturnType::EmitFunction emit,
    const OriginId originId,
    ///NOLINTNEXTLINE(performance-unnecessary-value-param) `jthread` does not allow references
    std::shared_ptr<AbstractBufferProvider> bufferProvider,
    ReplayableSourceStorage* replayStorage,
    const SequenceNumber::Underlying persistedSequenceOffset)
{
    SequenceNumber::Underlying sequenceNumberGenerator = SequenceNumber::INITIAL;
    const EmitFn dataEmit = [&](TupleBuffer&& buffer, bool shouldAddMetadata)
    {
        if (shouldAddMetadata)
        {
            const auto sequenceNumber = sequenceNumberGenerator++;
            addBufferMetaData(originId, SequenceNumber(sequenceNumber), buffer);
            if (replayStorage != nullptr)
            {
                replayStorage->persist(persistedSequenceOffset + sequenceNumber, buffer);
            }
        }
        emit(originId, SourceReturnType::Data{std::move(buffer)}, stopToken);
    };
    const auto replayEmit = [&](const SequenceNumber::Underlying persistedSequence, TupleBuffer&& buffer)
    {
        const auto sequenceNumber = sequenceNumberGenerator++;
        if (persistedSequence != persistedSequenceOffset + sequenceNumber)
        {
            throw CheckpointError(
                "Source {} cannot replay persisted buffer {}, as the buffers before it are missing", originId, persistedSequence);
        }
        addBufferMetaData(originId, SequenceNumber(sequenceNumber), buffer);
        emit(originId, SourceReturnType::Data{std::move(buffer)}, stopToken);
    };

    cpptrace::try_catch(
        [&]()
        {
            if (replayStorage != nullptr)
            {
                replayStorage->replayAfter(persistedSequenceOffset, *bufferProvider, replayEmit, stopToken);
            }
            result.set_value_at_thread_exit(
                dataSourceThreadRoutine(stopToken, std::move(backpressureListener), *source, std::move(bufferProvider), dataEmit));
            if (!stopToken.stop_requested())
            {
                emit(originId, SourceReturnType::EoS{}, stopToken);
            }
        },
        [&](NES::Exception& e)
        {
            result.set_exception_at_thread_exit(std::make_exception_ptr(e));
            emit(originId, SourceReturnType::Error{std::move(e)}, stopToken);
        },
        [&]()
        {
            auto exception = NES::wrapExternalException();
            auto backpressureListenerException = RunningRoutineFailure(exception.what());
            result.set_exception_at_thread_exit(std::make_exception_ptr(backpressureListenerException));
            emit(originId, SourceReturnType::Error{std::move(backpressureListenerException)}, stopToken);
        });
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
    if (checkpointing)
    {
        setUpReplay();
    }
    std::promise<SourceImplementationTermination> terminationPromise;
    this->terminationFuture = terminationPromise.get_future();

    Thread sourceThread(
        fmt::format("DataSrc-{}", originId),
        dataSourceThread,
        backpressureListener,
        std::move(terminationPromise),
        sourceImplementation.get(),
        std::move(emitFunction),
        originId,
        localBufferManager,
        replayStorage.get(),
        persistedSequenceOffset);
    thread = std::move(sourceThread);
    return true;
}

void SourceThread::setUpReplay()
{
    auto& checkpointManager = *checkpointing->checkpointManager;
    if (sourceImplementation->addsMetadata())
    {
        NES_WARNING("Source {} assigns its own sequence numbers, so a recovered query cannot replay its input", originId);
        return;
    }
    const auto& queryId = checkpointing->queryId;
    const auto originDirectoryName = fmt::format("origin_{}", originId);
    const auto liveDirectory = checkpointManager.getQueryDirectory(queryId) / "source_replay" / originDirectoryName;
    const auto recoveryDirectory = checkpointManager.getRecoveryQueryDirectory(queryId) / "source_replay" / originDirectoryName;
    const auto recovers = checkpointManager.shouldRecoverFromCheckpoint() and std::filesystem::exists(recoveryDirectory);
    if (not recovers and not checkpointManager.isCheckpointingEnabled())
    {
        return;
    }
    /// A recovered source continues with the live directory, which no one changed since the worker took the recovery snapshot of it.
    /// Replacing it with the snapshot instead would lose the persisted input if the worker crashed again while copying.
    if (not recovers)
    {
        std::filesystem::remove_all(liveDirectory);
    }

    replayStorage = std::make_shared<ReplayableSourceStorage>(liveDirectory);
    if (recovers)
    {
        if (const auto recoveryRound = checkpointManager.getRecoveryRoundDirectory(queryId))
        {
            if (const auto cut = CheckpointManager::loadFile(*recoveryRound / "source" / originDirectoryName))
            {
                persistedSequenceOffset = std::stoull(*cut);
            }
        }
        if (not sourceImplementation->resumeAtByteOffset(
                replayStorage->getByteOffsetAfter(std::numeric_limits<SequenceNumber::Underlying>::max())))
        {
            NES_WARNING("Source {} cannot skip the input it emitted before the recovery, so the recovered query reads it again", originId);
        }
    }

    if (not checkpointProgress)
    {
        return;
    }
    const auto callbackPrefix = CheckpointManager::getQueryCallbackPrefix(queryId) + fmt::format("source/{}/", originId);

    struct PreparedCut
    {
        std::mutex mutex;
        uint64_t round = 0;
        SequenceNumber::Underlying cut = 0;
    };

    auto preparedCut = std::make_shared<PreparedCut>();
    /// The callbacks stay registered after the source stopped, so that every round of the query covers the source, until the worker
    /// removes the query's callbacks.
    checkpointManager.registerCallback(
        callbackPrefix + "prepare",
        [&checkpointManager, queryId, originDirectoryName, progress = checkpointProgress, preparedCut, offset = persistedSequenceOffset](
            const uint64_t round)
        {
            const auto cut = offset + progress->getHighestContiguousCompletedSequence();
            checkpointManager.persistFile(
                checkpointManager.getRoundDirectory(queryId, round) / "source" / originDirectoryName, std::to_string(cut));
            const std::scoped_lock lock(preparedCut->mutex);
            preparedCut->round = round;
            preparedCut->cut = cut;
        },
        CheckpointManager::CallbackPhase::Prepare);
    /// A round that the query skipped does not cover the input of the round the source prepared last.
    checkpointManager.registerCallback(
        callbackPrefix + "commit",
        [storage = replayStorage, preparedCut](const uint64_t round)
        {
            const std::scoped_lock lock(preparedCut->mutex);
            if (preparedCut->round == round)
            {
                storage->prune(preparedCut->cut);
            }
        },
        CheckpointManager::CallbackPhase::Commit);
}

std::shared_ptr<SourceCheckpointProgress> SourceThread::getCheckpointProgress() const
{
    return checkpointProgress;
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
    out << ")\n";
    return out;
}
}
