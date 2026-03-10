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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <atomic>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/ReplayableSourceStorage.hpp>
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
    PhysicalSourceId physicalSourceId,
    std::filesystem::path replayBaseDir,
    std::filesystem::path replayRecoveryBaseDir,
    std::shared_ptr<AbstractBufferProvider> poolProvider,
    std::unique_ptr<Source> sourceImplementation)
    : originId(originId)
    , physicalSourceId(physicalSourceId)
    , replayBaseDir(std::move(replayBaseDir))
    , replayRecoveryBaseDir(std::move(replayRecoveryBaseDir))
    , localBufferManager(std::move(poolProvider))
    , sourceImplementation(std::move(sourceImplementation))
    , started(false)
    , backpressureListener(std::move(backpressureListener))
    , checkpointProgress(std::make_shared<SourceCheckpointProgress>())
    , preparedCheckpointSequence(std::make_shared<std::atomic<SequenceNumber::Underlying>>(INVALID_SEQ_NUMBER.getRawValue()))
{
    PRECONDITION(this->localBufferManager, "Invalid buffer manager");
}

SourceThread::~SourceThread()
{
    if (prepareCheckpointCallbackId)
    {
        CheckpointManager::unregisterCallback(*prepareCheckpointCallbackId);
        prepareCheckpointCallbackId.reset();
    }
    if (commitCheckpointCallbackId)
    {
        CheckpointManager::unregisterCallback(*commitCheckpointCallbackId);
        commitCheckpointCallbackId.reset();
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

std::filesystem::path getReplayStorageDirectory(
    const std::filesystem::path& replayBaseDir, const OriginId originId, const PhysicalSourceId physicalSourceId)
{
    return replayBaseDir / "source_replay" / fmt::format("origin_{}_ps_{}", originId.getRawValue(), physicalSourceId.getRawValue());
}

void restoreReplayStorageFromRecoverySnapshot(
    const std::filesystem::path& replayStorageDir, const std::filesystem::path& recoveryReplayStorageDir)
{
    std::error_code ec;
    std::filesystem::remove_all(replayStorageDir, ec);
    if (ec)
    {
        throw CheckpointError("Failed to reset source replay directory {}; {}", replayStorageDir.string(), ec.message());
    }
    if (!std::filesystem::exists(recoveryReplayStorageDir))
    {
        return;
    }

    const auto replayStorageParent = replayStorageDir.parent_path();
    if (!replayStorageParent.empty())
    {
        std::filesystem::create_directories(replayStorageParent, ec);
        if (ec)
        {
            throw CheckpointError("Failed to create source replay directory {}; {}", replayStorageParent.string(), ec.message());
        }
    }

    std::filesystem::copy(
        recoveryReplayStorageDir,
        replayStorageDir,
        std::filesystem::copy_options::recursive | std::filesystem::copy_options::overwrite_existing,
        ec);
    if (ec)
    {
        throw CheckpointError(
            "Failed to restore source replay snapshot from {} to {}; {}",
            recoveryReplayStorageDir.string(),
            replayStorageDir.string(),
            ec.message());
    }
}

SourceImplementationTermination dataSourceThreadRoutine(
    const std::stop_token& stopToken,
    BackpressureListener backpressureListener,
    Source& source,
    std::shared_ptr<AbstractBufferProvider> bufferProvider,
    const EmitFn& emit)
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
    ReplayableSourceStorage* replayStorage)
{
    SequenceNumber::Underlying sequenceNumberGenerator = SequenceNumber::INITIAL;
    std::optional<SequenceNumber::Underlying> lastCheckpointedSequence;
    if (replayStorage && CheckpointManager::shouldRecoverFromCheckpoint())
    {
        lastCheckpointedSequence = replayStorage->loadLastCheckpointedSequence();
        if (lastCheckpointedSequence)
        {
            sequenceNumberGenerator = std::max(
                sequenceNumberGenerator, static_cast<SequenceNumber::Underlying>(*lastCheckpointedSequence + 1));
        }
    }

    const EmitFn dataEmit = [&](TupleBuffer&& buffer, bool shouldAddMetadata)
    {
        if (shouldAddMetadata)
        {
            const auto sequenceNumber = SequenceNumber(sequenceNumberGenerator++);
            addBufferMetaData(originId, sequenceNumber, buffer);
        }
        if (replayStorage && shouldAddMetadata)
        {
            replayStorage->persistBuffer(buffer);
        }
        emit(originId, SourceReturnType::Data{std::move(buffer)}, stopToken);
    };

    try
    {
        if (CheckpointManager::shouldRecoverFromCheckpoint() && replayStorage)
        {
            if (const auto replayed = replayStorage->replayPending(*bufferProvider, dataEmit, stopToken))
            {
                sequenceNumberGenerator
                    = std::max(
                        sequenceNumberGenerator,
                        static_cast<SequenceNumber::Underlying>(replayed->getRawValue() + 1));
            }
        }

        result.set_value_at_thread_exit(
            dataSourceThreadRoutine(stopToken, std::move(backpressureListener), *source, std::move(bufferProvider), dataEmit));
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

    const bool enableReplay = CheckpointManager::isCheckpointingEnabled() || CheckpointManager::shouldRecoverFromCheckpoint();
    checkpointProgress->reset();
    preparedCheckpointSequence->store(INVALID_SEQ_NUMBER.getRawValue(), std::memory_order_release);
    if (enableReplay)
    {
        const auto storageDir = getReplayStorageDirectory(replayBaseDir, originId, physicalSourceId);
        if (CheckpointManager::shouldRecoverFromCheckpoint() && !replayRecoveryBaseDir.empty() && replayRecoveryBaseDir != replayBaseDir)
        {
            restoreReplayStorageFromRecoverySnapshot(
                storageDir,
                getReplayStorageDirectory(replayRecoveryBaseDir, originId, physicalSourceId));
        }
        replayStorage = std::make_shared<ReplayableSourceStorage>(originId, storageDir, physicalSourceId);
        if (CheckpointManager::shouldRecoverFromCheckpoint())
        {
            if (const auto checkpointedByteOffset = replayStorage->loadRecoveryByteOffset())
            {
                sourceImplementation->setCheckpointRecoveryByteOffset(*checkpointedByteOffset);
            }
        }

        if (CheckpointManager::isCheckpointingEnabled() && !prepareCheckpointCallbackId && !commitCheckpointCallbackId)
        {
            prepareCheckpointCallbackId
                = fmt::format("checkpoint_source_replay_prepare_{}_{}", originId.getRawValue(), physicalSourceId.getRawValue());
            commitCheckpointCallbackId
                = fmt::format("source_replay_commit_{}_{}", originId.getRawValue(), physicalSourceId.getRawValue());
            CheckpointManager::registerCallback(
                *prepareCheckpointCallbackId,
                [checkpointProgress = checkpointProgress, preparedCheckpointSequence = preparedCheckpointSequence]
                {
                    preparedCheckpointSequence->store(
                        checkpointProgress ? checkpointProgress->getHighestContiguousCompletedSequence()
                                           : INVALID_SEQ_NUMBER.getRawValue(),
                        std::memory_order_release);
                },
                CheckpointManager::CallbackPhase::Prepare);
            CheckpointManager::registerCallback(
                *commitCheckpointCallbackId,
                [preparedCheckpointSequence = preparedCheckpointSequence, replayStorage = replayStorage]
                {
                    const auto preparedSequence = preparedCheckpointSequence->load(std::memory_order_acquire);
                    if (preparedSequence == INVALID_SEQ_NUMBER.getRawValue() || !replayStorage)
                    {
                        return;
                    }
                    replayStorage->markCheckpoint(SequenceNumber(preparedSequence));
                },
                CheckpointManager::CallbackPhase::Commit);
        }
    }

    Thread sourceThread(
        fmt::format("DataSrc-{}", originId),
        dataSourceThread,
        backpressureListener,
        std::move(terminationPromise),
        sourceImplementation.get(),
        std::move(emitFunction),
        originId,
        localBufferManager,
        replayStorage.get());
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

std::shared_ptr<SourceCheckpointProgress> SourceThread::getCheckpointProgress() const
{
    return checkpointProgress;
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
