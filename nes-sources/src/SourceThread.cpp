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
#include <functional>
#include <future>
#include <iostream>
#include <thread>
#include <utility>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceThread.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/ThreadNaming.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES::Sources
{

SourceThread::SourceThread(
    OriginId originId,
    SchemaPtr schema,
    std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
    SourceReturnType::EmitFunction&& emitFunction,
    size_t numSourceLocalBuffers,
    std::unique_ptr<Source> sourceImplementation)
    : originId(originId)
    , schema(schema)
    , localBufferManager(std::move(poolProvider))
    , emitFunction(std::move(emitFunction))
    , numSourceLocalBuffers(numSourceLocalBuffers)
    , sourceImplementation(std::move(sourceImplementation))
{
    NES_DEBUG("SourceThread  {} : Init Data Source with schema  {}", originId, schema->toString());
    NES_ASSERT(this->localBufferManager, "Invalid buffer manager");
}

void SourceThread::emitWork(Memory::TupleBuffer& buffer, bool addBufferMetaData)
{
    if (addBufferMetaData)
    {
        /// set the origin id for this source
        buffer.setOriginId(originId);
        /// set the creation timestamp
        buffer.setCreationTimestampInMS(WatermarkTs(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
        /// Set the sequence number of this buffer.
        /// A data source generates a monotonic increasing sequence number
        maxSequenceNumber++;
        buffer.setSequenceNumber(SequenceNumber(maxSequenceNumber));
        buffer.setChunkNumber(ChunkNumber(1));
        buffer.setLastChunk(true);
        NES_DEBUG(
            "Setting the buffer metadata for source {} with originId={} sequenceNumber={} chunkNumber={} lastChunk={}",
            buffer.getOriginId(),
            buffer.getOriginId(),
            buffer.getSequenceNumber(),
            buffer.getChunkNumber(),
            buffer.isLastChunk());
    }
    emitFunction(originId, SourceReturnType::Data{buffer});
}

bool SourceThread::start()
{
    NES_DEBUG("SourceThread  {} : start source", originId);
    std::promise<bool> prom;
    std::unique_lock lock(startStopMutex);
    {
        bool expected = false;

        /// Check if the SourceThread is already running.
        if (!running.compare_exchange_strong(expected, true))
        {
            NES_WARNING("SourceThread {}: is already running", originId);
            return false;
        }
        NES_DEBUG("SourceThread {}: Spawn thread", originId);
        expected = false;
        if (wasStarted.compare_exchange_strong(expected, true))
        {
            /// thread runs the runningRoutine indepenently (detach), until runningRoutine finishes.
            thread = std::make_unique<std::thread>(
                [this, &prom]()
                {
                    prom.set_value(true);
                    runningRoutine();
                    NES_DEBUG("SourceThread {}: runningRoutine is finished", originId);
                });
        }
    }
    return prom.get_future().get();
}

namespace detail
{
/// Called only in asserts of stop(). If deadline is reached, the assert fails, thus during normal execution the future is not reached.
bool waitForFuture(std::future<bool>&& future, const std::chrono::seconds deadline)
{
    auto terminationStatus = future.wait_for(deadline);
    return (terminationStatus == std::future_status::ready) ? future.get() : false;
}
}


bool SourceThread::stop()
{
    using namespace std::chrono_literals;

    NES_DEBUG("SourceThread {}: Stop called and source is {}", originId, (running ? "running" : "not running"));
    bool expected = true;

    bool isStopped = false;
    try
    {
        if (!running.compare_exchange_strong(expected, false))
        {
            NES_DEBUG("SourceThread {} was not running, retrieving future now...", originId);
            expected = false;
            if (wasStarted && futureRetrieved.compare_exchange_strong(expected, true))
            {
                NES_ASSERT2_FMT(
                    detail::waitForFuture(completedPromise.get_future(), STOP_TIMEOUT_NOT_RUNNING),
                    "Cannot complete future to stop source " << originId);
            }
            NES_DEBUG("SourceThread {} was not running, future retrieved", originId);
        }
        else
        {
            NES_DEBUG("SourceThread {} was running, retrieving future now...", originId);
            auto expected = false;
            NES_ASSERT2_FMT(
                wasStarted && futureRetrieved.compare_exchange_strong(expected, true)
                    && detail::waitForFuture(completedPromise.get_future(), STOP_TIMEOUT_RUNNING),
                "Cannot complete future to stop source " << originId);
            NES_WARNING("Stopped Source {}", originId);
        }
        /// it's ok to return true because the source is stopped
        isStopped = true;
    }
    catch (...)
    {
        auto expPtr = std::current_exception();
        try
        {
            if (expPtr)
            {
                std::rethrow_exception(expPtr);
            }
            isStopped = false;
        }
        catch (std::exception const& e) /// e needs to be passed by reference
        {
            /// stop() can be called from an outside class before the start() finished creating the SourceThread thread.
            /// if the SourceThread thread was not started, it cannot call notifySourceFailure, so we need to do it here instead.
            if (!wasStarted)
            {
                /// Todo #237: Improve error handling in sources
                auto ingestionException = StopBeforeStartFailure();
                ingestionException.what() += e.what();
                emitFunction(originId, SourceReturnType::Error{ingestionException});
            }
            isStopped = true;
        }
    }

    return isStopped;
}

void SourceThread::close()
{
    sourceImplementation->close();
    std::unique_lock lock(startStopMutex);
    {
        emitFunction(originId, SourceReturnType::Stopped{});
        bufferProvider->destroy();
    }
}

OriginId SourceThread::getOriginId() const
{
    return this->originId;
}

void SourceThread::runningRoutine()
{
    try
    {
        NES_ASSERT(this->originId != INVALID_ORIGIN_ID, "The id of the source is not set properly");
        std::string thName = fmt::format("DataSrc-{}", originId);
        setThreadName(thName.c_str());
        NES_DEBUG("SourceThread: {}", fmt::streamed(*this));

        /// open
        bufferProvider = localBufferManager->createFixedSizeBufferPool(numSourceLocalBuffers);
        sourceImplementation->open();

        uint64_t numberOfBuffersProduced = 0;
        while (running)
        {
            auto tupleBuffer = bufferProvider->getBufferBlocking();
            /// filling the TupleBuffer might block.
            /// passing schema by value to create a new TestTupleBuffer in the Parser, will be improved in Todo: #72.
            auto isReceivedData = sourceImplementation->fillTupleBuffer(tupleBuffer, *bufferProvider, schema);
            NES_DEBUG("receivedData: {}, tupleBuffer.getNumberOfTuples: {}", isReceivedData, tupleBuffer.getNumberOfTuples());

            ///this checks we received a valid output buffer
            if (isReceivedData)
            {
                NES_TRACE(
                    "SourceThread produced buffer {}, Num filled tuples: {}, SourceThread: {}",
                    numberOfBuffersProduced,
                    tupleBuffer.getNumberOfTuples(),
                    fmt::streamed(*this));

                emitWork(tupleBuffer);
                ++numberOfBuffersProduced;
            }
            else
            {
                NES_DEBUG("SourceThread {}: stopping cause of invalid buffer", originId);
                running = false;
                NES_DEBUG("SourceThread {}: Thread going to terminating with graceful exit.", originId);
            }
            if (!running)
            { /// necessary if source stops while receiveData is called due to stricter shutdown logic
                NES_DEBUG("Source is not running anymore.")
                break;
            }
            NES_TRACE("SourceThread {} : Data Source finished processing iteration {}", originId, numberOfBuffersProduced);
        }
        NES_DEBUG("SourceThread {} call close", originId);
        close();
        completedPromise.set_value(true);
        NES_DEBUG("SourceThread {} end running", originId);
    }
    catch (std::exception const& e)
    {
        /// Todo #237: Improve error handling in sources
        auto ingestionException = RunningRoutineFailure();
        ingestionException.what() += e.what();
        emitFunction(originId, SourceReturnType::Error{ingestionException});
        completedPromise.set_exception(std::make_exception_ptr(ingestionException));
    }
    catch (...)
    {
        try
        {
            auto expPtr = std::current_exception();
            if (expPtr)
            {
                completedPromise.set_exception(expPtr);
                std::rethrow_exception(expPtr);
            }
        }
        catch (std::exception const& e)
        {
            /// Todo #237: Improve error handling in sources
            auto ingestionException = RunningRoutineFailure();
            ingestionException.what() += e.what();
            emitFunction(originId, SourceReturnType::Error{ingestionException});
        }
    }
    NES_DEBUG("SourceThread {} end runningRoutine", originId);
}

std::ostream& operator<<(std::ostream& out, const SourceThread& sourceThread)
{
    out << "\nSourceThread(";
    out << "\n  originId: " << sourceThread.originId;
    out << "\n  numSourceLocalBuffers: " << sourceThread.numSourceLocalBuffers;
    out << "\n  running: " << sourceThread.running;
    out << "\n  wasStarted: " << sourceThread.wasStarted;
    out << "\n  futureRetrieved: " << sourceThread.futureRetrieved;
    out << "\n  maxSequenceNumber: " << sourceThread.maxSequenceNumber;
    out << "\n  source implementation:" << *sourceThread.sourceImplementation;
    out << ")\n";
    return out;
}

}
