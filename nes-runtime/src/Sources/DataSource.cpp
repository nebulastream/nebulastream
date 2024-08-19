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
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/ThreadNaming.hpp>

#include <utility>
namespace NES
{

DataSource::DataSource(
    OriginId originId,
    SchemaPtr schema,
    std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
    Runtime::QueryManagerPtr queryManager,
    size_t numSourceLocalBuffers,
    std::unique_ptr<Source> sourceImplementation,
    uint64_t numberOfBuffersToProduce,
    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& executableSuccessors,
    uint64_t taskQueueId)
    : Runtime::Reconfigurable()
    , originId(originId)
    , schema(schema)
    , queryManager(std::move(queryManager))
    , bufferPoolProvider(std::move(poolProvider))
    , executableSuccessors(std::move(executableSuccessors))
    , numberOfBuffersToProduce(numberOfBuffersToProduce)
    , numSourceLocalBuffers(numSourceLocalBuffers)
    , taskQueueId(taskQueueId)
    , sourceImplementation(std::move(sourceImplementation))
{
    NES_DEBUG("DataSource  {} : Init Data Source with schema  {}", originId, schema->toString());
    NES_ASSERT(this->bufferPoolProvider, "Invalid buffer manager");
    NES_ASSERT(this->queryManager, "Invalid query manager");
    /// ReSharper disable once CppDFAUnreachableCode (the code below is reachable) | triple slashes disable ReSharper comment
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT)
    {
        memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, localBufferManager->getBufferSize());
    }
    else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
    {
        memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, localBufferManager->getBufferSize());
    }
}

DataSource::~DataSource() NES_NOEXCEPT(false)
{
    NES_ASSERT(running == false, "Data source destroyed but thread still running... stop() was not called");
    NES_DEBUG("DataSource {}: Destroy Data Source.", originId);
    executableSuccessors.clear();
}

void DataSource::emitWork(Memory::TupleBuffer& buffer, bool addBufferMetaData)
{
    if (addBufferMetaData)
    {
        /// set the origin id for this source
        buffer.setOriginId(originId);
        /// set the creation timestamp
        buffer.setCreationTimestampInMS(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count());
        /// Set the sequence number of this buffer.
        /// A data source generates a monotonic increasing sequence number
        maxSequenceNumber++;
        buffer.setSequenceNumber(maxSequenceNumber);
        buffer.setChunkNumber(1);
        buffer.setLastChunk(true);
        NES_DEBUG(
            "Setting the buffer metadata for source {} with originId={} sequenceNumber={} chunkNumber={} lastChunk={}",
            buffer.getOriginId(),
            buffer.getOriginId(),
            buffer.getSequenceNumber(),
            buffer.getChunkNumber(),
            buffer.isLastChunk());
    }

    uint64_t queueId = 0;
    for (const auto& successor : executableSuccessors)
    {
        queryManager->addWorkForNextPipeline(buffer, successor, taskQueueId);
    }
}

bool DataSource::start()
{
    NES_DEBUG("DataSource  {} : start source", originId);
    std::promise<bool> prom;
    std::unique_lock lock(startStopMutex);
    {
        bool expected = false;

        /// Check if the DataSource is already running.
        if (!running.compare_exchange_strong(expected, true))
        {
            NES_WARNING("DataSource {}: is already running", originId);
            return false;
        }
        std::unique_ptr<std::thread> thread{nullptr};
        type = sourceImplementation->getType();
        NES_DEBUG("DataSource {}: Spawn thread", originId);
        expected = false;
        /// Start the main thread and detach it if successfull.
        if (wasStarted.compare_exchange_strong(expected, true))
        {
            /// thread runs the runningRoutine indepenently (detach), until runningRoutine finishes.
            thread = std::make_unique<std::thread>(
                [this, &prom]()
                {
                    prom.set_value(true);
                    runningRoutine();
                    NES_DEBUG("DataSource {}: runningRoutine is finished", originId);
                });
        }
        if (thread)
        {
            thread->detach();
        }
    }
    return prom.get_future().get();
}

namespace detail
{
/// Called only in asserts of stop(). If deadline is reached, the assert fails, thus during normal execution the future is not reached.
template <typename R, typename P>
bool waitForFuture(std::future<bool>&& future, std::chrono::duration<R, P>&& deadline)
{
    auto terminationStatus = future.wait_for(deadline);
    return (terminationStatus == std::future_status::ready) ? future.get() : false;
}
} /// namespace detail

bool DataSource::stop(Runtime::QueryTerminationType graceful)
{
    using namespace std::chrono_literals;
    /// Do not call stop from the runningRoutine! <-- is fine, because we want to call stop from the outside, but why does it matter?
    {
        std::unique_lock lock(startStopMutex); /// this mutex guards the thread variable
        terminationType = graceful;
    }

    NES_DEBUG("DataSource {}: Stop called and source is {}", originId, (running ? "running" : "not running"));
    bool expected = true;

    /// TODO add wakeUp call if source is blocking on something, e.g., tcp socket
    /// TODO in general this highlights how our source model has some issues
    ///
    bool isStopped;
    try
    {
        if (!running.compare_exchange_strong(expected, false))
        {
            NES_DEBUG("DataSource {} was not running, retrieving future now...", originId);
            expected = false;
            if (wasStarted && futureRetrieved.compare_exchange_strong(expected, true))
            {
                /// Todo: random magic timeout after 60s
                NES_ASSERT2_FMT(
                    detail::waitForFuture(completedPromise.get_future(), 60s), "Cannot complete future to stop source " << originId);
            }
            NES_DEBUG("DataSource {} was not running, future retrieved", originId);
        }
        else
        {
            NES_DEBUG("DataSource {} was running, retrieving future now...", originId);
            auto expected = false;
            /// Todo: random magic timeout after 10min
            NES_ASSERT2_FMT(
                wasStarted && futureRetrieved.compare_exchange_strong(expected, true)
                    && detail::waitForFuture(completedPromise.get_future(), 10min),
                "Cannot complete future to stop source " << originId);
            NES_WARNING("Stopped Source {} = {}", originId, terminationType);
        }
        /// it's ok to return true because the source is stopped
        isStopped = true;
    }
    catch (...)
    {
        auto expPtr = std::current_exception();
        terminationType = Runtime::QueryTerminationType::Failure;
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
            /// stop() can be called from an outside class before the start() finished creating the DataSource thread.
            /// if the DataSource thread was not started, it cannot call notifySourceFailure, so we need to do it here instead.
            if (!wasStarted)
            {
                queryManager->notifySourceFailure(originId, std::string(e.what()));
            }
            isStopped = true;
        }
    }

    return false;
}

void DataSource::close()
{
    sourceImplementation->close();
    Runtime::QueryTerminationType queryTerminationType;
    {
        std::unique_lock lock(startStopMutex);
        queryTerminationType = this->terminationType;
    }
    if (queryTerminationType != Runtime::QueryTerminationType::Graceful
        || queryManager->canTriggerEndOfStream(originId, queryTerminationType))
    {
        /// inject reconfiguration task containing end of stream
        std::unique_lock lock(startStopMutex);
        NES_ASSERT2_FMT(!endOfStreamSent, "Eos was already sent for source " << this);
        NES_DEBUG("DataSource {} : Data Source add end of stream. Gracefully={}", originId, queryTerminationType);
        endOfStreamSent = queryManager->addEndOfStream(originId, executableSuccessors, queryTerminationType);
        NES_ASSERT2_FMT(endOfStreamSent, "Cannot send eos for source " << this);
        bufferProvider->destroy();
        queryManager->notifySourceCompletion(originId, queryTerminationType);
    }
}

OriginId DataSource::getOriginId() const
{
    return this->originId;
}

std::string DataSource::toString() const
{
    return sourceImplementation->toString();
}

void DataSource::runningRoutine()
{
    try
    {
        NES_ASSERT(this->originId != INVALID_ORIGIN_ID, "The id of the source is not set properly");
        std::string thName = fmt::format("DataSrc-{}", originId);
        setThreadName(thName.c_str());

        NES_DEBUG("DataSource {}: Running Data Source of type={}", originId, magic_enum::enum_name(sourceImplementation->getType()));
        if (numberOfBuffersToProduce == 0)
        {
            NES_DEBUG("DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                      "the source is empty");
        }
        else
        {
            NES_DEBUG("DataSource: the user specify to produce {} buffers", numberOfBuffersToProduce);
        }
        /// open
        bufferManager = localBufferManager->createFixedSizeBufferPool(numSourceLocalBuffers);
        sourceImplementation->open();

        uint64_t numberOfBuffersProduced = 0;
        while (running)
        {
            ///check if already produced enough buffer
            if (numberOfBuffersToProduce == 0 || numberOfBuffersProduced < numberOfBuffersToProduce)
            {
                auto tupleBuffer = allocateBuffer();
                auto isReceivedData = sourceImplementation->fillTupleBuffer(tupleBuffer); /// note that receiveData might block
                NES_DEBUG("receivedData: {}, tupleBuffer.getNumberOfTuplez: {}", isReceivedData, tupleBuffer.getNumberOfTuples());

                ///this checks we received a valid output buffer
                if (isReceivedData)
                {
                    std::stringstream sourceStringStream;
                    sourceStringStream << this;
                    NES_TRACE(
                        "DataSource produced buffer {} type= {} string={}: Received Data: {} "
                        "originId={}",
                        numberOfBuffersProduced,
                        magic_enum::enum_name(sourceImplementation->getType()),
                        sourceStringStream.str(),
                        tupleBuffer.getNumberOfTuples(),
                        this->originId);

                    if (Logger::getInstance()->getCurrentLogLevel() == LogLevel::LOG_TRACE)
                    {
                        auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, tupleBuffer.getBuffer().getBufferSize());
                        NES_TRACE("DataSource produced buffer content={}", tupleBuffer.toString(schema));
                    }
                    auto buffer = tupleBuffer.getBuffer();
                    emitWork(buffer);
                    ++numberOfBuffersProduced;
                }
                else
                {
                    NES_DEBUG("DataSource {}: stopping cause of invalid buffer", originId);
                    running = false;
                    NES_DEBUG("DataSource {}: Thread going to terminating with graceful exit.", originId);
                }
                if (!running)
                { /// necessary if source stops while receiveData is called due to stricter shutdown logic
                    NES_DEBUG("Source is not running anymore.")
                    break;
                }
            }
            else
            {
                NES_DEBUG(
                    "DataSource {}: Receiving thread terminated ... stopping because cnt={} smaller than "
                    "numBuffersToProcess={} now return",
                    originId,
                    numberOfBuffersProduced,
                    numberOfBuffersToProduce);
                running = false;
            }
            NES_TRACE("DataSource {} : Data Source finished processing iteration {}", originId, numberOfBuffersProduced);
        }
        NES_DEBUG("DataSource {} call close", originId);
        close();
        NES_DEBUG("DataSource {} end running", originId);
    }
    catch (std::exception const& exception)
    {
        queryManager->notifySourceFailure(originId, exception.what());
        completedPromise.set_exception(std::make_exception_ptr(exception));
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
        catch (std::exception const& exception)
        {
            queryManager->notifySourceFailure(originId, exception.what());
        }
    }
    NES_DEBUG("DataSource {} end runningRoutine", originId);
}

const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& DataSource::getExecutableSuccessors()
{
    return executableSuccessors;
}

Runtime::MemoryLayouts::TestTupleBuffer DataSource::allocateBuffer()
{
    auto buffer = bufferProvider->getBufferBlocking();
    return Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
}

} /// namespace NES