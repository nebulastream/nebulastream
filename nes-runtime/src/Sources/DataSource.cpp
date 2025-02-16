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
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/ThreadNaming.hpp>
#include <chrono>
#include <functional>
#include <future>
#include <iostream>
#include <thread>

#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
#if defined(__linux__)
#include <numa.h>
#include <numaif.h>
#endif
#endif
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateAndDrainQueryMetadata.hpp>
#include <utility>
using namespace std::string_literals;
namespace NES {

DataSource::DataSource(SchemaPtr pSchema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       OperatorId operatorId,
                       OriginId originId,
                       StatisticId statisticId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       const std::string& physicalSourceName,
                       bool persistentSource,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors,
                       uint64_t sourceAffinity,
                       uint64_t taskQueueId)
    : Runtime::Reconfigurable(), DataEmitter(), persistentSource(persistentSource),
      persistentSourceKey(operatorId.toString() + "-" + physicalSourceName), queryManager(std::move(queryManager)),
      localBufferManager(std::move(bufferManager)), executableSuccessors(std::move(executableSuccessors)), operatorId(operatorId),
      originId(originId), statisticId(statisticId), schema(std::move(pSchema)), numSourceLocalBuffers(numSourceLocalBuffers),
      gatheringMode(gatheringMode), sourceAffinity(sourceAffinity), taskQueueId(taskQueueId),
      physicalSourceName(physicalSourceName) {

    NES_DEBUG("DataSource  {} : Init Data Source with schema  {}", operatorId, schema->toString());
    NES_ASSERT(this->localBufferManager, "Invalid buffer manager");
    NES_ASSERT(this->queryManager, "Invalid query manager");
    // TODO #4094: enable this exception -- currently many UTs are designed to assume empty executableSuccessors
    //    if (this->executableSuccessors.empty()) {
    //        throw Exceptions::RuntimeException("empty executable successors");
    //    }
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, localBufferManager->getBufferSize());
    } else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, localBufferManager->getBufferSize());
    }
}

void DataSource::emitWork(Runtime::TupleBuffer& buffer, bool addBufferMetaData) {
    if (addBufferMetaData) {
        // set the origin id for this source
        buffer.setOriginId(originId);
        // set the creation timestamp
        buffer.setCreationTimestampInMS(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count());
        // Set the sequence number of this buffer.
        // A data source generates a monotonic increasing sequence number

        if (!getReplayData()) {
            maxSequenceNumber++;
            buffer.setSequenceNumber(maxSequenceNumber);
        }
        buffer.setChunkNumber(1);
        buffer.setLastChunk(true);
        buffer.setStatisticId(statisticId);
        NES_TRACE("Setting the buffer metadata for source {} with originId={} sequenceNumber={} chunkNumber={} lastChunk={} "
                  "statisticId={}",
                  buffer.getOriginId(),
                  buffer.getOriginId(),
                  buffer.getSequenceNumber(),
                  buffer.getChunkNumber(),
                  buffer.isLastChunk(),
                  buffer.getStatisticId());
    }

    uint64_t queueId = 0;
    for (const auto& successor : *executableSuccessors.wlock()) {
        //find the queue to which this sources pushes
        if (!sourceSharing) {
            queryManager->addWorkForNextPipeline(buffer, successor, taskQueueId);
        } else {
            NES_DEBUG("push task for queueid= {} successor= ", queueId /*, &successor*/);
            queryManager->addWorkForNextPipeline(buffer, successor, queueId);
        }
    }
}

std::vector<Runtime::Execution::SuccessorExecutablePipeline> DataSource::getExecutableSuccessors() {
    return executableSuccessors.copy();
}

void DataSource::addExecutableSuccessors(std::vector<Runtime::Execution::SuccessorExecutablePipeline> newPipelines) {
    for (auto& pipe : newPipelines) {
        executableSuccessors->push_back(pipe);
    }
}

OperatorId DataSource::getOperatorId() const { return operatorId; }

void DataSource::setOperatorId(OperatorId operatorId) { this->operatorId = operatorId; }

SchemaPtr DataSource::getSchema() const { return schema; }

DataSource::~DataSource() NES_NOEXCEPT(false) {
    NES_ASSERT(running == false, "Data source destroyed but thread still running... stop() was not called");
    NES_DEBUG("DataSource {}: Destroy Data Source.", operatorId);
    executableSuccessors->clear();
}

bool DataSource::start() {
    NES_DEBUG("DataSource  {} : start source", operatorId);
    std::promise<bool> prom;
    std::unique_lock lock(startStopMutex);
    bool expected = false;
    std::shared_ptr<std::thread> thread{nullptr};
    if (!running.compare_exchange_strong(expected, true)) {
        NES_WARNING("DataSource {}: is already running", operatorId);
        return false;
    } else {
        type = getType();
        NES_DEBUG("DataSource {}: Spawn thread", operatorId);
        auto expected = false;
        if (wasStarted.compare_exchange_strong(expected, true)) {
            thread = std::make_shared<std::thread>([this, &prom]() {
            // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
            // only CPU i as set.
#ifdef __linux__
                if (sourceAffinity != std::numeric_limits<uint64_t>::max()) {
                    NES_ASSERT(sourceAffinity < std::thread::hardware_concurrency(),
                               "pinning position is out of cpu range maxPosition=" << sourceAffinity);
                    cpu_set_t cpuset;
                    CPU_ZERO(&cpuset);
                    CPU_SET(sourceAffinity, &cpuset);
                    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
                    if (rc != 0) {
                        NES_THROW_RUNTIME_ERROR("Cannot set thread affinity on source thread " << operatorId);
                    }
                } else {
                    NES_WARNING("Use default affinity for source");
                }
#endif
                prom.set_value(true);
                runningRoutine();
                NES_DEBUG("DataSource {}: runningRoutine is finished", operatorId);
            });
        }
    }
    if (thread) {
        thread->detach();
    }
    return prom.get_future().get();
}

bool DataSource::fail() {
    bool isStopped = stop(Runtime::QueryTerminationType::Failure);// this will block until the thread is stopped
    NES_DEBUG("Source {} stop executed= {}", operatorId, (isStopped ? "stopped" : "cannot stop"));
    {
        // it may happen that the source failed prior of sending its eos
        std::unique_lock lock(startStopMutex);// do not call stop if holding this mutex
        auto self = shared_from_base<DataSource>();
        NES_DEBUG("Source {} has already injected failure? {}", operatorId, (endOfStreamSent ? "EoS sent" : "cannot send EoS"));
        if (!this->endOfStreamSent) {
            endOfStreamSent = queryManager->addEndOfStream(self, Runtime::QueryTerminationType::Failure);
            queryManager->notifySourceCompletion(self, Runtime::QueryTerminationType::Failure);
            NES_DEBUG("Source {} injecting failure  {}", operatorId, (endOfStreamSent ? "EoS sent" : "cannot send EoS"));
        }
        return isStopped && endOfStreamSent;
    }
    return false;
}

namespace detail {
template<typename R, typename P>
bool waitForFuture(std::future<bool>&& future, std::chrono::duration<R, P>&& deadline) {
    auto terminationStatus = future.wait_for(deadline);
    switch (terminationStatus) {
        case std::future_status::ready: {
            return future.get();
        }
        default: {
            return false;
        }
    }
}
}// namespace detail

bool DataSource::handleReconfigurationMarker(ReconfigurationMarkerPtr marker) {
    using namespace std::chrono_literals;

    NES_DEBUG("DataSource {}: Propagate reconfiguration marker called and source is {}",
              operatorId,
              (running ? "running" : "not running"));

    auto sourcePtr = shared_from_base<DataSource>();

    auto executablePlans = queryManager->getExecutablePlanIdsForSource(sourcePtr);
    if (executablePlans.size() > 1) {
        NES_ERROR("Source sharing is currently not compatible with query reconfiguration");
        NES_NOT_IMPLEMENTED();
    }

    NES_ASSERT(!executablePlans.empty(), "No executable plans found for source");
    auto decomposedQueryIdWithVersion = *executablePlans.begin();
    if (auto eventOptional = marker->getReconfigurationEvent(decomposedQueryIdWithVersion)) {
        auto event = eventOptional.value();
        auto metadata = event->reconfigurationMetadata;
        if (metadata->instanceOf<DrainQueryMetadata>() || metadata->instanceOf<UpdateAndDrainQueryMetadata>()) {
            //if this is a drain event, set the marker as a member var and proceed to set running to false
            //the source will then propagate the reconfiguration when close is called
            try {
                {
                    std::unique_lock lock(startStopMutex);// this mutex guards the thread variable
                    wasGracefullyStopped = Runtime::QueryTerminationType::Reconfiguration;
                    reconfigurationMarker = std::move(marker);
                }
                bool expected = true;
                if (!running.compare_exchange_strong(expected, false)) {
                    NES_DEBUG("DataSource {} was not running, retrieving future now...", operatorId);
                    auto expected = false;
                    if (wasStarted && futureRetrieved.compare_exchange_strong(expected, true)) {
                        NES_ASSERT2_FMT(detail::waitForFuture(completedPromise.get_future(), 60s),
                                        "Cannot complete future to stop source " << operatorId);
                    }
                    NES_DEBUG("DataSource {} was not running, future retrieved", operatorId);
                    return true;// it's ok to return true because the source is stopped
                } else {
                    NES_DEBUG("DataSource {} is running, retrieving future now...", operatorId);
                    auto expected = false;
                    NES_ASSERT2_FMT(wasStarted && futureRetrieved.compare_exchange_strong(expected, true)
                                        && detail::waitForFuture(completedPromise.get_future(), 10min),
                                    "Cannot complete future to stop source " << operatorId);
                    NES_WARNING("Stopped Source {} = {}", operatorId, wasGracefullyStopped);
                    return true;
                }
            } catch (...) {
                auto expPtr = std::current_exception();
                wasGracefullyStopped = Runtime::QueryTerminationType::Failure;
                try {
                    if (expPtr) {
                        std::rethrow_exception(expPtr);
                    }
                } catch (std::exception const& e) {// it would not work if you pass by value
                    // I leave the following lines just as a reminder:
                    // here we do not need to call notifySourceFailure because it is done from the main thread
                    // the only reason to call notifySourceFailure is when the main thread was not stated
                    if (!wasStarted) {
                        queryManager->notifySourceFailure(shared_from_base<DataSource>(), std::string(e.what()));
                    }
                    return true;
                }
            }
        } else {
            NES_ERROR("Currently only drain reconfigurations are supported");
            NES_NOT_IMPLEMENTED();
        }
    }
    return false;
}

bool DataSource::stop(Runtime::QueryTerminationType graceful) {
    using namespace std::chrono_literals;
    // Do not call stop from the runningRoutine!
    {
        std::unique_lock lock(startStopMutex);// this mutex guards the thread variable
        wasGracefullyStopped = graceful;
    }

    refCounter++;
    if (refCounter != numberOfConsumerQueries) {
        return true;
    }

    NES_ERROR("DataSource {}: Stop called and source is {}", operatorId, (running ? "running" : "not running"));
    bool expected = true;

    // TODO add wakeUp call if source is blocking on something, e.g., tcp socket
    // TODO in general this highlights how our source model has some issues

    // TODO this is also an issue in the current development of the StaticDataSource. If it is still running here, we give up to early and never join the thread.

    try {
        if (!running.compare_exchange_strong(expected, false)) {
            NES_DEBUG("DataSource {} was not running, retrieving future now...", operatorId);
            auto expected = false;
            if (wasStarted && futureRetrieved.compare_exchange_strong(expected, true)) {
                NES_ASSERT2_FMT(detail::waitForFuture(completedPromise.get_future(), 60s),
                                "Cannot complete future to stop source " << operatorId);
            }
            NES_DEBUG("DataSource {} was not running, future retrieved", operatorId);
            return true;// it's ok to return true because the source is stopped
        } else {
            NES_DEBUG("DataSource {} is running, retrieving future now...", operatorId);
            auto expected = false;
            NES_ASSERT2_FMT(wasStarted && futureRetrieved.compare_exchange_strong(expected, true)
                                && detail::waitForFuture(completedPromise.get_future(), 10min),
                            "Cannot complete future to stop source " << operatorId);
            NES_WARNING("Stopped Source {} = {}", operatorId, wasGracefullyStopped);
            return true;
        }
    } catch (...) {
        auto expPtr = std::current_exception();
        wasGracefullyStopped = Runtime::QueryTerminationType::Failure;
        try {
            if (expPtr) {
                std::rethrow_exception(expPtr);
            }
        } catch (std::exception const& e) {// it would not work if you pass by value
            // I leave the following lines just as a reminder:
            // here we do not need to call notifySourceFailure because it is done from the main thread
            // the only reason to call notifySourceFailure is when the main thread was not stated
            if (!wasStarted) {
                queryManager->notifySourceFailure(shared_from_base<DataSource>(), std::string(e.what()));
            }
            return true;
        }
    }

    return false;
}

void DataSource::setGatheringInterval(std::chrono::milliseconds interval) { this->gatheringInterval = interval; }

void DataSource::open() {
    bufferManager = localBufferManager->createFixedSizeBufferPool(numSourceLocalBuffers);
    createOrLoadPersistedProperties();
    storePersistedProperties();
}

void DataSource::close() {
    NES_WARNING("Close Called")
    Runtime::QueryTerminationType queryTerminationType;
    std::optional<ReconfigurationMarkerPtr> marker;
    {
        std::unique_lock lock(startStopMutex);
        queryTerminationType = this->wasGracefullyStopped;
        marker = reconfigurationMarker;
    }

    if (persistentSource) {
        clearPersistedProperties();
    }

    if (queryTerminationType != Runtime::QueryTerminationType::Graceful
        || queryManager->canTriggerEndOfStream(shared_from_base<DataSource>(), queryTerminationType)) {
        // inject reconfiguration task containing end of stream
        std::unique_lock lock(startStopMutex);
        NES_ASSERT2_FMT(!endOfStreamSent, "Eos was already sent for source " << toString());
        NES_WARNING("DataSource {} : Data Source add end of stream of type ={}",
                    operatorId,
                    magic_enum::enum_name(queryTerminationType));
        if (queryTerminationType == Runtime::QueryTerminationType::Reconfiguration) {
            NES_ASSERT2_FMT(marker, "Shutting down source with type Reconfiguration but no marker is available");
            endOfStreamSent = queryManager->propagateReconfigurationMarker(marker.value(), shared_from_base<DataSource>());
        } else {
            endOfStreamSent = queryManager->addEndOfStream(shared_from_base<DataSource>(), queryTerminationType);
        }
        NES_ASSERT2_FMT(endOfStreamSent, "Cannot send eos for source " << toString());
        bufferManager->destroy();
        queryManager->notifySourceCompletion(shared_from_base<DataSource>(), queryTerminationType);
    }
}

void DataSource::runningRoutine() {
    try {
        if (gatheringMode == GatheringMode::INTERVAL_MODE) {
            runningRoutineWithGatheringInterval();
        } else if (gatheringMode == GatheringMode::INGESTION_RATE_MODE) {
            runningRoutineWithIngestionRate();
        } else if (gatheringMode == GatheringMode::ADAPTIVE_MODE) {
            NES_FATAL_ERROR("ADAPTIVE GATHERING IS CURRENTLY NOT SUPPORTED!");
            NES_NOT_IMPLEMENTED();
        }
        completedPromise.set_value(true);
    } catch (std::exception const& exception) {
        queryManager->notifySourceFailure(shared_from_base<DataSource>(), exception.what());
        completedPromise.set_exception(std::make_exception_ptr(exception));
    } catch (...) {
        try {
            auto expPtr = std::current_exception();
            if (expPtr) {
                completedPromise.set_exception(expPtr);
                std::rethrow_exception(expPtr);
            }
        } catch (std::exception const& exception) {
            queryManager->notifySourceFailure(shared_from_base<DataSource>(), exception.what());
        }
    }
    NES_DEBUG("DataSource {} end runningRoutine", operatorId);
}

void DataSource::runningRoutineWithIngestionRate() {
    NES_ASSERT(this->operatorId != INVALID_OPERATOR_ID, "The id of the source is not set properly");
    NES_ASSERT(gatheringIngestionRate >= 10, "As we generate on 100 ms base we need at least an ingestion rate of 10");
    std::string thName = fmt::format("DataSrc-{}", operatorId);
    setThreadName(thName.c_str());

    NES_DEBUG("DataSource {} Running Data Source of type={} ingestion rate={}",
              operatorId,
              magic_enum::enum_name(getType()),
              gatheringIngestionRate);
    if (numberOfBuffersToProduce == 0) {
        NES_DEBUG(
            "DataSource: the user does not specify the number of buffers to produce therefore we will produce buffers until "
            "the source is empty");
    } else {
        NES_DEBUG("DataSource: the user specify to produce {} buffers", numberOfBuffersToProduce);
    }
    open();

    uint64_t nextPeriodStartTime = 0;
    uint64_t curPeriod = 0;
    uint64_t processedOverallBufferCnt = 0;
    uint64_t buffersToProducePer100Ms = gatheringIngestionRate / 10;
    while (running && (processedOverallBufferCnt < numberOfBuffersToProduce || numberOfBuffersToProduce == 0)) {
        //create as many tuples as requested and then sleep
        auto startPeriod =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        uint64_t buffersProcessedCnt = 0;

        //produce buffers until limit for this second or for all periods is reached or source is topped
        while (buffersProcessedCnt < buffersToProducePer100Ms && running
               && (processedOverallBufferCnt < numberOfBuffersToProduce || numberOfBuffersToProduce == 0)) {
            auto optBuf = receiveData();

            if (optBuf.has_value()) {
                // here we got a valid buffer
                NES_TRACE("DataSource: add task for buffer");
                auto& buf = optBuf.value();
                emitWork(buf);
                buffersProcessedCnt++;
                processedOverallBufferCnt++;
                if (!running) {// necessary if source stops while receiveData is called due to stricter shutdown logic
                    break;
                }
            } else {
                NES_ERROR("DataSource: Buffer is invalid");
                running = false;
            }
            NES_TRACE("DataSource: buffersProcessedCnt={} buffersPerSecond={}", buffersProcessedCnt, gatheringIngestionRate);
        }

        uint64_t endPeriod =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        //next point in time when to start producing again
        nextPeriodStartTime = uint64_t(startPeriod + (100));
        NES_ERROR("DataSource: startTimeSendBuffers={} endTimeSendBuffers={} nextPeriodStartTime={}",
                  startPeriod,
                  endPeriod,
                  nextPeriodStartTime);

        //If this happens then the second was not enough to create so many tuples and the ingestion rate should be decreased
        if (nextPeriodStartTime < endPeriod) {
            NES_ERROR(
                "Creating buffer(s) for DataSource took longer than periodLength. nextPeriodStartTime={} endTimeSendBuffers={}",
                nextPeriodStartTime,
                endPeriod);
        }

        uint64_t sleepCnt = 0;
        uint64_t curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        //wait until the next period starts
        while (curTime < nextPeriodStartTime) {
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
        }
        NES_DEBUG("DataSource: Done with period {} and overall buffers={} sleepCnt={} startPeriod={} endPeriod={} "
                  "nextPeriodStartTime={} curTime={}",
                  curPeriod++,
                  processedOverallBufferCnt,
                  sleepCnt,
                  startPeriod,
                  endPeriod,
                  nextPeriodStartTime,
                  curTime);
    }//end of while
    NES_DEBUG("DataSource {} call close", operatorId);
    close();
    NES_DEBUG("DataSource {} end running", operatorId);
}

void DataSource::runningRoutineWithGatheringInterval() {
    NES_ASSERT(this->operatorId != INVALID_OPERATOR_ID, "The id of the source is not set properly");
    std::string thName = fmt::format("DataSrc-{}", operatorId);
    setThreadName(thName.c_str());

    NES_DEBUG("DataSource {}: Running Data Source of type={} interval={}",
              operatorId,
              magic_enum::enum_name(getType()),
              gatheringInterval.count());
    if (numberOfBuffersToProduce == 0) {
        NES_DEBUG("DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                  "the source is empty");
    } else {
        NES_DEBUG("DataSource: the user specify to produce {} buffers", numberOfBuffersToProduce);
    }
    open();
    uint64_t numberOfBuffersProduced = 0;
    while (running) {
        //check if already produced enough buffer
        if (numberOfBuffersToProduce == 0 || numberOfBuffersProduced < numberOfBuffersToProduce) {
            auto optBuf = receiveData();// note that receiveData might block
            //this checks we received a valid output buffer
            if (optBuf.has_value()) {
                auto& buf = optBuf.value();
                NES_ERROR("DataSource produced buffer {} type= {} string={}: Received Data: {} "
                          "operatorId={} orgID={}",
                          numberOfBuffersProduced,
                          magic_enum::enum_name(getType()),
                          toString(),
                          buf.getNumberOfTuples(),
                          this->operatorId,
                          this->operatorId);

                if (Logger::getInstance()->getCurrentLogLevel() == LogLevel::LOG_TRACE) {
                    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
                    auto buffer = Runtime::MemoryLayouts::TestTupleBuffer(layout, buf);
                    NES_TRACE("DataSource produced buffer content={}", buffer.toString(schema));
                }
                emitWork(buf);
                ++numberOfBuffersProduced;
                if (!running) {// necessary if source stops while receiveData is called due to stricter shutdown logic
                    break;
                }
            } else {
                NES_DEBUG("DataSource {}: stopping cause of invalid buffer", operatorId);
                running = false;
                NES_DEBUG("DataSource {}: Thread going to terminating with graceful exit.", operatorId);
            }
        } else {
            NES_DEBUG("DataSource {}: Receiving thread terminated ... stopping because cnt={} smaller than "
                      "numBuffersToProcess={} now return",
                      operatorId,
                      numberOfBuffersProduced,
                      numberOfBuffersToProduce);
            running = false;
        }
        NES_TRACE("DataSource {} : Data Source finished processing iteration {}", operatorId, numberOfBuffersProduced);

        // this checks if the interval is zero or a ZMQ_Source, we don't create a watermark-only buffer
        if (getType() != SourceType::ZMQ_SOURCE && gatheringInterval.count() > 0) {
            std::this_thread::sleep_for(gatheringInterval);
        }
    }
    NES_WARNING("DataSource {} call close", operatorId);
    close();
    NES_WARNING("DataSource {} end running", operatorId);
}

// debugging
uint64_t DataSource::getNumberOfGeneratedTuples() const { return generatedTuples; };

uint64_t DataSource::getNumberOfGeneratedBuffers() const { return generatedBuffers; };

bool DataSource::performSoftStop() {
    bool expected = true;
    if (!running.compare_exchange_strong(expected, false)) {
        NES_WARNING("DataSource {}: was already not running", operatorId);
        return false;
    }
    return true;
}

std::string DataSource::getSourceSchemaAsString() { return schema->toString(); }

uint64_t DataSource::getNumBuffersToProcess() const { return numberOfBuffersToProduce; }

std::chrono::milliseconds DataSource::getGatheringInterval() const { return gatheringInterval; }
uint64_t DataSource::getGatheringIntervalCount() const { return gatheringInterval.count(); }
std::vector<Schema::MemoryLayoutType> DataSource::getSupportedLayouts() { return {Schema::MemoryLayoutType::ROW_LAYOUT}; }

Runtime::MemoryLayouts::TestTupleBuffer DataSource::allocateBuffer() {
    auto buffer = bufferManager->getBufferBlocking();
    return Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
}

void DataSource::onEvent(Runtime::BaseEvent& event) {
    NES_DEBUG("DataSource::onEvent(event) called. operatorId: {}", this->operatorId);
    // no behaviour needed, call onEvent of direct ancestor
    DataEmitter::onEvent(event);
}

void DataSource::onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef) {
    NES_DEBUG("DataSource::onEvent(event, wrkContext) called. operatorId:  {}", this->operatorId);
    onEvent(event);
}

void DataSource::createOrLoadPersistedProperties() {
    NES_WARNING("No implementation found for creating or loading persistent source properties.");
}

void DataSource::storePersistedProperties() { NES_WARNING("No implementation found to store persistent source properties."); }

void DataSource::clearPersistedProperties() { NES_WARNING("No implementation found to clear persistent source properties."); }

bool DataSource::updateSuccessors(const ReconfigurationMarkerPtr& marker,
                                  OperatorId sourceId,
                                  std::vector<Runtime::Execution::ExecutableQueryPlanPtr> newPlans) {
    /* maintain successors locked throughout the function, this also ensures that the source to qep mapping cannot be
    modified by the lockSuccessorsAndNotifySourceCompletion() function while this function executes */
    auto lockedSuccessors = executableSuccessors.wlock();
    NES_ASSERT(lockedSuccessors->size() == 1, "Source reuse is not supported if source sharing is active");

    //shutdown old plan
    queryManager->propagateReconfigurationMarkerToSuccessorList(marker, *lockedSuccessors);
    queryManager->notifySourceCompletion(shared_from_base<DataSource>(), Runtime::QueryTerminationType::Reconfiguration);

    // update the successors
    auto lockedNewSuccessors = newSuccessors.wlock();
    NES_ASSERT(!lockedNewSuccessors->empty(), "Trying to how swap source, but no successor was set");
    auto nextSuccessors = lockedNewSuccessors->front();
    *lockedSuccessors = nextSuccessors;
    lockedNewSuccessors->pop();
    queryManager->updateSourceToQepMapping(sourceId, newPlans);
    return true;
}

bool DataSource::compareId(const DataSourcePtr& first, const DataSourcePtr& second) {
    return first->getOperatorId() == second->getOperatorId();
}

void DataSource::scheduleSuccessors(std::vector<Runtime::Execution::SuccessorExecutablePipeline> newSuccessorList) {
    newSuccessors->push(newSuccessorList);
}

void DataSource::lockSuccessorsAndNotifySourceCompletion(Runtime::QueryTerminationType terminationType) {
    // lock the successors to make sure that they do not change while source completion is notified
    auto lock = executableSuccessors.rlock();

    queryManager->notifySourceCompletion(shared_from_base<DataSource>(), terminationType);
}

void DataSource::setReplayData() {
    NES_ERROR("setting source to replay data")
    replayData = true;
}

bool DataSource::getReplayData() { return replayData; }
}// namespace NES
