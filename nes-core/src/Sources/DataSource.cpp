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
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/ZmqSource.hpp>
#include <Util/KalmanFilter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <filesystem>
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
#include <utility>
using namespace std::string_literals;
namespace NES {

std::vector<Runtime::Execution::SuccessorExecutablePipeline> DataSource::getExecutableSuccessors() {
    return executableSuccessors;
}

void DataSource::addExecutableSuccessors(std::vector<Runtime::Execution::SuccessorExecutablePipeline> newPipelines) {
    successorModifyMutex.lock();
    for (auto& pipe : newPipelines) {
        executableSuccessors.push_back(pipe);
    }
}

DataSource::DataSource(SchemaPtr pSchema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       OperatorId operatorId,
                       OriginId originId,
                       size_t numSourceLocalBuffers,
                       GatheringMode::Value gatheringMode,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors,
                       uint64_t sourceAffinity,
                       uint64_t taskQueueId)
    : Runtime::Reconfigurable(), DataEmitter(), queryManager(std::move(queryManager)),
      localBufferManager(std::move(bufferManager)), executableSuccessors(std::move(executableSuccessors)), operatorId(operatorId),
      originId(originId), schema(std::move(pSchema)), numSourceLocalBuffers(numSourceLocalBuffers), gatheringMode(gatheringMode),
      sourceAffinity(sourceAffinity), taskQueueId(taskQueueId), kFilter(std::make_unique<KalmanFilter>()) {
    this->kFilter->setDefaultValues();
    NES_DEBUG("DataSource " << operatorId << ": Init Data Source with schema " << schema->toString());
    NES_ASSERT(this->localBufferManager, "Invalid buffer manager");
    NES_ASSERT(this->queryManager, "Invalid query manager");
    // TODO enable this exception -- currently many UTs are designed to assume empty executableSuccessors
    //    if (this->executableSuccessors.empty()) {
    //        throw Exceptions::RuntimeException("empty executable successors");
    //    }
    if (schema->getLayoutType() == Schema::ROW_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, localBufferManager->getBufferSize());
    } else if (schema->getLayoutType() == Schema::COLUMNAR_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, localBufferManager->getBufferSize());
    }
}

void DataSource::emitWorkFromSource(Runtime::TupleBuffer& buffer) {
    // set the origin id for this source
    buffer.setOriginId(originId);
    // set the creation timestamp
    //    buffer.setCreationTimestamp(
    //        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
    //            .count());
    // Set the sequence number of this buffer.
    // A data source generates a monotonic increasing sequence number
    maxSequenceNumber++;
    buffer.setSequenceNumber(maxSequenceNumber);
    emitWork(buffer);
}

void DataSource::emitWork(Runtime::TupleBuffer& buffer) {
    uint64_t queueId = 0;
    for (const auto& successor : executableSuccessors) {
        //find the queue to which this sources pushes
        if (!sourceSharing) {
            queryManager->addWorkForNextPipeline(buffer, successor, taskQueueId);
        } else {
            NES_DEBUG("push task for queueid=" << queueId << " successor=" << &successor);
            queryManager->addWorkForNextPipeline(buffer, successor, queueId);
        }
    }
}

OperatorId DataSource::getOperatorId() const { return operatorId; }

void DataSource::setOperatorId(OperatorId operatorId) { this->operatorId = operatorId; }

SchemaPtr DataSource::getSchema() const { return schema; }

DataSource::~DataSource() NES_NOEXCEPT(false) {
    NES_ASSERT(running == false, "Data source destroyed but thread still running... stop() was not called");
    NES_DEBUG("DataSource " << operatorId << ": Destroy Data Source.");
    executableSuccessors.clear();
}

bool DataSource::start() {
    NES_DEBUG("DataSource " << operatorId << ": start source " << this);
    std::promise<bool> prom;
    std::unique_lock lock(startStopMutex);
    bool expected = false;
    std::shared_ptr<std::thread> thread{nullptr};
    if (!running.compare_exchange_strong(expected, true)) {
        NES_WARNING("DataSource " << operatorId << ": is already running " << this);
        return false;
    } else {
        type = getType();
        NES_DEBUG("DataSource " << operatorId << ": Spawn thread");
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
                        NES_THROW_RUNTIME_ERROR("Cannot set thread affinity on source thread " + std::to_string(operatorId));
                    }
                } else {
                    NES_WARNING("Use default affinity for source");
                }
#endif
                prom.set_value(true);
                runningRoutine();
                NES_DEBUG("DataSource " << operatorId << ": runningRoutine is finished");
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
    NES_DEBUG("Source " << operatorId << " stop executed=" << (isStopped ? "stopped" : "cannot stop"));
    {
        // it may happen that the source failed prior of sending its eos
        std::unique_lock lock(startStopMutex);// do not call stop if holding this mutex
        auto self = shared_from_base<DataSource>();
        NES_DEBUG("Source " << operatorId << " has already injected failure? "
                            << (endOfStreamSent ? "EoS sent" : "cannot send EoS"));
        if (!this->endOfStreamSent) {
            endOfStreamSent = queryManager->addEndOfStream(self, Runtime::QueryTerminationType::Failure);
            queryManager->notifySourceCompletion(self, Runtime::QueryTerminationType::Failure);
            NES_DEBUG("Source " << operatorId << " injecting failure " << (endOfStreamSent ? "EoS sent" : "cannot send EoS"));
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

    NES_DEBUG("DataSource " << operatorId << ": Stop called and source is " << (running ? "running" : "not running"));
    bool expected = true;

    // TODO add wakeUp call if source is blocking on something, e.g., tcp socket
    // TODO in general this highlights how our source model has some issues

    // TODO this is also an issue in the current development of the StaticDataSource. If it is still running here, we give up to early and never join the thread.

    try {
        if (!running.compare_exchange_strong(expected, false)) {
            NES_DEBUG("DataSource " << operatorId << " was not running, retrieving future now...");
            auto expected = false;
            if (wasStarted && futureRetrieved.compare_exchange_strong(expected, true)) {
                NES_ASSERT2_FMT(detail::waitForFuture(completedPromise.get_future(), 60s),
                                "Cannot complete future to stop source " << operatorId);
            }
            NES_DEBUG("DataSource " << operatorId << " was not running, future retrieved");
            return true;// it's ok to return true because the source is stopped
        } else {
            NES_DEBUG("DataSource " << operatorId << " was running, retrieving future now...");
            auto expected = false;
            NES_ASSERT2_FMT(wasStarted && futureRetrieved.compare_exchange_strong(expected, true)
                                && detail::waitForFuture(completedPromise.get_future(), 10min),
                            "Cannot complete future to stop source " << operatorId);
            NES_WARNING("Stopped Source " << operatorId << " = " << wasGracefullyStopped);
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
            // i leave the following lines just as a reminder:
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

void DataSource::open() { bufferManager = localBufferManager->createFixedSizeBufferPool(numSourceLocalBuffers); }

void DataSource::close() {
    Runtime::QueryTerminationType queryTerminationType;
    {
        std::unique_lock lock(startStopMutex);
        queryTerminationType = this->wasGracefullyStopped;
    }
    if (queryTerminationType != Runtime::QueryTerminationType::Graceful
        || queryManager->canTriggerEndOfStream(shared_from_base<DataSource>(), queryTerminationType)) {
        // inject reconfiguration task containing end of stream
        std::unique_lock lock(startStopMutex);
        NES_ASSERT2_FMT(!endOfStreamSent, "Eos was already sent for source " << toString());
        NES_DEBUG("DataSource " << operatorId << ": Data Source add end of stream. Gracefully= " << queryTerminationType);
        endOfStreamSent = queryManager->addEndOfStream(shared_from_base<DataSource>(), queryTerminationType);
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
            runningRoutineAdaptiveGatheringInterval();
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
    NES_DEBUG("DataSource " << operatorId << " end runningRoutine");
}

void DataSource::runningRoutineWithIngestionRate() {
    NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");
//    NES_ASSERT(gatheringIngestionRate >= 10, "As we generate on 100 ms base we need at least an ingestion rate of 10");
    std::string thName = "DataSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    NES_DEBUG("DataSource " << operatorId << ": Running Data Source of type=" << getType()
                            << " ingestion rate=" << gatheringIngestionRate);
    if (numBuffersToProcess == 0) {
        NES_DEBUG(
            "DataSource: the user does not specify the number of buffers to produce therefore we will produce buffers until "
            "the source is empty");
    } else {
        NES_DEBUG("DataSource: the user specify to produce " << numBuffersToProcess << " buffers");
    }
    open();

    uint64_t nextPeriodStartTime = 0;
    uint64_t curPeriod = 0;
    uint64_t processedOverallBufferCnt = 0;
    uint64_t buffersToProducePer100Ms = gatheringIngestionRate;
    while (running) {
        //create as many tuples as requested and then sleep
        auto startPeriod =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        uint64_t buffersProcessedCnt = 0;

        //produce buffers until limit for this second or for all perionds is reached or source is topped
        while (buffersProcessedCnt < buffersToProducePer100Ms && running && processedOverallBufferCnt < numBuffersToProcess) {
            auto optBuf = receiveData();

            if (optBuf.has_value()) {
                // here we got a valid buffer
                NES_TRACE("DataSource: add task for buffer");
                auto& buf = optBuf.value();
                emitWorkFromSource(buf);

                buffersProcessedCnt++;
                processedOverallBufferCnt++;
            } else {
                NES_ERROR("DataSource: Buffer is invalid");
                running = false;
            }
            NES_TRACE("DataSource: buffersProcessedCnt=" << buffersProcessedCnt
                                                         << " buffersPerSecond=" << gatheringIngestionRate);
        }

        uint64_t endPeriod =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        //next point in time when to start producing again
        nextPeriodStartTime = uint64_t(startPeriod + (1));
        NES_TRACE("DataSource: startTimeSendBuffers=" << startPeriod << " endTimeSendBuffers=" << endPeriod
                                                      << " nextPeriodStartTime=" << nextPeriodStartTime);

        //If this happens then the second was not enough to create so many tuples and the ingestion rate should be decreased
        if (nextPeriodStartTime < endPeriod) {
            NES_ERROR("Creating buffer(s) for DataSource took longer than periodLength. nextPeriodStartTime="
                      << nextPeriodStartTime << " endTimeSendBuffers=" << endPeriod);
            //            std::cout << "Creating buffer(s) for DataSource took longer than periodLength. nextPeriodStartTime="
            //                      << nextPeriodStartTime << " endTimeSendBuffers=" << endPeriod << std::endl;
        }

        uint64_t sleepCnt = 0;
        uint64_t curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        //wait until the next period starts
        while (curTime < nextPeriodStartTime) {
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
        }
        NES_DEBUG("DataSource: Done with period " << curPeriod++
                                                  << " "
                                                     "and overall buffers="
                                                  << processedOverallBufferCnt << " sleepCnt=" << sleepCnt
                                                  << " startPeriod=" << startPeriod << " endPeriod=" << endPeriod
                                                  << " nextPeriodStartTime=" << nextPeriodStartTime << " curTime=" << curTime);
    }//end of while
    close();
    NES_DEBUG("DataSource " << operatorId << " end running");
}

void DataSource::runningRoutineWithGatheringInterval() {
    NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");
    std::string thName = "DataSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    NES_DEBUG("DataSource " << operatorId << ": Running Data Source of type=" << getType()
                            << " interval=" << gatheringInterval.count());
    if (numBuffersToProcess == 0) {
        NES_DEBUG("DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                  "the source is empty");
    } else {
        NES_DEBUG("DataSource: the user specify to produce " << numBuffersToProcess << " buffers");
    }
    open();
    uint64_t cnt = 0;
    while (running) {
        //check if already produced enough buffer
        if (numBuffersToProcess == 0 || cnt < numBuffersToProcess) {
            auto optBuf = receiveData();// note that receiveData might block
            if (!running) {             // necessary if source stops while receiveData is called due to stricter shutdown logic
                break;
            }
            //this checks we received a valid output buffer
            if (optBuf.has_value()) {
                auto& buf = optBuf.value();
                NES_TRACE("DataSource produced buffer" << operatorId << " type=" << getType() << " string=" << toString()
                                                       << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                                       << " iteration=" << cnt << " operatorId=" << this->operatorId
                                                       << " orgID=" << this->operatorId);

                if (Logger::getInstance()->getCurrentLogLevel() == LogLevel::LOG_TRACE) {
                    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
                    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);
                    NES_TRACE2("DataSource produced buffer content={}", buffer.toString(schema));
                }

                emitWorkFromSource(buf);
                ++cnt;
            } else {
                NES_DEBUG("DataSource " << operatorId << ": stopping cause of invalid buffer");
                running = false;
                NES_DEBUG("DataSource " << operatorId << ": Thread going to terminating with graceful exit.");
            }
        } else {
            NES_DEBUG("DataSource " << operatorId << ": Receiving thread terminated ... stopping because cnt=" << cnt
                                    << " smaller than numBuffersToProcess=" << numBuffersToProcess << " now return");
            running = false;
        }
        NES_TRACE("DataSource " << operatorId << ": Data Source finished processing iteration " << cnt);

        // this checks if the interval is zero or a ZMQ_Source, we don't create a watermark-only buffer
        if (getType() != SourceType::ZMQ_SOURCE && gatheringInterval.count() > 0) {
            std::this_thread::sleep_for(gatheringInterval);
        }
    }
    close();

    NES_DEBUG("DataSource " << operatorId << " end running");
}

void DataSource::runningRoutineAdaptiveGatheringInterval() {
    NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");
    std::string thName = "DataSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    NES_DEBUG("DataSource " << operatorId << ": Running Data Source of type=" << getType()
                            << " interval=" << gatheringInterval.count());
    if (numBuffersToProcess == 0) {
        NES_DEBUG("DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                  "the source is empty");
    } else {
        NES_DEBUG("DataSource: the user specify to produce " << numBuffersToProcess << " buffers");
    }

    this->kFilter->setGatheringInterval(this->gatheringInterval);
    this->kFilter->setGatheringIntervalRange(std::chrono::milliseconds{8000});

    open();
    uint64_t cnt = 0;
    while (running) {
        //check if already produced enough buffer
        if (cnt < numBuffersToProcess || numBuffersToProcess == 0) {
            auto optBuf = receiveData();// note that receiveData might block
            if (!running) {             // necessary if source stops while receiveData is called due to stricter shutdown logic
                break;
            }
            //this checks we received a valid output buffer
            if (optBuf.has_value()) {
                auto& buf = optBuf.value();

                if (this->gatheringInterval.count() != 0) {
                    NES_TRACE("DataSource old sourceGatheringInterval = " << this->gatheringInterval.count() << "ms");
                    this->kFilter->updateFromTupleBuffer(buf);
                    this->gatheringInterval = this->kFilter->getNewGatheringInterval();
                    NES_TRACE("DataSource new sourceGatheringInterval = " << this->gatheringInterval.count() << "ms");
                }

                NES_TRACE("DataSource produced buffer" << operatorId << " type=" << getType() << " string=" << toString()
                                                       << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                                       << " iteration=" << cnt << " operatorId=" << this->operatorId
                                                       << " orgID=" << this->operatorId);

                if (Logger::getInstance()->getCurrentLogLevel() == LogLevel::LOG_TRACE) {
                    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
                    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);
                    NES_TRACE("DataSource produced buffer content=" << buffer.toString(schema));
                }

                emitWorkFromSource(buf);
                ++cnt;
            } else {
                NES_ERROR("DataSource " << operatorId << ": stopping cause of invalid buffer");
                running = false;
                NES_DEBUG("DataSource " << operatorId << ": Thread terminating after graceful exit.");
            }
        } else {
            NES_DEBUG("DataSource " << operatorId << ": Receiving thread terminated ... stopping because cnt=" << cnt
                                    << " smaller than numBuffersToProcess=" << numBuffersToProcess << " now return");
            running = false;
        }
        NES_DEBUG("DataSource " << operatorId << ": Data Source finished processing iteration " << cnt);
    }

    // this checks if the interval is zero or a ZMQ_Source, we don't create a watermark-only buffer
    if (getType() != SourceType::ZMQ_SOURCE && gatheringInterval.count() > 0) {
        std::this_thread::sleep_for(gatheringInterval);
    }

    close();
    NES_DEBUG("DataSource " << operatorId << " end running");
}

bool DataSource::injectEpochBarrier(uint64_t epochBarrier, uint64_t queryId) const {
    NES_DEBUG("DataSource::injectEpochBarrier received timestamp " << epochBarrier << "with queryId " << queryId);
    return queryManager->injectEpochBarrier(epochBarrier, queryId, this->operatorId);
}

bool DataSource::resendData(uint64_t queryId) const {
    NES_DEBUG("DataSource::resendData for queryId " << queryId);
    return queryManager->resendData(queryId, this->operatorId);
}

// debugging
uint64_t DataSource::getNumberOfGeneratedTuples() const { return generatedTuples; };
uint64_t DataSource::getNumberOfGeneratedBuffers() const { return generatedBuffers; };

std::string DataSource::getSourceSchemaAsString() { return schema->toString(); }

uint64_t DataSource::getNumBuffersToProcess() const { return numBuffersToProcess; }

std::chrono::milliseconds DataSource::getGatheringInterval() const { return gatheringInterval; }
uint64_t DataSource::getGatheringIntervalCount() const { return gatheringInterval.count(); }
std::vector<Schema::MemoryLayoutType> DataSource::getSupportedLayouts() { return {Schema::MemoryLayoutType::ROW_LAYOUT}; }

bool DataSource::checkSupportedLayoutTypes(SchemaPtr& schema) {
    auto supportedLayouts = getSupportedLayouts();
    return std::find(supportedLayouts.begin(), supportedLayouts.end(), schema->getLayoutType()) != supportedLayouts.end();
}

Runtime::MemoryLayouts::DynamicTupleBuffer DataSource::allocateBuffer() {
    auto buffer = bufferManager->getBufferBlocking();
    return Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
}

void DataSource::onEvent(Runtime::BaseEvent& event) {
    NES_DEBUG("DataSource::onEvent(event) called. operatorId: " << this->operatorId);
    // no behaviour needed, call onEvent of direct ancestor
    DataEmitter::onEvent(event);
}

void DataSource::onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef) {
    NES_DEBUG("DataSource::onEvent(event, wrkContext) called. operatorId: " << this->operatorId);
    onEvent(event);
}

}// namespace NES
