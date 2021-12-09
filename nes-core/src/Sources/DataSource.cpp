/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <iostream>
#include <thread>

#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <future>

#include <Sources/ZmqSource.hpp>

#include <Sources/DataSource.hpp>
#include <Util/ThreadNaming.hpp>
#ifdef NES_ENABLE_NUMA_SUPPORT
#if defined(__linux__)
#include <numa.h>
#include <numaif.h>
#endif
#endif
#include <utility>
namespace NES {

std::vector<Runtime::Execution::SuccessorExecutablePipeline> DataSource::getExecutableSuccessors() {
    return executableSuccessors;
}

DataSource::GatheringMode DataSource::getGatheringModeFromString(const std::string& mode) {
    Util::trim(mode);
    if (mode == "frequency") {
        return GatheringMode::FREQUENCY_MODE;
    }
    if (mode == "ingestionrate") {
        return GatheringMode::INGESTION_RATE_MODE;
    }
    if (mode == "adaptive") {
        return GatheringMode::ADAPTIVE_MODE;
    }
    NES_THROW_RUNTIME_ERROR("mode not supported " << mode);
}

DataSource::DataSource(SchemaPtr pSchema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       OperatorId operatorId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
    : queryManager(std::move(queryManager)), localBufferManager(std::move(bufferManager)),
      executableSuccessors(std::move(executableSuccessors)), operatorId(operatorId), schema(std::move(pSchema)),
      numSourceLocalBuffers(numSourceLocalBuffers), gatheringMode(gatheringMode) {
#ifdef ENABLE_ADAPTIVE_BUILD
    this->kFilter.setDefaultValues();
#endif
    NES_DEBUG("DataSource " << operatorId << ": Init Data Source with schema");
    NES_ASSERT(this->localBufferManager, "Invalid buffer manager");
    NES_ASSERT(this->queryManager, "Invalid query manager");
    if (schema->getLayoutType() == Schema::ROW_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, localBufferManager->getBufferSize());
    } else if (schema->getLayoutType() == Schema::COLUMNAR_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, localBufferManager->getBufferSize());
    }
}

void DataSource::emitWorkFromSource(Runtime::TupleBuffer& buffer) {
    // set the origin id for this source
    buffer.setOriginId(operatorId);
    // set the creation timestamp
    buffer.setCreationTimestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
            .count());
    // Set the sequence number of this buffer.
    // A data source generates a monotonic increasing sequence number
    maxSequenceNumber++;
    buffer.setSequenceNumber(maxSequenceNumber);
    emitWork(buffer);
}

void DataSource::emitWork(Runtime::TupleBuffer& buffer) {
    for (const auto& successor : executableSuccessors) {
        queryManager->addWorkForNextPipeline(buffer, successor, numaNode);
    }
}

OperatorId DataSource::getOperatorId() const { return operatorId; }

void DataSource::setOperatorId(OperatorId operatorId) { this->operatorId = operatorId; }

SchemaPtr DataSource::getSchema() const { return schema; }

DataSource::~DataSource() NES_NOEXCEPT(false) {
    executableSuccessors.clear();
    stop(false);
    NES_DEBUG("DataSource " << operatorId << ": Destroy Data Source.");
}

bool DataSource::start() {
    NES_DEBUG("DataSource " << operatorId << ": start source " << this);
    std::promise<bool> prom;
    std::unique_lock lock(startStopMutex);
    if (running) {
        NES_WARNING("DataSource " << operatorId << ": is already running " << this);
        return false;
    }
    running = true;
    type = getType();
    NES_DEBUG("DataSource " << operatorId << ": Spawn thread");
    thread = std::make_shared<std::thread>([this, &prom]() {
    // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
    // only CPU i as set.
#ifdef __linux__
#ifdef NES_ENABLE_NUMA_SUPPORT
        if (sourceAffinity != std::numeric_limits<uint64_t>::max()) {
            NES_ASSERT(sourceAffinity < std::thread::hardware_concurrency(), "pinning position is out of cpu range");
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(sourceAffinity, &cpuset);
            int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

            if (rc != 0) {
                NES_ERROR("Error calling set pthread_setaffinity_np: " << rc);
            } else {
                int cpu = sched_getcpu();
                numaNode = numa_node_of_cpu(cpu);

                unsigned long cur_mask;
                auto ret = pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), (cpu_set_t*) &cur_mask);
                if (ret != 0) {
                    NES_ERROR("Error calling set pthread_getaffinity_np: " << rc);
                }
                std::cout << "source " << operatorId << " pins to core=" << sourceAffinity << " on numaNode=" << numaNode << " ";
                printf("setted affinity after assignment: %08lx\n", cur_mask);
            }
        } else {
            NES_WARNING("Use default affinity for source");
        }
#endif
#endif

        prom.set_value(true);
        runningRoutine();
    });
    return prom.get_future().get();
}

bool DataSource::stop(bool graceful) {
    std::unique_lock lock(startStopMutex);
    NES_DEBUG("DataSource " << operatorId << ": Stop called and source is " << (running ? "running" : "not running"));
    if (!running) {
        NES_DEBUG("DataSource " << operatorId << " is not running");
        if (thread && thread->joinable()) {
            thread->join();
            thread.reset();
        }
        return false;
    }

    wasGracefullyStopped = graceful;
    // TODO add wakeUp call if source is blocking on something, e.g., tcp socket
    // TODO in general this highlights how our source model has some issues
    running = false;
    bool ret = false;

    try {
        NES_ASSERT2_FMT(!!thread, "Thread for source " << operatorId << " is not existing");
        {
            NES_DEBUG("DataSource::stop try to join threads=" << thread->get_id());
            if (thread->joinable()) {
                NES_DEBUG("DataSource::stop thread is joinable=" << thread->get_id());
                // TODO this is only a workaround and will be replaced by the network stack upate
                if (type == 0) {
                    NES_WARNING("DataSource::stop source hard cause of zmq_source");
                    auto* ptr = dynamic_cast<ZmqSource*>(this);
                    ptr->disconnect();
                }
                thread->join();

                NES_DEBUG("DataSource: Thread joined");
                ret = true;
                thread.reset();
            } else {
                NES_ERROR("DataSource " << operatorId << ": Thread is not joinable");
                wasGracefullyStopped = false;
                return false;
            }
        }
    } catch (...) {
        NES_ERROR("DataSource::stop error IN CATCH");
        auto expPtr = std::current_exception();
        wasGracefullyStopped = false;
        try {
            if (expPtr) {
                std::rethrow_exception(expPtr);
            }
        } catch (const std::exception& e) {// it would not work if you pass by value
            NES_ERROR("DataSource::stop error while stopping data source " << this << " error=" << e.what());
        }
    }
    NES_WARNING("Stopped Source " << operatorId << " = "
                                  << (wasGracefullyStopped ? "wasGracefullyStopped" : "notGracefullyStopped"));
    return ret;
}

void DataSource::setGatheringInterval(std::chrono::milliseconds interval) { this->gatheringInterval = interval; }

void DataSource::open() { bufferManager = localBufferManager->createFixedSizeBufferPool(numSourceLocalBuffers); }

void DataSource::close() {}

void DataSource::runningRoutine() {
    //TDOD startup delay
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    if (gatheringMode == GatheringMode::FREQUENCY_MODE) {
        runningRoutineWithFrequency();
    } else if (gatheringMode == GatheringMode::INGESTION_RATE_MODE) {
        runningRoutineWithIngestionRate();
    } else if (gatheringMode == GatheringMode::ADAPTIVE_MODE) {
        runningRoutineAdaptive();
    }
}

void DataSource::runningRoutineWithIngestionRate() {
    NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");
    NES_ASSERT(gatheringIngestionRate >= 10, "As we generate on 100 ms base we need at least an ingestion rate of 10");
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
    uint64_t buffersToProducePer100Ms = gatheringIngestionRate / 10;
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
                NES_DEBUG("DataSource: add task for buffer");
                auto& buf = optBuf.value();
                emitWorkFromSource(buf);

                buffersProcessedCnt++;
                processedOverallBufferCnt++;
            } else {
                NES_ERROR("DataSource: Buffer is invalid");
            }
            NES_DEBUG("DataSource: buffersProcessedCnt=" << buffersProcessedCnt
                                                         << " buffersPerSecond=" << gatheringIngestionRate);
        }

        uint64_t endPeriod =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        //next point in time when to start producing again
        nextPeriodStartTime = uint64_t(startPeriod + (100));
        NES_DEBUG("DataSource: startTimeSendBuffers=" << startPeriod << " endTimeSendBuffers=" << endPeriod
                                                      << " nextPeriodStartTime=" << nextPeriodStartTime);

        //If this happens then the second was not enough to create so many tuples and the ingestion rate should be decreased
        if (nextPeriodStartTime < endPeriod) {
            NES_ERROR("Creating buffer(s) for DataSource took longer than periodLength. nextPeriodStartTime="
                      << nextPeriodStartTime << " endTimeSendBuffers=" << endPeriod);
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

    // inject reconfiguration task containing end of stream
    queryManager->addEndOfStream(shared_from_base<DataSource>(), wasGracefullyStopped);//
    bufferManager->destroy();
    queryManager.reset();
    NES_DEBUG("DataSource " << operatorId << " end running");
}

void DataSource::runningRoutineWithFrequency() {
    NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");
    std::string thName = "DataSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    auto ts = std::chrono::system_clock::now();
    std::chrono::milliseconds lastTimeStampMillis = std::chrono::duration_cast<std::chrono::milliseconds>(ts.time_since_epoch());

    NES_DEBUG("DataSource " << operatorId << ": Running Data Source of type=" << getType()
                            << " frequency=" << gatheringInterval.count());
    if (numBuffersToProcess == 0) {
        NES_DEBUG("DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                  "the source is empty");
    } else {
        NES_DEBUG("DataSource: the user specify to produce " << numBuffersToProcess << " buffers");
    }
    open();
    uint64_t cnt = 0;
    while (running) {
        bool recNow = false;
        auto tsNow = std::chrono::steady_clock::now();
        std::chrono::milliseconds nowInMillis = std::chrono::duration_cast<std::chrono::milliseconds>(tsNow.time_since_epoch());

        //this check checks if the gathering interval is zero or a ZMQ_Source, where we do not create a watermark-only buffer
        NES_DEBUG("DataSource::runningRoutine will now check src type with gatheringInterval=" << gatheringInterval.count());
        if (gatheringInterval.count() == 0 || getType() == SourceType::ZMQ_SOURCE) {// 0 means never sleep
            NES_DEBUG("DataSource::runningRoutine will produce buffers fast enough for source type="
                      << getType() << " and gatheringInterval=" << gatheringInterval.count()
                      << "ms, tsNow=" << lastTimeStampMillis.count() << "ms, now=" << nowInMillis.count() << "ms");
            if (gatheringInterval.count() == 0 || lastTimeStampMillis != nowInMillis) {
                NES_DEBUG("DataSource::runningRoutine gathering interval reached so produce a buffer gatheringInterval="
                          << gatheringInterval.count() << "ms, tsNow=" << lastTimeStampMillis.count()
                          << "ms, now=" << nowInMillis.count() << "ms");
                recNow = true;
                lastTimeStampMillis = nowInMillis;
            } else {
                NES_DEBUG("lastTimeStampMillis=" << lastTimeStampMillis.count() << "nowInMillis=" << nowInMillis.count());
            }
        } else {
            NES_DEBUG("DataSource::runningRoutine check for interval");
            // check each interval
            if (nowInMillis != lastTimeStampMillis) {//we are in another interval
                if ((nowInMillis - lastTimeStampMillis) <= gatheringInterval
                    || ((nowInMillis - lastTimeStampMillis) % gatheringInterval).count() == 0) {//produce a regular buffer
                    NES_DEBUG("DataSource::runningRoutine sending regular buffer");
                    recNow = true;
                }
                lastTimeStampMillis = nowInMillis;
            }
        }

        //repeat test
        if (!recNow) {
            std::this_thread::sleep_for(gatheringInterval);
            continue;
        }

        //check if already produced enough buffer
        if (cnt < numBuffersToProcess || numBuffersToProcess == 0) {
            auto optBuf = receiveData();// note that receiveData might block
            if (!running) {             // necessary if source stops while receiveData is called due to stricter shutdown logic
                break;
            }
            //this checks we received a valid output buffer
            if (optBuf.has_value()) {
                auto& buf = optBuf.value();
                NES_DEBUG("DataSource produced buffer" << operatorId << " type=" << getType() << " string=" << toString()
                                                       << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                                       << " iteration=" << cnt << " operatorId=" << this->operatorId
                                                       << " orgID=" << this->operatorId);
                NES_TRACE("DataSource produced buffer content=" << Util::prettyPrintTupleBuffer(buf, schema));

                emitWorkFromSource(buf);
                ++cnt;
            } else {
                if (!wasGracefullyStopped) {
                    NES_ERROR("DataSource " << operatorId << ": stopping cause of invalid buffer");
                    running = false;
                }
                NES_DEBUG("DataSource " << operatorId << ": Thread terminating after graceful exit.");
            }
        } else {
            NES_DEBUG("DataSource " << operatorId << ": Receiving thread terminated ... stopping because cnt=" << cnt
                                    << " smaller than numBuffersToProcess=" << numBuffersToProcess << " now return");
            running = false;
            wasGracefullyStopped = true;
        }
        NES_DEBUG("DataSource " << operatorId << ": Data Source finished processing iteration " << cnt);
    }
    close();
    // inject reconfiguration task containing end of stream
    NES_DEBUG("DataSource " << operatorId << ": Data Source add end of stream. Gracefully= " << wasGracefullyStopped);
    queryManager->addEndOfStream(shared_from_base<DataSource>(), wasGracefullyStopped);//
    bufferManager->destroy();
    queryManager.reset();
    NES_DEBUG("DataSource " << operatorId << " end running");
}

void DataSource::runningRoutineAdaptive() {
    NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");
    std::string thName = "DataSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    auto ts = std::chrono::system_clock::now();
    std::chrono::milliseconds lastTimeStampMillis = std::chrono::duration_cast<std::chrono::milliseconds>(ts.time_since_epoch());

    NES_DEBUG("DataSource " << operatorId << ": Running Data Source of type=" << getType()
                            << " frequency=" << gatheringInterval.count());
    if (numBuffersToProcess == 0) {
        NES_DEBUG("DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                  "the source is empty");
    } else {
        NES_DEBUG("DataSource: the user specify to produce " << numBuffersToProcess << " buffers");
    }

#ifdef ENABLE_ADAPTIVE_BUILD
    // TODO: add issue to remove the ifdef in the project
    // TODO: add issue to properly parameterize frequency range and defaults
    this->kFilter.setFrequency(this->gatheringInterval);
    this->kFilter.setFrequencyRange(std::chrono::milliseconds{8000});
#endif

    open();
    uint64_t cnt = 0;
    while (running) {
        bool recNow = false;
        auto tsNow = std::chrono::steady_clock::now();
        std::chrono::milliseconds nowInMillis = std::chrono::duration_cast<std::chrono::milliseconds>(tsNow.time_since_epoch());

        //this check checks if the gathering interval is zero or a ZMQ_Source, where we do not create a watermark-only buffer
        NES_DEBUG("DataSource::runningRoutine will now check src type with gatheringInterval=" << gatheringInterval.count());
        if (gatheringInterval.count() == 0 || getType() == SourceType::ZMQ_SOURCE) {// 0 means never sleep
            NES_DEBUG("DataSource::runningRoutine will produce buffers fast enough for source type="
                      << getType() << " and gatheringInterval=" << gatheringInterval.count()
                      << "ms, tsNow=" << lastTimeStampMillis.count() << "ms, now=" << nowInMillis.count() << "ms");
            if (gatheringInterval.count() == 0 || lastTimeStampMillis != nowInMillis) {
                NES_DEBUG("DataSource::runningRoutine gathering interval reached so produce a buffer gatheringInterval="
                          << gatheringInterval.count() << "ms, tsNow=" << lastTimeStampMillis.count()
                          << "ms, now=" << nowInMillis.count() << "ms");
                recNow = true;
                lastTimeStampMillis = nowInMillis;
            } else {
                NES_DEBUG("lastTimeStampMillis=" << lastTimeStampMillis.count() << "nowInMillis=" << nowInMillis.count());
            }
        } else {
            NES_DEBUG("DataSource::runningRoutine check for interval");
            // check each interval
            if (nowInMillis != lastTimeStampMillis) {//we are in another interval
                if ((nowInMillis - lastTimeStampMillis) <= gatheringInterval
                    || ((nowInMillis - lastTimeStampMillis) % gatheringInterval).count() == 0) {//produce a regular buffer
                    NES_DEBUG("DataSource::runningRoutine sending regular buffer");
                    recNow = true;
                }
                lastTimeStampMillis = nowInMillis;
            }
        }

        //repeat test
        if (!recNow) {
            std::this_thread::sleep_for(gatheringInterval);
            continue;
        }

        //check if already produced enough buffer
        if (cnt < numBuffersToProcess || numBuffersToProcess == 0) {
            auto optBuf = receiveData();// note that receiveData might block
            if (!running) {             // necessary if source stops while receiveData is called due to stricter shutdown logic
                break;
            }
            //this checks we received a valid output buffer
            if (optBuf.has_value()) {
                auto& buf = optBuf.value();
#ifdef ENABLE_ADAPTIVE_BUILD
                this->kFilter.updateFromTupleBuffer(buf);
                this->gatheringInterval = this->kFilter.getNewFrequency();
#endif
                NES_DEBUG("DataSource produced buffer" << operatorId << " type=" << getType() << " string=" << toString()
                                                       << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                                       << " iteration=" << cnt << " operatorId=" << this->operatorId
                                                       << " orgID=" << this->operatorId);
                NES_TRACE("DataSource produced buffer content=" << Util::prettyPrintTupleBuffer(buf, schema));

                emitWorkFromSource(buf);
                ++cnt;
            } else {
                if (!wasGracefullyStopped) {
                    NES_ERROR("DataSource " << operatorId << ": stopping cause of invalid buffer");
                    running = false;
                }
                NES_DEBUG("DataSource " << operatorId << ": Thread terminating after graceful exit.");
            }
        } else {
            NES_DEBUG("DataSource " << operatorId << ": Receiving thread terminated ... stopping because cnt=" << cnt
                                    << " smaller than numBuffersToProcess=" << numBuffersToProcess << " now return");
            running = false;
            wasGracefullyStopped = true;
        }
        NES_DEBUG("DataSource " << operatorId << ": Data Source finished processing iteration " << cnt);
    }
    close();
    // inject reconfiguration task containing end of stream
    NES_DEBUG("DataSource " << operatorId << ": Data Source add end of stream. Gracefully= " << wasGracefullyStopped);
    queryManager->addEndOfStream(shared_from_base<DataSource>(), wasGracefullyStopped);//
    bufferManager->destroy();
    queryManager.reset();
    NES_DEBUG("DataSource " << operatorId << " end running");
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

}// namespace NES
