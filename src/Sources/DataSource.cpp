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

#include <cassert>
#include <chrono>
#include <functional>
#include <iostream>
#include <limits>
#include <random>
#include <thread>

#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <future>

#include <Sources/ZmqSource.hpp>

#include <Util/ThreadBarrier.hpp>

#include <Sources/DataSource.hpp>
#include <Util/ThreadNaming.hpp>
#include <zconf.h>
namespace NES {

DataSource::DataSource(const SchemaPtr pSchema, NodeEngine::BufferManagerPtr bufferManager,
                       NodeEngine::QueryManagerPtr queryManager, OperatorId operatorId, size_t numSourceLocalBuffers)
    : running(false), thread(nullptr), schema(pSchema), globalBufferManager(bufferManager), queryManager(queryManager),
      generatedTuples(0), generatedBuffers(0), numBuffersToProcess(UINT64_MAX), gatheringInterval(0), operatorId(operatorId),
      numSourceLocalBuffers(numSourceLocalBuffers), wasGracefullyStopped(true) {
    NES_DEBUG("DataSource " << operatorId << ": Init Data Source with schema");
    NES_ASSERT(this->globalBufferManager, "Invalid buffer manager");
    NES_ASSERT(this->queryManager, "Invalid query manager");
}
OperatorId DataSource::getOperatorId() { return operatorId; }

void DataSource::setOperatorId(OperatorId operatorId) { this->operatorId = operatorId; }

SchemaPtr DataSource::getSchema() const { return schema; }

DataSource::~DataSource() {
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
                    auto ptr = dynamic_cast<ZmqSource*>(this);
                    ptr->disconnect();
                }

                thread->join();
                NES_DEBUG("DataSource: Thread joinded");
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
    NES_WARNING("Stopped Source = " << wasGracefullyStopped);
    return ret;
}

bool DataSource::isRunning() { return running; }

void DataSource::setGatheringInterval(std::chrono::milliseconds interval) { this->gatheringInterval = interval; }

void DataSource::open() { bufferManager = globalBufferManager->createFixedSizeBufferPool(numSourceLocalBuffers); }

void DataSource::runningRoutine() {
    NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");
    std::string thName = "DataSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    auto ts = std::chrono::system_clock::now();
    std::chrono::milliseconds lastTimeStampMillis = std::chrono::duration_cast<std::chrono::milliseconds>(ts.time_since_epoch());

    NES_DEBUG("DataSource " << operatorId << ": Running Data Source of type=" << getType());
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
        auto tsNow = std::chrono::system_clock::now();
        std::chrono::milliseconds nowInMillis = std::chrono::duration_cast<std::chrono::milliseconds>(tsNow.time_since_epoch());

        //this check checks if the gathering interval is less than one second or it is a ZMQ_Source, in both cases we do not need to create a watermark-only buffer
        NES_DEBUG("DataSource::runningRoutine will now check the type with gatheringInterval=" << gatheringInterval.count());
        if (gatheringInterval.count() <= 1000 || type == ZMQ_SOURCE) {
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
            NES_DEBUG("DataSource::runningRoutine check for specific source type");
            //check each second
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
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }

        //check if already produced enough buffer
        if (cnt < numBuffersToProcess || numBuffersToProcess == 0) {
            NES_DEBUG("DataSource::before receiveData");
            auto optBuf = receiveData();
            NES_DEBUG("DataSource::after receiveData");
            //this checks we received a valid output buffer
            if (optBuf.has_value()) {
                auto& buf = optBuf.value();
                NES_DEBUG("DataSource " << operatorId << " type=" << getType() << " string=" << toString()
                                        << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                        << " iteration=" << cnt << " operatorId=" << this->operatorId
                                        << " orgID=" << this->operatorId);
                if(buf.getNumberOfTuples() == 0){
                    running = false;
                }
                buf.setOriginId(operatorId);
                queryManager->addWork(operatorId, buf);
                cnt++;
            }
        } else {
            NES_DEBUG("DataSource " << operatorId << ": Receiving thread terminated ... stopping because cnt=" << cnt
                                    << " smaller than numBuffersToProcess=" << numBuffersToProcess << " now return");
            running = false;
        }
        NES_DEBUG("DataSource " << operatorId << ": Data Source finished processing iteration " << cnt);
    }
    // inject reconfiguration task containing end of stream
    queryManager->addEndOfStream(operatorId, wasGracefullyStopped);//
    bufferManager->destroy();
    queryManager.reset();
    NES_DEBUG("DataSource " << operatorId << " end running");
}

// debugging
uint64_t DataSource::getNumberOfGeneratedTuples() { return generatedTuples; };
uint64_t DataSource::getNumberOfGeneratedBuffers() { return generatedBuffers; };

std::string DataSource::getSourceSchemaAsString() { return schema->toString(); }

uint64_t DataSource::getNumBuffersToProcess() const { return numBuffersToProcess; }

std::chrono::milliseconds DataSource::getGatheringInterval() const { return gatheringInterval; }
uint64_t DataSource::getGatheringIntervalCount() const { return gatheringInterval.count(); }

}// namespace NES