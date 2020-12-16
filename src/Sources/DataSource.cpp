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
#include <functional>
#include <iostream>
#include <random>

#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <Sources/ZmqSource.hpp>

#include <Util/ThreadBarrier.hpp>

#include <Sources/DataSource.hpp>
#include <Util/ThreadNaming.hpp>
#include <zconf.h>
namespace NES {

DataSource::DataSource(const SchemaPtr pSchema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                       OperatorId operatorId)
    : running(false), thread(nullptr), schema(pSchema), bufferManager(bufferManager), queryManager(queryManager),
      generatedTuples(0), generatedBuffers(0), numBuffersToProcess(UINT64_MAX), gatheringInterval(0), operatorId(operatorId) {
    NES_DEBUG("DataSource " << operatorId << ": Init Data Source with schema");
    NES_ASSERT(this->bufferManager, "Invalid buffer manager");
    NES_ASSERT(this->queryManager, "Invalid query manager");
}
OperatorId DataSource::getOperatorId() { return operatorId; }

void DataSource::setOperatorId(OperatorId operatorId) { this->operatorId = operatorId; }

SchemaPtr DataSource::getSchema() const { return schema; }

DataSource::~DataSource() {
    stop();
    NES_DEBUG("DataSource " << operatorId << ": Destroy Data Source.");
}

bool DataSource::start() {
    NES_DEBUG("DataSource " << operatorId << ": start source " << this);
    auto barrier = std::make_shared<ThreadBarrier>(2);
    std::unique_lock lock(startStopMutex);
    if (running) {
        NES_WARNING("DataSource " << operatorId << ": is already running " << this);
        return false;
    }
    running = true;
    type = getType();
    NES_DEBUG("DataSource " << operatorId << ": Spawn thread");
    thread = std::make_shared<std::thread>([this, barrier]() {
        barrier->wait();
        runningRoutine(bufferManager, queryManager);
    });
    barrier->wait();
    return true;
}

bool DataSource::stop() {
    std::unique_lock lock(startStopMutex);
    NES_DEBUG("DataSource " << operatorId << ": Stop called and source is " << (running ? "running" : "not running"));
    if (!running) {
        NES_DEBUG("DataSource " << operatorId << " is not running");
        return false;
    }
    running = false;
    bool ret = false;

    try {
        if (thread) {
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
                NES_DEBUG("DataSource " << operatorId << ": Thread is not joinable");
                return false;
            }
        }
    } catch (...) {
        NES_ERROR("DataSource::stop error IN CATCH");
        auto expPtr = std::current_exception();
        try {
            if (expPtr)
                std::rethrow_exception(expPtr);
        } catch (const std::exception& e)// it would not work if you pass by value
        {
            NES_ERROR("DataSource::stop error while stopping data source " << this << " error=" << e.what());
        }
    }

    return ret;
}

bool DataSource::isRunning() { return running; }

void DataSource::setGatheringInterval(uint64_t interval) { this->gatheringInterval = interval; }

void DataSource::runningRoutine(BufferManagerPtr bufferManager, QueryManagerPtr queryManager) {
    NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");

    std::string thName = "DataSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    if (!queryManager) {
        NES_ERROR("query Manager not set");
        throw std::logic_error("DataSource: QueryManager not set");
    }
    if (!bufferManager) {
        NES_ERROR("bufferManager not set");
        throw std::logic_error("DataSource: BufferManager not set");
    }

    auto ts = std::chrono::system_clock::now();
    std::chrono::seconds lastTimeStampSec = std::chrono::duration_cast<std::chrono::seconds>(ts.time_since_epoch());

    NES_DEBUG("DataSource " << operatorId << ": Running Data Source of type=" << getType());
    if (numBuffersToProcess == 0) {
        NES_DEBUG("DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                  "the source is empty");
    } else {
        NES_DEBUG("DataSource: the user specify to produce " << numBuffersToProcess << " buffers");
    }

    uint64_t cnt = 0;
    while (running) {
        bool recNow = false;
        auto tsNow = std::chrono::system_clock::now();
        std::chrono::seconds nowInSec = std::chrono::duration_cast<std::chrono::seconds>(tsNow.time_since_epoch());

        //this check checks if the gathering interval is less than one second or it is a ZMQ_Source, in both cases we do not need to create a watermark-only buffer
        if (gatheringInterval <= 1 || type == ZMQ_SOURCE) {
            NES_DEBUG("DataSource::runningRoutine will produce buffers fast enough for source type=" << getType());
            if (gatheringInterval == 0 || lastTimeStampSec != nowInSec) {
                NES_DEBUG("DataSource::runningRoutine gathering interval reached so produce a buffer gatheringInterval="
                          << gatheringInterval << " tsNow=" << lastTimeStampSec.count() << " now=" << nowInSec.count());
                recNow = true;
                lastTimeStampSec = nowInSec;
            }
        } else {
            //check each second
            if (nowInSec != lastTimeStampSec) {                 //we are in another second
                if (nowInSec.count() % gatheringInterval == 0) {//produce a regular buffer
                    NES_DEBUG("DataSource::runningRoutine sending regular buffer");
                    recNow = true;
                }
                lastTimeStampSec = nowInSec;
            }
        }

        //repeat test
        if (!recNow) {
            sleep(1);
            continue;
        }

        //check if already produced enough buffer
        if (cnt < numBuffersToProcess || numBuffersToProcess == 0) {
            auto optBuf = receiveData();

            //this checks we received a valid output buffer
            if (optBuf.has_value()) {
                auto& buf = optBuf.value();
                NES_DEBUG("DataSource " << operatorId << " type=" << getType() << " string=" << toString()
                                        << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                        << " iteration=" << cnt << " operatorId=" << this->operatorId
                                        << " orgID=" << this->operatorId);
                buf.setOriginId(operatorId);
                queryManager->addWork(operatorId, buf);
                cnt++;
            }
        } else {
            NES_DEBUG("DataSource " << operatorId << ": Receiving thread terminated ... stopping because cnt=" << cnt
                                    << " smaller than numBuffersToProcess=" << numBuffersToProcess << " now return");
            return;
        }
        NES_DEBUG("DataSource " << operatorId << ": Data Source finished processing iteration " << cnt);
        NES_DEBUG("DataSource::runningRoutine: exist routine " << this);
    }
}

// debugging
uint64_t DataSource::getNumberOfGeneratedTuples() { return generatedTuples; };
uint64_t DataSource::getNumberOfGeneratedBuffers() { return generatedBuffers; };

std::string DataSource::getSourceSchemaAsString() { return schema->toString(); }

uint64_t DataSource::getNumBuffersToProcess() const { return numBuffersToProcess; }

uint64_t DataSource::getGatheringInterval() const { return gatheringInterval; }

}// namespace NES