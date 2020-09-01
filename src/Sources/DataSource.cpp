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
#include <Windows/Watermark/ProcessingTimeWatermarkGenerator.hpp>
namespace NES {

DataSource::DataSource(const SchemaPtr pSchema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager)
    : running(false), thread(nullptr), schema(pSchema), bufferManager(bufferManager), queryManager(queryManager),
      generatedTuples(0), generatedBuffers(0), numBuffersToProcess(UINT64_MAX), gatheringInterval(0), sourceId(UtilityFunctions::generateIdString()) {
    NES_DEBUG("DataSource " << this->getSourceId() << ": Init Data Source with schema");
    NES_ASSERT(this->bufferManager, "Invalid buffer manager");
    NES_ASSERT(this->queryManager, "Invalid query manager");
    watermark = std::make_shared<ProcessingTimeWatermarkGenerator>();
}

DataSource::DataSource(const SchemaPtr pSchema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                       std::string sourceId)
    : running(false), thread(nullptr), schema(pSchema), bufferManager(bufferManager), queryManager(queryManager),
      generatedTuples(0), generatedBuffers(0), numBuffersToProcess(UINT64_MAX), gatheringInterval(0),
      sourceId(sourceId) {
    NES_DEBUG("DataSource " << this->getSourceId() << ": Init Data Source with schema");
    NES_ASSERT(this->bufferManager, "Invalid buffer manager");
    NES_ASSERT(this->queryManager, "Invalid query manager");
}

SchemaPtr DataSource::getSchema() const { return schema; }

DataSource::~DataSource() {
    stop();
    NES_DEBUG("DataSource " << this->getSourceId() << ": Destroy Data Source.");
}

bool DataSource::start() {
    NES_DEBUG("DataSource " << this->getSourceId() << ": start source " << this);
    auto barrier = std::make_shared<ThreadBarrier>(2);
    std::unique_lock lock(startStopMutex);
    if (running) {
        NES_WARNING("DataSource " << this->getSourceId() << ": is already running " << this);
        return false;
    }
    running = true;
    type = getType();
    NES_DEBUG("DataSource " << this->getSourceId() << ": Spawn thread");
    thread = std::make_shared<std::thread>([this, barrier]() {
        barrier->wait();
        runningRoutine(bufferManager, queryManager);
    });
    barrier->wait();
    return true;
}

bool DataSource::stop() {
    std::unique_lock lock(startStopMutex);
    NES_DEBUG("DataSource " << this->getSourceId() << ": Stop called and source is "
                            << (running ? "running" : "not running"));
    if (!running) {
        NES_DEBUG("DataSource " << this->getSourceId() << " is not running");
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
                NES_DEBUG("DataSource " << this->getSourceId() << ": Thread is not joinable");
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

void DataSource::setGatheringInterval(size_t interval) { this->gatheringInterval = interval; }

void DataSource::runningRoutine(BufferManagerPtr bufferManager, QueryManagerPtr queryManager) {
    setThreadName("DataSrc-%d", getSourceId().c_str());
    if (!queryManager) {
        NES_ERROR("query Manager not set");
        throw std::logic_error("DataSource: QueryManager not set");
    }
    if (!bufferManager) {
        NES_ERROR("bufferManager not set");
        throw std::logic_error("DataSource: BufferManager not set");
    }
    auto lastTimeStamp = std::chrono::system_clock::now();

    if (!this->sourceId.empty()) {
        NES_DEBUG("DataSource " << this->getSourceId() << ": Running Data Source of type=" << getType());
        size_t cnt = 0;
        while (running) {
            bool recNow = false;
            auto now = std::chrono::system_clock::now();
            //this check checks if the gathering interval is less than one second or it is a ZMQ_Source, in both cases we do not need to create a watermark-only buffer
            if (gatheringInterval <= 1 || type == ZMQ_SOURCE) {
                NES_DEBUG("DataSource::runningRoutine will produce buffers fast enough for source type=" << getType());
                if (gatheringInterval == 0 || (lastTimeStamp != now && now.time_since_epoch().count() % gatheringInterval == 0)) {
                    NES_DEBUG("DataSource::runningRoutine gathering interval reached so produce a buffer");
                    recNow = true;
                }
            } else {
                //check each second
                if (now != lastTimeStamp) {                                       //we are in another second
                    if (now.time_since_epoch().count() % gatheringInterval == 0) {//produce a regular buffer
                        NES_DEBUG("DataSource::runningRoutine sending regular buffer");
                        recNow = true;
                    } else {//produce watermark only buffer
                        NES_DEBUG("DataSource::runningRoutine sending watermark-only buffer");
                        auto buffer = bufferManager->getBufferBlocking();
                        buffer.setWatermark(watermark->getWatermark());
                        buffer.setNumberOfTuples(0);
                        queryManager->addWork(this->sourceId, buffer);
                    }
                }
            }
            lastTimeStamp = now;

            //repeat test
            if (!recNow) {
                sleep(1);
                continue;
            }

            //check if already produced enough buffer
            if (cnt < numBuffersToProcess) {
                auto optBuf = receiveData();

                //this checks we received a valid output buffer
                if (!!optBuf) {
                    auto& buf = optBuf.value();
                    NES_DEBUG("DataSource " << this->getSourceId() << " type=" << getType()
                                            << " string=" << toString()
                                            << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                            << " iteration=" << cnt);

                    queryManager->addWork(this->sourceId, buf);
                    cnt++;
                }
            } else {
                NES_DEBUG("DataSource "
                          << this->getSourceId() << ": Receiving thread terminated ... stopping because cnt=" << cnt
                          << " smaller than numBuffersToProcess=" << numBuffersToProcess << " now return");
                return;
            }
            NES_DEBUG("DataSource " << this->getSourceId() << ": Data Source finished processing iteration " << cnt);
            NES_DEBUG("DataSource::runningRoutine: exist routine " << this);
        }
    }//end of if souce not empty
}

// debugging
void DataSource::setNumBuffersToProcess(size_t cnt) { numBuffersToProcess = cnt; };
size_t DataSource::getNumberOfGeneratedTuples() { return generatedTuples; };
size_t DataSource::getNumberOfGeneratedBuffers() { return generatedBuffers; };

std::string DataSource::getSourceSchemaAsString() { return schema->toString(); }

const std::string& DataSource::getSourceId() const { return this->sourceId; }

size_t DataSource::getNumBuffersToProcess() const {
    return numBuffersToProcess;
}

size_t DataSource::getGatheringInterval() const {
    return gatheringInterval;
}

}// namespace NES
