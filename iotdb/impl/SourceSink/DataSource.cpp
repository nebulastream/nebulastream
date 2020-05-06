#include <cassert>
#include <functional>
#include <iostream>
#include <random>

#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>

#include <Util/ThreadBarrier.hpp>

#include <SourceSink/DataSource.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::DataSource);

namespace NES {

DataSource::DataSource(const SchemaPtr pSchema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager)
    : running(false), thread(nullptr), schema(pSchema), bufferManager(bufferManager), queryManager(queryManager),
      generatedTuples(0), generatedBuffers(0), numBuffersToProcess(UINT64_MAX), gatheringInterval(0),
      lastGatheringTimeStamp(0), sourceId(UtilityFunctions::generateUuid()) { NES_DEBUG(
                                     "DataSource " << this->getSourceId() << ": Init Data Source with schema"); };

DataSource::DataSource() {
    NES_DEBUG("DataSource " << this->getSourceId() << ": Init Data Source Default w/o schema");
    running = false;
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
                    return true;
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

void DataSource::setQueryManager(QueryManagerPtr queryManager) { this->queryManager = queryManager; }
void DataSource::setBufferManger(BufferManagerPtr bufferManager) { this->bufferManager = bufferManager; }

void DataSource::runningRoutine(BufferManagerPtr bufferManager, QueryManagerPtr queryManager) {
    if (!queryManager) {
        NES_ERROR("query Manager not set");
        assert(0);
    }
    if (!bufferManager) {
        NES_ERROR("bufferManager not set");
        assert(0);
    }

    if (!this->sourceId.empty()) {
        NES_DEBUG("DataSource " << this->getSourceId() << ": Running Data Source of type=" << getType());
        size_t cnt = 0;

        while (running) {
            size_t currentTime = time(NULL);
            if (gatheringInterval == 0 || (lastGatheringTimeStamp != currentTime && currentTime % gatheringInterval == 0)) {// produce a buffer
                lastGatheringTimeStamp = currentTime;
                if (cnt < numBuffersToProcess) {
                    auto optBuf = receiveData();
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
                    return;// TODO: check if this really has to be done of if we just continue looping
                    // TODO: maybe set running false
                }
            } else {
                NES_DEBUG("DataSource::runningRoutine sleep " << this);
                sleep(gatheringInterval);
                continue;
            }
            NES_DEBUG("DataSource " << this->getSourceId() << ": Data Source finished processing iteration " << cnt);
        }// end of while running
        NES_DEBUG("DataSource " << this->getSourceId() << ": end running");
    } else {
        NES_FATAL_ERROR("DataSource " << this->getSourceId() << ": No ID assigned. Running_routine is not possible!");
        throw std::logic_error("DataSource: No ID assigned. Running_routine is not possible!");
    }
    NES_DEBUG("DataSource::runningRoutine: exist routine " << this);
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

} // namespace NES
