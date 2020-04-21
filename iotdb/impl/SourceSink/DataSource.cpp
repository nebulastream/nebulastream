#include <cassert>
#include <functional>
#include <iostream>
#include <random>

#include <NodeEngine/Dispatcher.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/Logger.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>

#include <Util/ThreadBarrier.hpp>

#include <SourceSink/DataSource.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::DataSource);

namespace NES {

DataSource::DataSource(const SchemaPtr pSchema)
    :
    running(false),
    thread(nullptr),
    schema(pSchema),
    generatedTuples(0),
    generatedBuffers(0),
    numBuffersToProcess(UINT64_MAX),
    gatheringInterval(0),
    lastGatheringTimeStamp(0),
    sourceId(UtilityFunctions::generateUuid()) {
    NES_DEBUG(
        "DataSource " << this->getSourceId() << ": Init Data Source with schema")
}

DataSource::DataSource()
    : DataSource(nullptr) {
    NES_DEBUG(
        "DataSource " << this->getSourceId() << ": Init Data Source Default w/o schema")
}

SchemaPtr DataSource::getSchema() const {
    return schema;
}

DataSource::~DataSource() {
    stop();
    NES_DEBUG("DataSource " << this->getSourceId() << ": Destroy Data Source.")
}

bool DataSource::start() {
    auto barrier = std::make_shared<ThreadBarrier>(2);
    std::unique_lock lock(startStopMutex);
    if (running) {
        return false;
    }
    running = true;

    NES_DEBUG("DataSource " << this->getSourceId() << ": Spawn thread")
    thread = std::make_shared<std::thread>([this, barrier]() {
        barrier->wait();
        running_routine();

    });
    barrier->wait();
    return true;
}

bool DataSource::stop() {
    std::unique_lock lock(startStopMutex);
    NES_DEBUG("DataSource " << this->getSourceId() << ": Stop called and source is "
                            << (running ? "running" : "not running"));
    if (!running) {
        NES_DEBUG("DataSource " << this->getSourceId() << " is not running")
        return false;
    }
    running = false;
    bool ret = false;
    try {
        if (thread) {
            NES_DEBUG("DataSource::stop try to join threads=" << thread->get_id())
            if (thread->joinable()) {
                NES_DEBUG("DataSource::stop thread is joinable=" << thread->get_id())
                thread->join();
                NES_DEBUG("DataSource: Thread joinded")
                ret = true;
                thread.reset();
            } else {
                NES_DEBUG(
                    "DataSource " << this->getSourceId() << ": Thread is not joinable")
                return false;
            }
        }
    }
    catch (...) {
        NES_ERROR("DataSource::stop error IN CATCH")
        auto expPtr = std::current_exception();
        try {
            if (expPtr) std::rethrow_exception(expPtr);
        }
        catch (const std::exception& e) //it would not work if you pass by value
        {
            NES_ERROR("DataSource::stop error while stopping data source " << this << " error=" << e.what())
        }
    }

    return ret;
}

bool DataSource::isRunning() {
    return running;
}

void DataSource::setGatheringInterval(size_t interval) {
    this->gatheringInterval = interval;
}

void DataSource::running_routine() {
    if (!this->sourceId.empty()) {
        NES_DEBUG("DataSource " << this->getSourceId() << ": Running Data Source")
        size_t cnt = 0;

        while (running) {
            size_t currentTime = time(NULL);
            if (gatheringInterval == 0
                || (lastGatheringTimeStamp != currentTime && currentTime%gatheringInterval == 0)) {  //produce a buffer
                lastGatheringTimeStamp = currentTime;
                if (cnt < numBuffersToProcess) {
                    auto optBuf = receiveData();
                    if (!!optBuf) {
                        auto& buf = optBuf.value();
                        NES_DEBUG(
                            "DataSource " << this->getSourceId() << " type=" << getType() << " string=" << toString()
                                          << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                          << " iteration="
                                          << cnt)
                        Dispatcher::instance().addWork(this->sourceId, buf);
                        cnt++;
                    }
                } else {
                    NES_DEBUG(
                        "DataSource " << this->getSourceId() << ": Receiving thread terminated ... stopping")
                    //                    return;//TODO: check if this really has to be done of if we just continue looping
                }
            } else {
                NES_DEBUG("DataSource::running_routine sleep " << this)
                sleep(gatheringInterval);
//                continue;
            }
            NES_DEBUG(
                "DataSource " << this->getSourceId() << ": Data Source finished processing iteration " << cnt)
        }//end of while running
    } else {
        NES_FATAL_ERROR(
            "DataSource " << this->getSourceId() << ": No ID assigned. Running_routine is not possible!")
        throw std::logic_error(
            "DataSource: No ID assigned. Running_routine is not possible!");
    }
    NES_DEBUG("DataSource::running_routine: exist routine " << this)
}

// debugging
void DataSource::setNumBuffersToProcess(size_t cnt) {
    numBuffersToProcess = cnt;
};
size_t DataSource::getNumberOfGeneratedTuples() {
    return generatedTuples;
};
size_t DataSource::getNumberOfGeneratedBuffers() {
    return generatedBuffers;
};

std::string DataSource::getSourceSchemaAsString() {
    return schema->toString();
}

const std::string& DataSource::getSourceId() const {
    return this->sourceId;
}

}  // namespace NES
