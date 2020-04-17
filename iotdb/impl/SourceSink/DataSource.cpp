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

#include <SourceSink/DataSource.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::DataSource);

namespace NES {

DataSource::DataSource(const SchemaPtr pSchema)
    :
    running(false),
    thread(),
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
    :
    running(false),
    thread(),
    generatedTuples(0),
    generatedBuffers(0),
    numBuffersToProcess(UINT64_MAX),
    gatheringInterval(0),
    lastGatheringTimeStamp(0),
    sourceId(UtilityFunctions::generateUuid()) {
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
    if (running)
        return false;
    running = true;

    NES_DEBUG("DataSource " << this->getSourceId() << ": Spawn thread")
    thread = std::make_shared<std::thread>([this]() {
        running_routine();
    });
    return true;
}

bool DataSource::stop() {
    NES_DEBUG("DataSource " << this->getSourceId() << ": Stop called")
    running = false;

    if (thread && thread->joinable()) {
        thread->join();
        NES_DEBUG("DataSource " << this->getSourceId() << ": Thread joinded")
        return true;
    } else {
        NES_DEBUG(
            "DataSource " << this->getSourceId() << ": Thread is not joinable")
    }
    thread.reset();
    return false;
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
            if (gatheringInterval == 0 || (lastGatheringTimeStamp != currentTime && currentTime%gatheringInterval == 0)) {  //produce a buffer
                lastGatheringTimeStamp = currentTime;
                if (cnt < numBuffersToProcess) {
                    auto optBuf = receiveData();
                    if (!!optBuf) {
                        auto& buf = optBuf.value();
                        NES_DEBUG(
                            "DataSource " << this->getSourceId() << " type=" << getType() << " string=" << toString()
                                          << ": Received Data: " << buf.getNumberOfTuples() << " tuples" << " iteration="
                                          << cnt)
                        Dispatcher::instance().addWork(this->sourceId, buf);
                        cnt++;
                    }
                } else {
                    NES_DEBUG(
                        "DataSource " << this->getSourceId() << ": Receiving thread terminated ... stopping")
                    running = false;
                    break;
                }

            } else {
                NES_DEBUG("DataSource::running_routine sleep")
                sleep(gatheringInterval);
                continue;
            }
            NES_DEBUG(
                "DataSource " << this->getSourceId() << ": Data Source finished processing iteration " << cnt)
        }
    } else {
        NES_FATAL_ERROR(
            "DataSource " << this->getSourceId() << ": No ID assigned. Running_routine is not possible!")
        throw std::logic_error(
            "DataSource: No ID assigned. Running_routine is not possible!");
    }
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
