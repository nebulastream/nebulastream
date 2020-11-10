#include <Sources/YSBSource.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <algorithm>
#include <atomic>
#include <bitset>
#include <cstring>
#include <iostream>
#include <memory>

namespace NES {
static const std::string events[] = {"view", "click", "purchase"};

struct __attribute__((packed)) ysbRecord {
    char user_id[16];
    char page_id[16];
    char campaign_id[16];
    char ad_type[9];
    char event_type[9];
    int64_t current_ms;
    uint32_t ip;

    ysbRecord() {
        event_type[0] = '-';// invalid record
        current_ms = 0;
        ip = 0;
    }

    ysbRecord(const ysbRecord& rhs) {
        memcpy(&user_id, &rhs.user_id, 16);
        memcpy(&page_id, &rhs.page_id, 16);
        memcpy(&campaign_id, &rhs.campaign_id, 16);
        memcpy(&ad_type, &rhs.ad_type, 9);
        memcpy(&event_type, &rhs.event_type, 9);
        current_ms = rhs.current_ms;
        ip = rhs.ip;
    }
};
// size 78 bytes

void generate(ysbRecord& rec) {
    strncpy(rec.user_id, "0", 16);
    strncpy(rec.page_id, "0", 16);
    strncpy(rec.campaign_id, "0", 16);
    strncpy(rec.ad_type, "banner78", 9);
    strncpy(rec.event_type, events[rand() % 3].c_str(), 9);

    auto ts = std::chrono::high_resolution_clock::now();
    rec.current_ms = ts.time_since_epoch().count();

    rec.ip = 0x01020304;
}

YSBSource::YSBSource(BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const uint64_t numbersOfBufferToProduce,
                     size_t frequency, size_t numberOfTuplesPerBuffer) : DefaultSource(YSB_SCHEMA, bufferManager, queryManager, numbersOfBufferToProduce, frequency),
                                                                         numberOfTuplesPerBuffer(numberOfTuplesPerBuffer) {}

std::optional<TupleBuffer> YSBSource::receiveData() {
    NES_DEBUG("YSBSource:" << this << " requesting buffer");
    auto buf = this->bufferManager->getBufferBlocking();

    NES_DEBUG("YSBSource:" << this << " got buffer");

    NES_DEBUG("YSBSource: Filling buffer with " << numberOfTuplesPerBuffer << " tuples.");
    for (uint64_t recordIndex = 0; recordIndex < numberOfTuplesPerBuffer; recordIndex++) {
        //generate tuple and copy record to buffer
        auto records = buf.getBufferAs<ysbRecord>();
        ysbRecord& rec = records[recordIndex];
        generate(rec);
    }
    buf.setNumberOfTuples(numberOfTuplesPerBuffer);
    NES_DEBUG("YSBSource: Generated buffer with " << buf.getNumberOfTuples() << "/" << schema->getSchemaSizeInBytes());
    buf.setWatermark(this->watermark->getWatermark());

    //update statistics
    generatedTuples += numberOfTuplesPerBuffer;
    generatedBuffers++;

    return buf;
}

SourceType YSBSource::getType() const {
    return YSB_SOURCE;
}

const std::string YSBSource::toString() const {
    std::stringstream ss;
    ss << "YSBSource(SCHEMA(" << schema->toString() << ")"
       << ")";
    return ss.str();
}

}// namespace NES
