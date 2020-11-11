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

SchemaPtr YSBSource::YSB_SCHEMA() {
    return Schema::create()
        ->addField("user_id", UINT16)
        ->addField("page_id", UINT16)
        ->addField("campaign_id", UINT16)
        ->addField("ad_type", UINT16)
        ->addField("event_type", UINT16)
        ->addField("current_ms", UINT64)
        ->addField("ip", INT32);
}

void generate(YSBSource::ysbRecord& rec) {
    rec.user_id = 0;
    rec.page_id = 0;
    rec.ad_type = 0;

    rec.campaign_id = rand() % 10000;
    rec.event_type = rand() % 3;

    auto ts = std::chrono::high_resolution_clock::now();
    rec.current_ms = ts.time_since_epoch().count();

    rec.ip = 0x01020304;
}

YSBSource::YSBSource(BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const uint64_t numbersOfBufferToProduce,
                     size_t numberOfTuplesPerBuffer, size_t frequency, bool endlessRepeat)
    : DefaultSource(YSB_SCHEMA(), bufferManager, queryManager, numbersOfBufferToProduce, frequency),
      numberOfTuplesPerBuffer(numberOfTuplesPerBuffer),
      endlessRepeat(endlessRepeat) {}

std::optional<TupleBuffer> YSBSource::receiveData() {
    NES_DEBUG("YSBSource:" << this << " requesting buffer");
    auto buf = this->bufferManager->getBufferBlocking();

    NES_DEBUG("YSBSource:" << this << " got buffer");

    NES_DEBUG("YSBSource: Filling buffer with " << numberOfTuplesPerBuffer << " tuples.");
    for (uint64_t recordIndex = 0; recordIndex < numberOfTuplesPerBuffer; recordIndex++) {
        //generate tuple and copy record to buffer
        auto records = buf.getBufferAs<YSBSource::ysbRecord>();
        auto& rec = records[recordIndex];
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
