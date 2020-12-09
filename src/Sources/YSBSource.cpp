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

void YSBSource::generate(YSBSource::YsbRecord& rec, uint64_t ts, uint64_t eventType) {
    memset(&rec, 0, sizeof(YsbRecord));
    rec.userId = 1;
    rec.pageId = 0;
    rec.adType = 0;

    rec.campaignId = rand() % 10000;

    rec.eventType = eventType;

    rec.currentMs = ts;

    rec.ip = 0x01020304;
}

YSBSource::YSBSource(BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const uint64_t numbersOfBufferToProduce,
                     uint64_t numberOfTuplesPerBuffer, uint64_t frequency, OperatorId operatorId)
    : DefaultSource(YsbSchema(), bufferManager, queryManager, numbersOfBufferToProduce, frequency, operatorId),
      numberOfTuplesPerBuffer(numberOfTuplesPerBuffer), tmpEventType(0), currentMs(0) {}

std::optional<TupleBuffer> YSBSource::receiveData() {
    NES_DEBUG("YSBSource:" << this << " requesting buffer");
    auto buf = this->bufferManager->getBufferBlocking();
    NES_DEBUG("YSBSource:" << this << " got buffer");
    NES_DEBUG("YSBSource: start new current ms=" << currentMs);

    NES_DEBUG("YSBSource: Filling buffer with " << numberOfTuplesPerBuffer << " tuples.");
    auto records = buf.getBufferAs<YSBSource::YsbRecord>();
    for (uint64_t recordIndex = 0; recordIndex < numberOfTuplesPerBuffer; recordIndex++) {
        //generate tuple and copy record to buffer
        auto& rec = records[recordIndex];
        generate(rec, currentMs++, (currentEventType++) % 3);
    }
    buf.setNumberOfTuples(numberOfTuplesPerBuffer);
    NES_DEBUG("YSBSource: Generated buffer with " << buf.getNumberOfTuples() << "/" << schema->getSchemaSizeInBytes());

    //update statistics
    generatedTuples += numberOfTuplesPerBuffer;
    generatedBuffers++;

    NES_DEBUG("YSBSource::Buffer content: " << UtilityFunctions::prettyPrintTupleBuffer(buf, schema));

    return buf;
}

SourceType YSBSource::getType() const { return YSB_SOURCE; }

const std::string YSBSource::toString() const {
    std::stringstream ss;
    ss << "YSBSource(SCHEMA(" << schema->toString() << ")"
       << ")";
    return ss.str();
}

}// namespace NES
