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

#ifndef NES_BENCHMARK_SRC_UTIL_BENCHMARKSOURCEYSB_HPP_
#define NES_BENCHMARK_SRC_UTIL_BENCHMARKSOURCEYSB_HPP_

#include "SimpleBenchmarkSource.hpp"
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <cstdint>
#include <list>
#include <memory>

namespace NES::Benchmarking {
/**
 * @brief A benchmark source that will output data (key, value) with a predefined selectivity
 * Key is an uniform distribution from 0 to 999
 * Value is always 1
 * Selectivity can be set
 */
class YSBBenchmarkSource : public SimpleBenchmarkSource {
  public:
    YSBBenchmarkSource(const SchemaPtr& schema, const NodeEngine::BufferManagerPtr& bufferManager, const NodeEngine::QueryManagerPtr& queryManager,
                       uint64_t ingestionRate, uint64_t numberOfTuplesPerBuffer, uint64_t operatorId)
        : SimpleBenchmarkSource(schema, bufferManager, queryManager, ingestionRate, numberOfTuplesPerBuffer, operatorId) {}

    static std::shared_ptr<YSBBenchmarkSource> create(NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                                                      SchemaPtr& benchmarkSchema, uint64_t ingestionRate, uint64_t operatorId) {

        auto maxTuplesPerBuffer = bufferManager->getBufferSize() / benchmarkSchema->getSchemaSizeInBytes();
        //        maxTuplesPerBuffer = maxTuplesPerBuffer % 1000 >= 500 ? (maxTuplesPerBuffer + 1000 - maxTuplesPerBuffer % 1000)
        //                                                              : (maxTuplesPerBuffer - maxTuplesPerBuffer % 1000);

        NES_INFO("BM_YSB_QUERY: maxTuplesPerBuffer=" << maxTuplesPerBuffer);
        // at this point maxTuplesPerBuffer will be rounded to nearest thousands. This makes it easier to work with ingestion rates
        if (maxTuplesPerBuffer == 0) {
            throw RuntimeException("maxTuplesPerBuffer == 0");
        }

        return std::make_shared<YSBBenchmarkSource>(benchmarkSchema, bufferManager, queryManager, ingestionRate,
                                                    maxTuplesPerBuffer, operatorId);
    }

    struct __attribute__((packed)) YsbRecord {
        YsbRecord() = default;
        YsbRecord(uint64_t userId, uint64_t pageId, uint64_t campaignId, uint64_t adType, uint64_t eventType, uint64_t currentMs,
                  uint64_t ip)
            : userId(userId), pageId(pageId), campaignId(campaignId), adType(adType), eventType(eventType), currentMs(currentMs),
              ip(ip) {}

        uint64_t userId;
        uint64_t pageId;
        uint64_t campaignId;
        uint64_t adType;
        uint64_t eventType;
        uint64_t currentMs;
        uint64_t ip;

        // placeholder to reach 78 bytes
        uint64_t dummy1{0};
        uint64_t dummy2{0};
        uint32_t dummy3{0};
        uint16_t dummy4{0};

        YsbRecord(const YsbRecord& rhs) {
            userId = rhs.userId;
            pageId = rhs.pageId;
            campaignId = rhs.campaignId;
            adType = rhs.adType;
            eventType = rhs.eventType;
            currentMs = rhs.currentMs;
            ip = rhs.ip;
        }

        std::string toString() const {
            return "YsbRecord(userId=" + std::to_string(userId) + ", pageId=" + std::to_string(pageId) + ", campaignId="
                + std::to_string(campaignId) + ", adType=" + std::to_string(adType) + ", eventType=" + std::to_string(eventType)
                + ", currentMs=" + std::to_string(currentMs) + ", ip=" + std::to_string(ip);
        }
    };

    void generate(YsbRecord& rec, uint64_t ts, uint64_t eventType) {
        memset(&rec, 0, sizeof(YsbRecord));
        rec.userId = 1;
        rec.pageId = 0;
        rec.adType = 0;

        rec.campaignId = rand() % 10000;

        rec.eventType = eventType;

        rec.currentMs = ts;

        rec.ip = 0x01020304;
    }
    std::optional<NodeEngine::TupleBuffer> receiveData() override {
        NES_DEBUG("YSBSource:" << this << " requesting buffer");
        auto buf = this->bufferManager->getBufferBlocking();
        NES_DEBUG("YSBSource:" << this << " got buffer");
        uint64_t currentMs = 0;
        uint64_t currentEventType = 0;

        NES_DEBUG("YSBSource: start new current ms=" << currentMs);

        NES_DEBUG("YSBSource: Filling buffer with " << numberOfTuplesPerBuffer << " tuples.");
        auto records = buf.getBufferAs<YsbRecord>();
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
};
}// namespace NES::Benchmarking
#endif//NES_BENCHMARK_SRC_UTIL_BENCHMARKSOURCE_HPP_
