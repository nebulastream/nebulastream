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

#ifndef NES_INCLUDE_SOURCES_YSBSOURCE_HPP_
#define NES_INCLUDE_SOURCES_YSBSOURCE_HPP_

#include <Sources/DefaultSource.hpp>

namespace NES {

class YSBSource : public DefaultSource {
  public:
    explicit YSBSource(BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const uint64_t numbersOfBufferToProduce, size_t numberOfTuplesPerBuffer, size_t frequency, bool endlessRepeat,
                       OperatorId operatorId);

    SourceType getType() const override;

    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the binary source
     * @return returns string describing the binary source
     */
    const std::string toString() const override;

    static SchemaPtr YsbSchema() {
        return Schema::create()
            ->addField("user_id", UINT64)
            ->addField("page_id", UINT64)
            ->addField("campaign_id", UINT64)
            ->addField("ad_type", UINT64)
            ->addField("event_type", UINT64)
            ->addField("current_ms", UINT64)
            ->addField("ip", UINT64)

            ->addField("d1", UINT64)
            ->addField("d2", UINT64)
            ->addField("d3", UINT32)
            ->addField("d4", UINT16);
    };

  public:
    struct __attribute__((packed)) YsbRecord {
        YsbRecord() = default;
        YsbRecord(uint64_t userId, uint64_t pageId, uint64_t campaignId, uint64_t adType, uint64_t eventType, uint64_t currentMs, uint64_t ip) : userId(userId), pageId(pageId), campaignId(campaignId), adType(adType), eventType(eventType), currentMs(currentMs), ip(ip) {}

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
            return "YsbRecord(userId=" + std::to_string(userId)
                + ", pageId=" + std::to_string(pageId)
                + ", campaignId=" + std::to_string(campaignId)
                + ", adType=" + std::to_string(adType)
                + ", eventType=" + std::to_string(eventType)
                + ", currentMs=" + std::to_string(currentMs)
                + ", ip=" + std::to_string(ip);
        }
    };
    static_assert(sizeof(YsbRecord) == 78, "YSBSource: The record must be 78 bytes.");
    // 78 bytes

  private:
    void generate(YSBSource::YsbRecord& rec);

  private:
    size_t numberOfTuplesPerBuffer;
    bool endlessRepeat;
    uint64_t tmpEventType;
    uint64_t currentMs;
};

typedef std::shared_ptr<YSBSource> YSBSourcePtr;
};// namespace NES

#endif//NES_INCLUDE_SOURCES_YSBSOURCE_HPP_
