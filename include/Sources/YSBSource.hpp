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
            ->addField("user_id", UINT16)
            ->addField("page_id", UINT16)
            ->addField("campaign_id", UINT16)
            ->addField("ad_type", UINT16)
            ->addField("event_type", UINT16)
            ->addField("current_ms", UINT64)
            ->addField("ip", INT32);
    };

  public:
    struct __attribute__((packed)) YsbRecord {
        YsbRecord() = default;
        YsbRecord(uint16_t userId, uint16_t pageId, uint16_t campaignId, uint16_t adType, uint16_t eventType, uint64_t currentMs, uint32_t ip):
        userId(userId), pageId(pageId), campaignId(campaignId), adType(adType), eventType(eventType), currentMs(currentMs), ip(ip){}

        uint16_t userId;
        uint16_t pageId;
        uint16_t campaignId;
        uint16_t adType;
        uint16_t eventType;
        uint64_t currentMs;
        uint32_t ip;

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

  private:
    void generate(YSBSource::YsbRecord& rec);

  private:
    size_t numberOfTuplesPerBuffer;
    bool endlessRepeat;
    uint8_t tmpEventType;
};

typedef std::shared_ptr<YSBSource> YSBSourcePtr;
};// namespace NES

#endif//NES_INCLUDE_SOURCES_YSBSOURCE_HPP_
