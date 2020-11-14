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

    static SchemaPtr YSB_SCHEMA();

  public:
    struct __attribute__((packed)) ysbRecord {
        uint16_t user_id;
        uint16_t page_id;
        uint16_t campaign_id;
        uint16_t ad_type;
        uint16_t event_type;
        int64_t current_ms;
        uint32_t ip;

        ysbRecord(const ysbRecord& rhs) {
            user_id = rhs.user_id;
            page_id = rhs.page_id;
            campaign_id = rhs.campaign_id;
            ad_type = rhs.ad_type;
            event_type = rhs.event_type;
            current_ms = rhs.current_ms;
            ip = rhs.ip;
        }
    };
    // size 78 bytes

  private:
    size_t numberOfTuplesPerBuffer;
    bool endlessRepeat;
};

typedef std::shared_ptr<YSBSource> YSBSourcePtr;
};// namespace NES

#endif//NES_INCLUDE_SOURCES_YSBSOURCE_HPP_
