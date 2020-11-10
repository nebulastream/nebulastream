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

// 78 bytes
static const auto YSB_SCHEMA = Schema::create()
    ->addField("user_id", DataTypeFactory::createFixedChar(16))
    ->addField("page_id", DataTypeFactory::createFixedChar(16))
    ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
    ->addField("ad_type", DataTypeFactory::createFixedChar(9))
    ->addField("event_type", DataTypeFactory::createFixedChar(9))
    ->addField("current_ms", UINT64)
    ->addField("ip", INT32);


class YSBSource : public DefaultSource {
  public:
    explicit YSBSource(BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const uint64_t numbersOfBufferToProduce, size_t frequency, size_t numberOfTuplesPerBuffer);

    SourceType getType() const override;

    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the binary source
     * @return returns string describing the binary source
     */
    const std::string toString() const override;

  private:
    size_t numberOfTuplesPerBuffer;
};

typedef std::shared_ptr<YSBSource> YSBSourcePtr;
};// namespace NES

#endif//NES_INCLUDE_SOURCES_YSBSOURCE_HPP_
