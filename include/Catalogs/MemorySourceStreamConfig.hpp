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
#ifndef NES_INCLUDE_CATALOGS_MEMORYSOURCESTREAMCONFIG_HPP_
#define NES_INCLUDE_CATALOGS_MEMORYSOURCESTREAMCONFIG_HPP_

#include <Catalogs/AbstractPhysicalStreamConfig.hpp>


namespace NES {

class MemorySourceStreamConfig : public AbstractPhysicalStreamConfig {
  public:
    explicit MemorySourceStreamConfig(std::string sourceType, std::string physicalStreamName,
                                      std::string logicalStreamName, uint8_t* memoryArea, size_t memoryAreaSize);

    SourceDescriptorPtr build(SchemaPtr ptr) override;
    const std::string toString() override;
    const std::string getSourceType() override;
    const std::string getPhysicalStreamName();
    const std::string getLogicalStreamName();

    static AbstractPhysicalStreamConfigPtr create(std::string sourceType, std::string physicalStreamName, std::string logicalStreamName, uint8_t* memoryArea, size_t memoryAreaSize);

  private:

    std::string sourceType;
    std::string physicalStreamName;
    std::string logicalStreamName;
    std::shared_ptr<uint8_t> memoryArea;
    const size_t memoryAreaSize;
};

}

#endif//NES_INCLUDE_CATALOGS_MEMORYSOURCESTREAMCONFIG_HPP_
