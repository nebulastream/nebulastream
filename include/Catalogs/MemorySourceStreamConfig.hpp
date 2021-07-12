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
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Sources/MemorySource.hpp>

namespace NES {

/**
 * @brief A stream config for a memory source
 */
class MemorySourceStreamConfig : public PhysicalStreamConfig {
  public:
    /**
     * @brief Create a MemorySourceStreamConfig using a set of parameters
     * @param sourceType the type of the source
     * @param physicalStreamName the name of the physical stream
     * @param logicalStreamName the name of the logical stream
     * @param memoryArea the pointer to the memory area
     * @param memoryAreaSize the size of the memory area
     */
    explicit MemorySourceStreamConfig(std::string sourceType,
                                      std::string physicalStreamName,
                                      std::string logicalStreamName,
                                      uint8_t* memoryArea,
                                      size_t memoryAreaSize,
                                      uint64_t numBuffersToProcess,
                                      uint64_t gatheringValue,
                                      const std::string& gatheringMode,
                                      const std::string& sourceMode);

    ~MemorySourceStreamConfig() noexcept override = default;

    /**
     * @brief Creates the source descriptor for the underlying source
     * @param ptr the schama to build the source with
     * @return
     */
    SourceDescriptorPtr build(SchemaPtr) override;

    /**
     * @brief The string representation of the object
     * @return the string representation of the object
     */
    std::string toString() override;

    /**
    * @brief The source type as a string
    * @return The source type as a string
    */
    std::string getSourceType() override;

    /**
     * @brief Provides the physical stream name of the source
     * @return the physical stream name of the source
     */
    std::string getPhysicalStreamName() override;

    /**
     * @brief Provides the logical stream name of the source
     * @return the logical stream name of the source
     */
    std::string getLogicalStreamName() override;

    /**
     * @brief Factory method of MemorySourceStreamConfig
      * @param sourceType the type of the source
     * @param physicalStreamName the name of the physical stream
     * @param logicalStreamName the name of the logical stream
     * @param memoryArea the pointer to the memory area
     * @param memoryAreaSize the size of the memory area
     * @param sourceMode how the memroy source create the content, either by wrapping or by copy buffer
     * @return a constructed MemorySourceStreamConfig
     */
    static AbstractPhysicalStreamConfigPtr create(const std::string& sourceType,
                                                  const std::string& physicalStreamName,
                                                  const std::string& logicalStreamName,
                                                  uint8_t* memoryArea,
                                                  size_t memoryAreaSize,
                                                  uint64_t numBuffersToProcess,
                                                  uint64_t gatheringValue,
                                                  const std::string& gatheringMode,
                                                  const std::string& sourceMode);

  private:
    std::string sourceType;
    std::shared_ptr<uint8_t> memoryArea;
    const size_t memoryAreaSize;
    uint64_t gatheringValue;
    DataSource::GatheringMode gatheringMode;
    MemorySource::SourceMode sourceMode;
};

}// namespace NES

#endif//NES_INCLUDE_CATALOGS_MEMORYSOURCESTREAMCONFIG_HPP_
