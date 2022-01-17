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

#ifndef NES_INCLUDE_CATALOGS_BENCHMARK_SOURCE_STREAM_CONFIG_HPP_
#define NES_INCLUDE_CATALOGS_BENCHMARK_SOURCE_STREAM_CONFIG_HPP_

#include <Catalogs/AbstractPhysicalStreamConfig.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Sources/BenchmarkSource.hpp>

namespace NES {

/**
 * @brief A stream config for a benchm source
 */
class BenchmarkSourceStreamConfig : public PhysicalStreamConfig {
  public:
    /**
     * @brief Create a BenchmarkSourceStreamConfig using a set of parameters
     * @param sourceType the type of the source
     * @param physicalStreamName the name of the physical stream
     * @param logicalStreamName the name of the logical stream
     * @param memoryArea the pointer to the memory area
     * @param memoryAreaSize the size of the memory area
     * @param source mode on how to bring data into the buffer
     * @param sourceAffinity id of cpu where to pin the source
     */
    explicit BenchmarkSourceStreamConfig(const Configurations::PhysicalStreamTypeConfigurationPtr physicalStreamTypeConfig,
                                         uint8_t* memoryArea,
                                         size_t memoryAreaSize,
                                         uint64_t gatheringValue,
                                         const std::string& gatheringMode,
                                         const std::string& sourceMode,
                                         uint64_t sourceAffinity);

    ~BenchmarkSourceStreamConfig() noexcept override = default;

    /**
     * @brief Creates the source descriptor for the underlying source
     * @param ptr the schema to build the source with
     * @return
     */
    SourceDescriptorPtr build(SchemaPtr) override;

    /**
     * @brief The string representation of the object
     * @return the string representation of the object
     */
    std::string toString() override;

    /**
     * @brief The physicalStreamTypeConfiguration Object with all needed configurations
     * @return The physicalStreamTypeConfiguration
     */
    virtual Configurations::PhysicalStreamTypeConfigurationPtr getPhysicalStreamTypeConfig() override;

    /**
     * @brief Factory method of BenchmarkSourceStreamConfig
      * @param sourceType the type of the source
     * @param physicalStreamName the name of the physical stream
     * @param logicalStreamName the name of the logical stream
     * @param memoryArea the pointer to the memory area
     * @param memoryAreaSize the size of the memory area
     * @param sourceMode how the benchmark source create the content, either by wrapping or by copy buffer
     * @param sourceAffinity id of cpu where to pin the source
     * @return a constructed BenchmarkSourceStreamConfig
     */
    static AbstractPhysicalStreamConfigPtr create(const Configurations::PhysicalStreamTypeConfigurationPtr physicalStreamTypeConfig,
                                                  uint8_t* memoryArea,
                                                  size_t memoryAreaSize,
                                                  uint64_t gatheringValue,
                                                  const std::string& gatheringMode,
                                                  const std::string& sourceMode,
                                                  uint64_t sourceAffinity = std::numeric_limits<uint64_t>::max());

    static BenchmarkSource::SourceMode getSourceModeFromString(const std::string& mode);

  private:
    Configurations::PhysicalStreamTypeConfigurationPtr physicalStreamTypeConfig;
    std::shared_ptr<uint8_t> memoryArea;
    const size_t memoryAreaSize;
    uint64_t gatheringValue;
    DataSource::GatheringMode gatheringMode;
    BenchmarkSource::SourceMode sourceMode;
    uint64_t sourceAffinity;
};
}// namespace NES
#endif// NES_INCLUDE_CATALOGS_BENCHMARK_SOURCE_STREAM_CONFIG_HPP_
