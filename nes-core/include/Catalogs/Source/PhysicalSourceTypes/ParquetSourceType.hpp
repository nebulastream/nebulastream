/*
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
#ifdef ENABLE_PARQUET_BUILD
#ifndef NES_NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_PARQUETSOURCETYPE_HPP_
#define NES_NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_PARQUETSOURCETYPE_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>

namespace NES{

class ParquetSourceType;
using ParquetSourceTypePtr = std::shared_ptr<ParquetSourceType>;


class ParquetSourceType : public PhysicalSourceType {
  public:
    ~ParquetSourceType() noexcept override = default;

    static ParquetSourceTypePtr create(std::map<std::string,std::string> sourceConfig);

    /**
     * @brief creates a string representation of the source
     * @return
     */
    std::string toString() override;

    /**
     * Checks equality
     * @param other mqttSourceType ot check equality for
     * @return true if equal, false otherwise
     */
    bool equal(PhysicalSourceTypePtr const& other) override;

    void reset() override;

    /**
     * @brief Set file path, needed for: CSVSource, BinarySource
     */
    void setFilePath(std::string filePath);

    /**
     * @brief Get file path, needed for: CSVSource, BinarySource
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getFilePath() const;

    /**
     * @brief gets a ConfigurationOption object with numberOfBuffersToProduce
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getNumberOfBuffersToProduce() const;

    /**
     * @brief set the value for numberOfBuffersToProduce with the appropriate data format
     */
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    /**
     * @brief gets a ConfigurationOption object with numberOfTuplesToProducePerBuffer
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief set the value for numberOfTuplesToProducePerBuffer with the appropriate data format
     */
    void setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBuffer);

  private:
    /**
     * @brief constructor to create a new Parquet source config object initialized with values from sourceConfigMap
     */
    explicit ParquetSourceType(std::map<std::string, std::string> sourceConfigMap);

    ParquetSourceType();

    Configurations::StringConfigOption filePath;
    Configurations::IntConfigOption numberOfBuffersToProduce;
    Configurations::IntConfigOption numberOfTuplesToProducePerBuffer;

};
}//namespace NES
#endif//NES_NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_PARQUETSOURCETYPE_HPP_
#endif
