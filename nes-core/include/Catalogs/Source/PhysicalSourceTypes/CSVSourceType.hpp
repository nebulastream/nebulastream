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

#ifndef NES_CSVSOURCETYPE_HPP
#define NES_CSVSOURCETYPE_HPP

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/rapidyaml.hpp>
#include <map>
#include <string>

namespace NES {

class CSVSourceType;
using CSVSourceTypePtr = std::shared_ptr<CSVSourceType>;

/**
 * @brief Configuration object for csv source config
 * define configurations for a csv source, i.e. this source reads from data from a csv file
 */
class CSVSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a CSVSourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return CSVSourceTypePtr
     */
    static CSVSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a CSVSourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return CSVSourceTypePtr
     */
    static CSVSourceTypePtr create(ryml::NodeRef yamlConfig);

    /**
     * @brief create a default CSVSourceTypePtr object
     * @return CSVSourceTypePtr
     */
    static CSVSourceTypePtr create();

    ~CSVSourceType() = default;

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
     * @brief Get file path, needed for: CSVSource, BinarySource
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getFilePath() const;

    /**
     * @brief Set file path, needed for: CSVSource, BinarySource
     */
    void setFilePath(std::string filePath);

    /**
     * @brief gets a ConfigurationOption object with skipHeader
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<bool>> getSkipHeader() const;

    /**
     * @brief set the value for skipHeader with the appropriate data format
     */
    void setSkipHeader(bool skipHeader);

    /**
     * @brief gets a ConfigurationOption object with skipHeader
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getDelimiter() const;

    /**
     * @brief set the value for skipHeader with the appropriate data format
     */
    void setDelimiter(std::string delimiter);

    /**
     * @brief gets a ConfigurationOption object with sourceFrequency
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getSourceFrequency() const;

    /**
     * @brief set the value for sourceFrequency with the appropriate data format
     */
    void setSourceFrequency(uint32_t sourceFrequencyValue);

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

    /**
     * @brief Get input data format
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getInputFormat() const;

    /**
     * @brief Set input data format
     */
    void setInputFormat(std::string inputFormat);

  private:
    /**
     * @brief constructor to create a new CSV source config object initialized with values from sourceConfigMap
     */
    explicit CSVSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new CSV source config object initialized with values from sourceConfigMap
     */
    explicit CSVSourceType(ryml::NodeRef yamlConfig);

    /**
     * @brief constructor to create a new CSV source config object initialized with default values
     */
    CSVSourceType();
    Configurations::StringConfigOption filePath;
    Configurations::BoolConfigOption skipHeader;
    Configurations::StringConfigOption delimiter;
    Configurations::IntConfigOption numberOfBuffersToProduce;
    Configurations::IntConfigOption numberOfTuplesToProducePerBuffer;
    Configurations::IntConfigOption sourceFrequency;
    Configurations::StringConfigOption inputFormat;
};

}// namespace NES
#endif