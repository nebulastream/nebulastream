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

#ifndef NES_CSVSOURCECONFIG_HPP
#define NES_CSVSOURCECONFIG_HPP

#include <Configurations/Sources/SourceConfig.hpp>
#include <map>
#include <string>

namespace NES {

namespace Configurations {

class CSVSourceConfig;
using CSVSourceConfigPtr = std::shared_ptr<CSVSourceConfig>;

/**
 * @brief Configuration object for csv source config
 * define configurations for a csv source, i.e. this source reads from data from a csv file
 */
class CSVSourceConfig : public SourceConfig {

  public:
    /**
     * @brief create a CSVSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return CSVSourceConfigPtr
     */
    static CSVSourceConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a CSVSourceConfigPtr object
     * @return CSVSourceConfigPtr
     */
    static CSVSourceConfigPtr create();

    ~CSVSourceConfig() override = default;

    /**
     * @brief resets alls Source configuration to default values
     */
    void resetSourceOptions() override;
    /**
     * @brief creates a string representation of the source
     * @return
     */
    std::string toString() override;

    /**
     * @brief Get file path, needed for: CSVSource, BinarySource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getFilePath() const;

    /**
     * @brief Set file path, needed for: CSVSource, BinarySource
     */
    void setFilePath(std::string filePath);

    /**
     * @brief gets a ConfigOption object with skipHeader
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<bool>> getSkipHeader() const;

    /**
     * @brief set the value for skipHeader with the appropriate data format
     */
    void setSkipHeader(bool skipHeader);

  private:
    /**
     * @brief constructor to create a new CSV source config object initialized with values from sourceConfigMap
     */
    explicit CSVSourceConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new CSV source config object initialized with default values
     */
    CSVSourceConfig();
    StringConfigOption filePath;
    BoolConfigOption skipHeader;
};
}// namespace Configurations
}// namespace NES
#endif