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

#ifndef NES_BINARYSOURCECONFIG_HPP
#define NES_BINARYSOURCECONFIG_HPP

#include <Configurations/Sources/SourceConfig.hpp>
#include <map>
#include <string>

namespace NES {

namespace Configurations {

class BinarySourceConfig;
using BinarySourceConfigPtr = std::shared_ptr<BinarySourceConfig>;

/**
 * @brief Configuration object for binary source
 * A binary source reads data from a binary file
 */
class BinarySourceConfig : public SourceConfig {

  public:
    /**
     * @brief create a BinarySourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return BinarySourceConfigPtr
     */
    static BinarySourceConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a BinarySourceConfigPtr object with default values
     * @return BinarySourceConfigPtr
     */
    static BinarySourceConfigPtr create();

    ~BinarySourceConfig() override = default;

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
     * Checks equality
     * @param other sourceConfig ot check equality for
     * @return true if equal, false otherwise
     */
    bool equal(SourceConfigPtr const& other) override;

    /**
     * @brief Get file path
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getFilePath() const;

    /**
     * @brief Set file path
     */
    void setFilePath(std::string filePath);

  private:
    /**
     * @brief constructor to create a new Binary source config object initialized with values from sourceConfigMap
     */
    explicit BinarySourceConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Binary source config object initialized with default values as set below
     */
    BinarySourceConfig();

    StringConfigOption filePath;
};
}// namespace Configurations
}// namespace NES
#endif