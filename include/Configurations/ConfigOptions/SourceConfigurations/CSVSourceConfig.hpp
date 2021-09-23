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

#include <Configurations/ConfigOptions/SourceConfigurations/SourceConfig.hpp>
#include <map>
#include <string>

namespace NES {

class CSVSourceConfig;
using CSVSourceConfigPtr = std::shared_ptr<CSVSourceConfig>;

template<class T>
class ConfigOption;
using FloatConfigOption = std::shared_ptr<ConfigOption<float>>;
using IntConfigOption = std::shared_ptr<ConfigOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigOption<bool>>;

/**
* @brief Configuration object for source config
*/

class CSVSourceConfig : public SourceConfig {

  public:
    void overwriteConfigWithYAMLFileInput(const std::string& filePath) override;
    void overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) override;
    void resetSourceOptions() override;
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
     * @brief constructor to create a new CSV source config object initialized with default values as set below
     */
    CSVSourceConfig();
    StringConfigOption filePath;
    BoolConfigOption skipHeader;

};
}// namespace NES
#endif