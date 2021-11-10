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

#include <Configurations/ConfigOption.hpp>
#include <Configurations/Sources/BinarySourceConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

BinarySourceConfigPtr BinarySourceConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<BinarySourceConfig>(BinarySourceConfig(std::move(sourceConfigMap)));
}

BinarySourceConfigPtr BinarySourceConfig::create() { return std::make_shared<BinarySourceConfig>(BinarySourceConfig()); }

BinarySourceConfig::BinarySourceConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceConfig(sourceConfigMap, BINARY_SOURCE_CONFIG),
      filePath(ConfigOption<std::string>::create(FILE_PATH_CONFIG, "", "file path, needed for: CSVSource, BinarySource")) {
    NES_INFO("BinarySourceConfig: Init source config object.");
    if (sourceConfigMap.find(BINARY_SOURCE_FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find(BINARY_SOURCE_FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("BinarySourceConfig:: no filePath defined! Please define a filePath.");
    }
}

BinarySourceConfig::BinarySourceConfig()
    : SourceConfig(BINARY_SOURCE_CONFIG),
      filePath(ConfigOption<std::string>::create(FILE_PATH_CONFIG,
                                                 "../tests/test_data/QnV_short.csv",
                                                 "file path, needed for: CSVSource, BinarySource")) {
    NES_INFO("BinarySourceConfig: Init source config object with default params.");
}

void BinarySourceConfig::resetSourceOptions() {
    setFilePath(filePath->getDefaultValue());
    SourceConfig::resetSourceOptions(BINARY_SOURCE_CONFIG);
}

std::string BinarySourceConfig::toString() {
    std::stringstream ss;
    ss << filePath->toStringNameCurrentValue();
    ss << SourceConfig::toString();
    return ss.str();
}

StringConfigOption BinarySourceConfig::getFilePath() const { return filePath; }

void BinarySourceConfig::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

}// namespace Configurations
}// namespace NES