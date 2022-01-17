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

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/BinarySourceTypeConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

BinarySourceTypeConfigPtr BinarySourceTypeConfig::create(ryml::NodeRef sourcTypeConfig) {
    return std::make_shared<BinarySourceTypeConfig>(BinarySourceTypeConfig(sourcTypeConfig));
}

BinarySourceTypeConfig::BinarySourceTypeConfig(ryml::NodeRef sourcTypeConfig)
    : SourceTypeConfig(BINARY_SOURCE_CONFIG),
      filePath(ConfigurationOption<std::string>::create(FILE_PATH_CONFIG, "", "file path, needed for: CSVSource, BinarySource")) {
    NES_INFO("BinarySourceTypeConfig: Init source config object.");
    if (sourcTypeConfig.find_child(ryml::to_csubstr (FILE_PATH_CONFIG)).has_val()) {
        filePath->setValue(sourcTypeConfig.find_child(ryml::to_csubstr (FILE_PATH_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("BinarySourceTypeConfig:: no filePath defined! Please define a filePath.");
    }
}

BinarySourceTypeConfigPtr BinarySourceTypeConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<BinarySourceTypeConfig>(BinarySourceTypeConfig(std::move(sourceConfigMap)));
}

BinarySourceTypeConfig::BinarySourceTypeConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceTypeConfig(BINARY_SOURCE_CONFIG),
      filePath(ConfigurationOption<std::string>::create(FILE_PATH_CONFIG, "", "file path, needed for: CSVSource, BinarySource")) {
    NES_INFO("BinarySourceConfig: Init source config object.");
    if (sourceConfigMap.find(FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find(FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("BinarySourceConfig:: no filePath defined! Please define a filePath.");
    }
}

BinarySourceTypeConfigPtr BinarySourceTypeConfig::create() { return std::make_shared<BinarySourceTypeConfig>(BinarySourceTypeConfig()); }

BinarySourceTypeConfig::BinarySourceTypeConfig()
    : SourceTypeConfig(BINARY_SOURCE_CONFIG),
      filePath(ConfigurationOption<std::string>::create(FILE_PATH_CONFIG,
                                                 "../tests/test_data/QnV_short.csv",
                                                 "file path, needed for: CSVSource, BinarySource")) {
    NES_INFO("BinarySourceTypeConfig: Init source config object with default params.");
}

void BinarySourceTypeConfig::resetSourceOptions() {
    setFilePath(filePath->getDefaultValue());
    SourceTypeConfig::resetSourceOptions(BINARY_SOURCE_CONFIG);
}

std::string BinarySourceTypeConfig::toString() {
    std::stringstream ss;
    ss << FILE_PATH_CONFIG + ":" +  filePath->toStringNameCurrentValue();
    ss << SourceTypeConfig::toString();
    return ss.str();
}

bool BinarySourceTypeConfig::equal(const SourceTypeConfigPtr& other) {
    if (!other->instanceOf<BinarySourceTypeConfig>()) {
        return false;
    }
    auto otherSourceConfig = other->as<BinarySourceTypeConfig>();
    return filePath->getValue() == otherSourceConfig->filePath->getValue() && SourceTypeConfig::equal(other);
}

StringConfigOption BinarySourceTypeConfig::getFilePath() const { return filePath; }

void BinarySourceTypeConfig::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

}// namespace Configurations
}// namespace NES