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
#include <Configurations/ConfigOptions/SourceConfigurations/CSVSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {
CSVSourceConfigPtr CSVSourceConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<CSVSourceConfig>(CSVSourceConfig(std::move(sourceConfigMap)));
}

CSVSourceConfigPtr CSVSourceConfig::create() {
    return std::make_shared<CSVSourceConfig>(CSVSourceConfig());
}

CSVSourceConfig::CSVSourceConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceConfig(sourceConfigMap),
      filePath(ConfigOption<std::string>::create("filePath",
                                                 "../tests/test_data/QnV_short.csv",
                                                 "file path, needed for: CSVSource, BinarySource")),
      skipHeader(ConfigOption<bool>::create("skipHeader", false, "Skip first line of the file.")) {
    NES_INFO("CSVSourceConfig: Init source config object.");
    if (sourceConfigMap.find("CSVSourceFilePath") != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find("CSVSourceFilePath")->second);
    }
    if (sourceConfigMap.find("CSVSourceSkipHeader") != sourceConfigMap.end()) {
        skipHeader->setValue((sourceConfigMap.find("CSVSourceSkipHeader")->second == "true"));
    }
}

CSVSourceConfig::CSVSourceConfig()
    : SourceConfig(),
      filePath(ConfigOption<std::string>::create("filePath",
                                                 "../tests/test_data/QnV_short.csv",
                                                 "file path, needed for: CSVSource, BinarySource")),
      skipHeader(ConfigOption<bool>::create("skipHeader", false, "Skip first line of the file.")) {
    NES_INFO("CSVSourceConfig: Init source config object with default values.");
}

void CSVSourceConfig::resetSourceOptions() {
    setFilePath(filePath->getDefaultValue());
    setSkipHeader(skipHeader->getDefaultValue());
    SourceConfig::resetSourceOptions();
}

std::string CSVSourceConfig::toString() {
    std::stringstream ss;
    ss << filePath->toStringNameCurrentValue();
    ss << skipHeader->toStringNameCurrentValue();
    ss << SourceConfig::toString();
    return ss.str();
}

StringConfigOption CSVSourceConfig::getFilePath() const { return filePath; }

BoolConfigOption CSVSourceConfig::getSkipHeader() const { return skipHeader; }

void CSVSourceConfig::setSkipHeader(bool skipHeaderValue) { skipHeader->setValue(skipHeaderValue); }

void CSVSourceConfig::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

}// namespace NES