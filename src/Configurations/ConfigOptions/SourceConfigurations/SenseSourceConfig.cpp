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
#include <Configurations/ConfigOptions/SourceConfigurations/SenseSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

std::shared_ptr<SenseSourceConfig> SenseSourceConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<SenseSourceConfig>(SenseSourceConfig(std::move(sourceConfigMap)));
}

std::shared_ptr<SenseSourceConfig> SenseSourceConfig::create() {
    return std::make_shared<SenseSourceConfig>(SenseSourceConfig());
}

SenseSourceConfig::SenseSourceConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceConfig(std::move(sourceConfigMap)),
      udsf(ConfigOption<std::string>::create("udsf", "", "udsf, needed for: SenseSource")) {
    NES_INFO("SenseSourceConfig: Init source config object with values from sourceConfigMap.");
    if (sourceConfigMap.find("udsf") != sourceConfigMap.end()) {
        udsf->setValue(sourceConfigMap.find("udsf")->second);
    }
}

SenseSourceConfig::SenseSourceConfig()
    : SourceConfig(),
      udsf(ConfigOption<std::string>::create("udsf", "", "udsf, needed for: SenseSource")) {
    NES_INFO("SenseSourceConfig: Init source config object with default values.");

}

void SenseSourceConfig::resetSourceOptions() {
    setUdsf(udsf->getDefaultValue());
    SourceConfig::resetSourceOptions();
}

std::string SenseSourceConfig::toString() {
    std::stringstream ss;
    ss << udsf->toStringNameCurrentValue();
    ss << SourceConfig::toString();
    return ss.str();
}

StringConfigOption SenseSourceConfig::getUdsf() const { return udsf; }

void SenseSourceConfig::setUdsf(std::string udsfValue) { udsf->setValue(udsfValue); }

}// namespace NES