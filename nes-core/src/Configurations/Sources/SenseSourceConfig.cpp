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
#include <Configurations/Sources/SenseSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

std::shared_ptr<SenseSourceConfig> SenseSourceConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<SenseSourceConfig>(SenseSourceConfig(std::move(sourceConfigMap)));
}

std::shared_ptr<SenseSourceConfig> SenseSourceConfig::create() {
    return std::make_shared<SenseSourceConfig>(SenseSourceConfig());
}

SenseSourceConfig::SenseSourceConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceConfig(sourceConfigMap, SENSE_SOURCE_CONFIG),
      udfs(ConfigOption<std::string>::create(UDFS_CONFIG, "", "udfs, needed for: SenseSource")) {
    NES_INFO("SenseSourceConfig: Init source config object with values from sourceConfigMap.");
    /*if (sourceConfigMap.find(SENSE_SOURCE_UDFS_CONFIG) != sourceConfigMap.end()) {
        udfs->setValue(sourceConfigMap.find(SENSE_SOURCE_UDFS_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("OPCSourceConfig:: no udfs defined! Please define a udfs.");
    }*/
}

SenseSourceConfig::SenseSourceConfig()
    : SourceConfig(SENSE_SOURCE_CONFIG),
      udfs(ConfigOption<std::string>::create(UDFS_CONFIG, "", "udfs, needed for: SenseSource")) {
    NES_INFO("SenseSourceConfig: Init source config object with default values.");
}

void SenseSourceConfig::resetSourceOptions() {
    setUdfs(udfs->getDefaultValue());
    SourceConfig::resetSourceOptions(SENSE_SOURCE_CONFIG);
}

std::string SenseSourceConfig::toString() {
    std::stringstream ss;
    ss << udfs->toStringNameCurrentValue();
    ss << SourceConfig::toString();
    return ss.str();
}

bool SenseSourceConfig::equal(const SourceConfigPtr& other) {
    if (!other->instanceOf<SenseSourceConfig>()) {
        return false;
    }
    auto otherSourceConfig = other->as<SenseSourceConfig>();
    return SourceConfig::equal(other) && udfs->getValue() == otherSourceConfig->udfs->getValue();
}

StringConfigOption SenseSourceConfig::getUdfs() const { return udfs; }

void SenseSourceConfig::setUdfs(std::string udfsValue) { udfs->setValue(udfsValue); }

}// namespace Configurations
}// namespace NES