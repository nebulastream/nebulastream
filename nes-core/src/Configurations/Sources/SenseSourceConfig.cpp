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
#include <Configurations/Worker/PhysicalStreamConfig/SenseSourceTypeConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

SenseSourceTypeConfigPtr SenseSourceTypeConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<SenseSourceTypeConfig>(SenseSourceTypeConfig(std::move(sourceConfigMap)));
}

SenseSourceTypeConfigPtr SenseSourceTypeConfig::create(ryml::NodeRef sourcTypeConfig) {
    return std::make_shared<SenseSourceTypeConfig>(SenseSourceTypeConfig(std::move(sourcTypeConfig)));
}

SenseSourceTypeConfigPtr SenseSourceTypeConfig::create() {
    return std::make_shared<SenseSourceTypeConfig>(SenseSourceTypeConfig());
}

SenseSourceTypeConfig::SenseSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceTypeConfig(SENSE_SOURCE_CONFIG),
      udfs(ConfigurationOption<std::string>::create(UDFS_CONFIG, "", "udfs, needed for: SenseSource")) {
    NES_INFO("SenseSourceConfig: Init source config object with values from sourceConfigMap.");
    if (sourceConfigMap.find(UDFS_CONFIG) != sourceConfigMap.end()) {
        udfs->setValue(sourceConfigMap.find(UDFS_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("OPCSourceConfig:: no udfs defined! Please define a udfs.");
    }
}

SenseSourceTypeConfig::SenseSourceTypeConfig(ryml::NodeRef sourcTypeConfig)
    : SourceTypeConfig(SENSE_SOURCE_CONFIG),
      udfs(ConfigurationOption<std::string>::create(UDFS_CONFIG, "", "udfs, needed for: SenseSource")) {
    NES_INFO("SenseSourceConfig: Init source config object with values from sourceConfigMap.");
    if (sourcTypeConfig.find_child(ryml::to_csubstr (UDFS_CONFIG)).has_val()) {
        udfs->setValue(sourcTypeConfig.find_child(ryml::to_csubstr (UDFS_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("SenseSourceTypeConfig:: no udfs defined! Please define a udfs.");
    }
}

SenseSourceTypeConfig::SenseSourceTypeConfig()
    : SourceTypeConfig(SENSE_SOURCE_CONFIG),
      udfs(ConfigurationOption<std::string>::create(UDFS_CONFIG, "", "udfs, needed for: SenseSource")) {
    NES_INFO("SenseSourceTypeConfig: Init source config object with default values.");
}

void SenseSourceTypeConfig::resetSourceOptions() {
    setUdfs(udfs->getDefaultValue());
    SourceTypeConfig::resetSourceOptions(SENSE_SOURCE_CONFIG);
}

std::string SenseSourceTypeConfig::toString() {
    std::stringstream ss;
    ss << UDFS_CONFIG + ":" + udfs->toStringNameCurrentValue();
    ss << SourceTypeConfig::toString();
    return ss.str();
}

bool SenseSourceTypeConfig::equal(const SourceTypeConfigPtr& other) {
    if (!other->instanceOf<SenseSourceTypeConfig>()) {
        return false;
    }
    auto otherSourceConfig = other->as<SenseSourceTypeConfig>();
    return SourceTypeConfig::equal(other) && udfs->getValue() == otherSourceConfig->udfs->getValue();
}

StringConfigOption SenseSourceTypeConfig::getUdfs() const { return udfs; }

void SenseSourceTypeConfig::setUdfs(std::string udfsValue) { udfs->setValue(udfsValue); }

}// namespace Configurations
}// namespace NES