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

#include <Catalogs/Source/PhysicalSourceTypes/SenseSourceType.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

SenseSourceTypePtr SenseSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<SenseSourceType>(SenseSourceType(std::move(sourceConfigMap)));
}

SenseSourceTypePtr SenseSourceType::create(ryml::NodeRef sourcTypeConfig) {
    return std::make_shared<SenseSourceType>(SenseSourceType(std::move(sourcTypeConfig)));
}

SenseSourceTypePtr SenseSourceType::create() { return std::make_shared<SenseSourceType>(SenseSourceType()); }

SenseSourceType::SenseSourceType(std::map<std::string, std::string> sourceConfigMap) : SenseSourceType() {
    NES_INFO("SenseSourceConfig: Init source config object with values from sourceConfigMap.");
    if (sourceConfigMap.find(Configurations::UDFS_CONFIG) != sourceConfigMap.end()) {
        udfs->setValue(sourceConfigMap.find(Configurations::UDFS_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("OPCSourceConfig:: no udfs defined! Please define a udfs.");
    }
}

SenseSourceType::SenseSourceType(ryml::NodeRef sourcTypeConfig) : SenseSourceType() {
    NES_INFO("SenseSourceConfig: Init source config object with values from sourceConfigMap.");
    if (sourcTypeConfig.find_child(ryml::to_csubstr(Configurations::UDFS_CONFIG)).has_val()) {
        udfs->setValue(sourcTypeConfig.find_child(ryml::to_csubstr(Configurations::UDFS_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("SenseSourceType:: no udfs defined! Please define a udfs.");
    }
}

SenseSourceType::SenseSourceType()
    : PhysicalSourceType(SENSE_SOURCE),
      udfs(Configurations::ConfigurationOption<std::string>::create(Configurations::UDFS_CONFIG,
                                                                    "",
                                                                    "udfs, needed for: SenseSource")) {
    NES_INFO("SenseSourceType: Init source config object with default values.");
}

std::string SenseSourceType::toString() {
    std::stringstream ss;
    ss << "SenseSourceType => {\n";
    ss << Configurations::UDFS_CONFIG + ":" + udfs->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool SenseSourceType::equal(const PhysicalSourceTypePtr& other) {

    if (!other->instanceOf<SenseSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<SenseSourceType>();
    return udfs->getValue() == otherSourceConfig->udfs->getValue();
}

Configurations::StringConfigOption SenseSourceType::getUdfs() const { return udfs; }

void SenseSourceType::setUdfs(std::string udfsValue) { udfs->setValue(udfsValue); }

void SenseSourceType::reset() { setUdfs(udfs->getDefaultValue()); }

}// namespace NES