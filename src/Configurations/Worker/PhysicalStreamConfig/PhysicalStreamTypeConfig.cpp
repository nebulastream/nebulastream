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
#include <Configurations/Worker/PhysicalStreamConfig/DefaultSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/PhysicalStreamTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfigFactory.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

PhysicalStreamTypeConfigPtr PhysicalStreamTypeConfig::create(ryml::NodeRef physicalStreamConfigNode) {
    return std::make_shared<PhysicalStreamTypeConfig>(PhysicalStreamTypeConfig(physicalStreamConfigNode));
}

PhysicalStreamTypeConfig::PhysicalStreamTypeConfig(ryml::NodeRef physicalStreamConfigNode)
    : physicalStreamName(
        ConfigOption<std::string>::create(PHYSICAL_STREAM_NAME_CONFIG, "default_physical", "Physical name of the stream.")),
      logicalStreamName(
          ConfigOption<std::string>::create(LOGICAL_STREAM_NAME_CONFIG, "default_logical", "Logical name of the stream.")) {
    NES_INFO("NesSourceConfig: Init physical stream config object with new values.");

    std::string sourceType = physicalStreamConfigNode.find_child(ryml::to_csubstr(SOURCE_TYPE_CONFIG)).val().str;

    sourceTypeConfig =
        SourceTypeConfigFactory::createSourceConfig(physicalStreamConfigNode.find_child(ryml::to_csubstr(sourceType)),
                                                    sourceType);
}

PhysicalStreamTypeConfigPtr PhysicalStreamTypeConfig::create(const std::map<std::string, std::string>& inputParams) {
    return std::make_shared<PhysicalStreamTypeConfig>(PhysicalStreamTypeConfig(inputParams));
}

PhysicalStreamTypeConfig::PhysicalStreamTypeConfig(const std::map<std::string, std::string>& inputParams)
    : physicalStreamName(
        ConfigOption<std::string>::create(PHYSICAL_STREAM_NAME_CONFIG, "default_physical", "Physical name of the stream.")),
      logicalStreamName(
          ConfigOption<std::string>::create(LOGICAL_STREAM_NAME_CONFIG, "default_logical", "Logical name of the stream.")) {
    NES_INFO("NesSourceConfig: Init physical stream config object with new values.");

    if (inputParams.find("--" + PHYSICAL_STREAM_NAME_CONFIG) != inputParams.end()
        && !inputParams.find("--" + PHYSICAL_STREAM_NAME_CONFIG)->second.empty()) {
        physicalStreamName->setValue(inputParams.find("--" + PHYSICAL_STREAM_NAME_CONFIG)->second);
    }
    if (inputParams.find("--" + LOGICAL_STREAM_NAME_CONFIG) != inputParams.end()
        && !inputParams.find("--" + LOGICAL_STREAM_NAME_CONFIG)->second.empty()) {
        physicalStreamName->setValue(inputParams.find("--" + LOGICAL_STREAM_NAME_CONFIG)->second);
    }

    sourceTypeConfig = SourceTypeConfigFactory::createSourceConfig(inputParams);
}

PhysicalStreamTypeConfigPtr PhysicalStreamTypeConfig::create(std::string sourceType) {
    return std::make_shared<PhysicalStreamTypeConfig>(PhysicalStreamTypeConfig(sourceType));
}

PhysicalStreamTypeConfig::PhysicalStreamTypeConfig(std::string sourceType)
    : physicalStreamName(
        ConfigOption<std::string>::create(PHYSICAL_STREAM_NAME_CONFIG, "default_physical", "Physical name of the stream.")),
      logicalStreamName(
          ConfigOption<std::string>::create(LOGICAL_STREAM_NAME_CONFIG, "default_logical", "Logical name of the stream.")) {
    NES_INFO("NesSourceConfig: Init physical stream config object with new values.");

    sourceTypeConfig = SourceTypeConfigFactory::createSourceConfig(sourceType);
}

PhysicalStreamTypeConfigPtr PhysicalStreamTypeConfig::create() {
    return std::make_shared<PhysicalStreamTypeConfig>(PhysicalStreamTypeConfig());
}

PhysicalStreamTypeConfig::PhysicalStreamTypeConfig()
    : physicalStreamName(
        ConfigOption<std::string>::create(PHYSICAL_STREAM_NAME_CONFIG, "default_physical", "Physical name of the stream.")),
      logicalStreamName(
          ConfigOption<std::string>::create(LOGICAL_STREAM_NAME_CONFIG, "default_logical", "Logical name of the stream.")),
      sourceTypeConfig(DefaultSourceTypeConfig::create()) {
    NES_INFO("NesSourceConfig: Init physical Stream config object with default values.");
}

void PhysicalStreamTypeConfig::resetPhysicalStreamOptions() {
    setPhysicalStreamName(physicalStreamName->getDefaultValue());
    setLogicalStreamName(logicalStreamName->getDefaultValue());
    sourceTypeConfig->resetSourceOptions(getSourceTypeConfig()->getSourceType()->getValue());
}

std::string PhysicalStreamTypeConfig::toString() {
    std::stringstream ss;
    ss << PHYSICAL_STREAM_NAME_CONFIG + ":" + physicalStreamName->toStringNameCurrentValue();
    ss << LOGICAL_STREAM_NAME_CONFIG + ":" + logicalStreamName->toStringNameCurrentValue();
    ss << sourceTypeConfig->toString();
    return ss.str();
}

bool PhysicalStreamTypeConfig::equal(const PhysicalStreamTypeConfigPtr& other) {
    return physicalStreamName->getValue() == other->physicalStreamName->getValue()
        && logicalStreamName->getValue() == other->logicalStreamName->getValue()
        && sourceTypeConfig->equal(other->sourceTypeConfig);
}

StringConfigOption PhysicalStreamTypeConfig::getPhysicalStreamName() const { return physicalStreamName; }

StringConfigOption PhysicalStreamTypeConfig::getLogicalStreamName() const { return logicalStreamName; }

void PhysicalStreamTypeConfig::setPhysicalStreamName(std::string physicalStreamNameValue) {
    physicalStreamName->setValue(std::move(physicalStreamNameValue));
}

void PhysicalStreamTypeConfig::setLogicalStreamName(std::string logicalStreamNameValue) {
    logicalStreamName->setValue(std::move(logicalStreamNameValue));
}
const SourceTypeConfigPtr& PhysicalStreamTypeConfig::getSourceTypeConfig() const { return sourceTypeConfig; }

void PhysicalStreamTypeConfig::setSourceTypeConfig(const SourceTypeConfigPtr& sourceTypeConfigValue) {
    PhysicalStreamTypeConfig::sourceTypeConfig = sourceTypeConfigValue;
}

}// namespace Configurations
}// namespace NES