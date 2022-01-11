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
#include <Configurations/Worker/PhysicalStreamConfig/PhysicalStreamConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

PhysicalStreamConfigPtr PhysicalStreamConfig::create() {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig());
}

PhysicalStreamConfig::PhysicalStreamConfig()
    : physicalStreamName(
          ConfigOption<std::string>::create(PHYSICAL_STREAM_NAME_CONFIG, "default_physical", "Physical name of the stream.")),
      logicalStreamName(
          ConfigOption<std::string>::create(LOGICAL_STREAM_NAME_CONFIG, "default_logical", "Logical name of the stream.")){
    NES_INFO("NesSourceConfig: Init source config object with new values.");
}

void PhysicalStreamConfig::resetPhysicalStreamOptions() {
    setPhysicalStreamName(physicalStreamName->getDefaultValue());
    setLogicalStreamName(logicalStreamName->getDefaultValue());
    sourceTypeConfig->resetSourceOptions(getSourceTypeConfig()->getSourceType()->getValue());
}

std::string PhysicalStreamConfig::toString() {
    std::stringstream ss;
    ss << physicalStreamName->toStringNameCurrentValue();
    ss << logicalStreamName->toStringNameCurrentValue();
    ss << sourceTypeConfig->toString();
    return ss.str();
}

bool PhysicalStreamConfig::equal(const PhysicalStreamConfigPtr& other) {
    if (!other->instanceOf<PhysicalStreamConfig>()) {
        return false;
    }
    return physicalStreamName->getValue() == other->physicalStreamName->getValue()
        && logicalStreamName->getValue() == other->logicalStreamName->getValue()
        && sourceTypeConfig->equal(other->sourceTypeConfig);
}

StringConfigOption PhysicalStreamConfig::getPhysicalStreamName() const { return physicalStreamName; }

StringConfigOption PhysicalStreamConfig::getLogicalStreamName() const { return logicalStreamName; }

void PhysicalStreamConfig::setPhysicalStreamName(std::string physicalStreamNameValue) {
    physicalStreamName->setValue(std::move(physicalStreamNameValue));
}

void PhysicalStreamConfig::setLogicalStreamName(std::string logicalStreamNameValue) {
    logicalStreamName->setValue(std::move(logicalStreamNameValue));
}
const SourceTypeConfigPtr& PhysicalStreamConfig::getSourceTypeConfig() const { return sourceTypeConfig; }
void PhysicalStreamConfig::setSourceTypeConfig(const SourceTypeConfigPtr& sourceTypeConfig) {
    PhysicalStreamConfig::sourceTypeConfig = sourceTypeConfig;
}

}// namespace Configurations
}// namespace NES