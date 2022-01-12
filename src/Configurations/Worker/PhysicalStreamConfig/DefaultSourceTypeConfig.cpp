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
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

DefaultSourceTypeConfigPtr DefaultSourceTypeConfig::create(ryml::NodeRef sourcTypeConfig) {
    return std::make_shared<DefaultSourceTypeConfig>(DefaultSourceTypeConfig(sourcTypeConfig));
}

DefaultSourceTypeConfigPtr DefaultSourceTypeConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<DefaultSourceTypeConfig>(DefaultSourceTypeConfig(std::move(sourceConfigMap)));
}

DefaultSourceTypeConfigPtr DefaultSourceTypeConfig::create() {
    return std::make_shared<DefaultSourceTypeConfig>(DefaultSourceTypeConfig());
}

DefaultSourceTypeConfig::DefaultSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceTypeConfig(DEFAULT_SOURCE_CONFIG),
      numberOfBuffersToProduce(
          ConfigOption<uint32_t>::create(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG, 1, "Number of buffers to produce.")) {

    if (sourceConfigMap.find(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG) != sourceConfigMap.end()) {
        numberOfBuffersToProduce->setValue(std::stoi(sourceConfigMap.find(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)->second));
    }
    if (sourceConfigMap.find(SOURCE_FREQUENCY_CONFIG) != sourceConfigMap.end()) {
        sourceFrequency->setValue(std::stoi(sourceConfigMap.find(SOURCE_FREQUENCY_CONFIG)->second));
    }
    NES_INFO("NesSourceConfig: Init source config object with new values.");
}

DefaultSourceTypeConfig::DefaultSourceTypeConfig(ryml::NodeRef sourcTypeConfig)
    : SourceTypeConfig(DEFAULT_SOURCE_CONFIG),
      numberOfBuffersToProduce(
          ConfigOption<uint32_t>::create(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG, 1, "Number of buffers to produce.")),
      sourceFrequency(ConfigOption<uint32_t>::create(SOURCE_FREQUENCY_CONFIG, 1, "Sampling frequency of the source.")) {
    if (sourcTypeConfig.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)).has_val()) {
        numberOfBuffersToProduce->setValue(
            std::stoi(sourcTypeConfig.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)).val().str));
    }
    if (sourcTypeConfig.find_child(ryml::to_csubstr (SOURCE_FREQUENCY_CONFIG)).has_val()) {
        sourceFrequency->setValue(std::stoi(sourcTypeConfig.find_child(ryml::to_csubstr (SOURCE_FREQUENCY_CONFIG)).val().str));
    }
    NES_INFO("DefaultSourceTypeConfig: Init source config object with new values.");
}

DefaultSourceTypeConfig::DefaultSourceTypeConfig()
    : SourceTypeConfig(DEFAULT_SOURCE_CONFIG),
      numberOfBuffersToProduce(
          ConfigOption<uint32_t>::create(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG, 1, "Number of buffers to produce.")),
      sourceFrequency(ConfigOption<uint32_t>::create(SOURCE_FREQUENCY_CONFIG, 1, "Sampling frequency of the source.")) {
    NES_INFO("DefaultSourceTypeConfig: Init source config object with default values.");
}

void DefaultSourceTypeConfig::resetSourceOptions() {
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setSourceFrequency(sourceFrequency->getDefaultValue());
    SourceTypeConfig::resetSourceOptions(DEFAULT_SOURCE_CONFIG);
}

std::string DefaultSourceTypeConfig::toString() {
    std::stringstream ss;
    ss << NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG + ":" +  numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << SOURCE_FREQUENCY_CONFIG + ":" +  sourceFrequency->toStringNameCurrentValue();
    ss << SourceTypeConfig::toString();
    return ss.str();
}

bool DefaultSourceTypeConfig::equal(const SourceTypeConfigPtr& other) {
    if (!other->instanceOf<DefaultSourceTypeConfig>()) {
        return false;
    }
    auto otherSourceConfig = other->as<DefaultSourceTypeConfig>();
    return numberOfBuffersToProduce->getValue() == otherSourceConfig->numberOfBuffersToProduce->getValue()
        && sourceFrequency->getValue() == otherSourceConfig->sourceFrequency->getValue()
        && SourceTypeConfig::equal(other);
}

IntConfigOption DefaultSourceTypeConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

IntConfigOption DefaultSourceTypeConfig::getSourceFrequency() const { return sourceFrequency; }

void DefaultSourceTypeConfig::setSourceFrequency(uint32_t sourceFrequencyValue) { sourceFrequency->setValue(sourceFrequencyValue); }

void DefaultSourceTypeConfig::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}
}// namespace Configurations
}// namespace NES