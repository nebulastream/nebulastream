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
#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

SourceTypeConfig::SourceTypeConfig(std::map<std::string, std::string> sourceConfigMap, std::string _sourceType)
    :
      sourceType(ConfigOption<std::string>::create(SOURCE_TYPE_CONFIG,
                                                   std::move(_sourceType),
                                                   "Type of the Source (available options: NoSource, DefaultSource, CSVSource, "
                                                   "BinarySource, MQTTSource, KafkaSource, OPCSource).")) {
    NES_INFO("NesSourceConfig: Init source config object with new values.");

    if (sourceConfigMap.find(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG) != sourceConfigMap.end()) {
        numberOfBuffersToProduce->setValue(std::stoi(sourceConfigMap.find(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)->second));
    }
    if (sourceConfigMap.find(NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG) != sourceConfigMap.end()) {
        numberOfTuplesToProducePerBuffer->setValue(
            std::stoi(sourceConfigMap.find(NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG)->second));
    }
    if (sourceConfigMap.find(PHYSICAL_STREAM_NAME_CONFIG) != sourceConfigMap.end()) {
        physicalStreamName->setValue(sourceConfigMap.find(PHYSICAL_STREAM_NAME_CONFIG)->second);
    }
    if (sourceConfigMap.find(LOGICAL_STREAM_NAME_CONFIG) != sourceConfigMap.end()) {
        logicalStreamName->setValue(sourceConfigMap.find(LOGICAL_STREAM_NAME_CONFIG)->second);
    }
    if (sourceConfigMap.find(SOURCE_FREQUENCY_CONFIG) != sourceConfigMap.end()) {
        sourceFrequency->setValue(std::stoi(sourceConfigMap.find(SOURCE_FREQUENCY_CONFIG)->second));
    }
    if (sourceConfigMap.find(INPUT_FORMAT_CONFIG) != sourceConfigMap.end()) {
        inputFormat->setValue(sourceConfigMap.find(INPUT_FORMAT_CONFIG)->second);
    }
    if (sourceConfigMap.find(SOURCE_TYPE_CONFIG) != sourceConfigMap.end()) {
        sourceType->setValue(sourceConfigMap.find(SOURCE_TYPE_CONFIG)->second);
    }
}

SourceTypeConfig::SourceTypeConfig(std::string _sourceType)
    : sourceType(
          ConfigOption<std::string>::create(SOURCE_TYPE_CONFIG,
                                            std::move(_sourceType),
                                            "Type of the Source (available options: DefaultSource, CSVSource, BinarySource).")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

void SourceTypeConfig::resetSourceOptions(std::string _sourceType) {
    setSourceType(std::move(_sourceType));
}

std::string SourceTypeConfig::toString() {
    std::stringstream ss;
    ss << sourceType->toStringNameCurrentValue();
    return ss.str();
}

bool SourceTypeConfig::equal(const SourceTypeConfigPtr& other) {
    if (!other->instanceOf<SourceTypeConfig>()) {
        return false;
    }
    return sourceType->getValue() == other->sourceType->getValue();
}

StringConfigOption SourceTypeConfig::getSourceType() const { return sourceType; }

void SourceTypeConfig::setSourceType(std::string sourceTypeValue) { sourceType->setValue(std::move(sourceTypeValue)); }

}// namespace Configurations
}// namespace NES