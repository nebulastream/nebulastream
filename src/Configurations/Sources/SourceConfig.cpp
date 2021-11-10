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
#include <Configurations/Sources/SourceConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

SourceConfig::SourceConfig(std::map<std::string, std::string> sourceConfigMap, std::string _sourceType)
    : numberOfBuffersToProduce(
        ConfigOption<uint32_t>::create(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG, 1, "Number of buffers to produce.")),
      numberOfTuplesToProducePerBuffer(ConfigOption<uint32_t>::create(NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                                      1,
                                                                      "Number of tuples to produce per buffer.")),
      physicalStreamName(
          ConfigOption<std::string>::create(PHYSICAL_STREAM_NAME_CONFIG, "default_physical", "Physical name of the stream.")),
      logicalStreamName(
          ConfigOption<std::string>::create(LOGICAL_STREAM_NAME_CONFIG, "default_logical", "Logical name of the stream.")),
      sourceFrequency(ConfigOption<uint32_t>::create(SOURCE_FREQUENCY_CONFIG, 1, "Sampling frequency of the source.")),
      rowLayout(ConfigOption<bool>::create(ROW_LAYOUT_CONFIG, true, "storage layout, true = row layout, false = column layout")),
      inputFormat(ConfigOption<std::string>::create(INPUT_FORMAT_CONFIG, "JSON", "input data format")),
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
    if (sourceConfigMap.find(ROW_LAYOUT_CONFIG) != sourceConfigMap.end()) {
        rowLayout->setValue((sourceConfigMap.find(ROW_LAYOUT_CONFIG)->second == "true"));
    }
    if (sourceConfigMap.find(INPUT_FORMAT_CONFIG) != sourceConfigMap.end()) {
        inputFormat->setValue(sourceConfigMap.find(INPUT_FORMAT_CONFIG)->second);
    }
    if (sourceConfigMap.find(SOURCE_TYPE_CONFIG) != sourceConfigMap.end()) {
        sourceType->setValue(sourceConfigMap.find(SOURCE_TYPE_CONFIG)->second);
    }
}

SourceConfig::SourceConfig(std::string _sourceType)
    : numberOfBuffersToProduce(
        ConfigOption<uint32_t>::create(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG, 1, "Number of buffers to produce.")),
      numberOfTuplesToProducePerBuffer(ConfigOption<uint32_t>::create(NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                                      1,
                                                                      "Number of tuples to produce per buffer.")),
      physicalStreamName(
          ConfigOption<std::string>::create(PHYSICAL_STREAM_NAME_CONFIG, "default_physical", "Physical name of the stream.")),
      logicalStreamName(
          ConfigOption<std::string>::create(LOGICAL_STREAM_NAME_CONFIG, "default_logical", "Logical name of the stream.")),
      sourceFrequency(ConfigOption<uint32_t>::create(SOURCE_FREQUENCY_CONFIG, 1, "Sampling frequency of the source.")),
      rowLayout(ConfigOption<bool>::create(ROW_LAYOUT_CONFIG, true, "storage layout, true = row layout, false = column layout")),
      inputFormat(ConfigOption<std::string>::create(INPUT_FORMAT_CONFIG, "JSON", "input data format")),
      sourceType(
          ConfigOption<std::string>::create(SOURCE_TYPE_CONFIG,
                                            std::move(_sourceType),
                                            "Type of the Source (available options: DefaultSource, CSVSource, BinarySource).")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

void SourceConfig::resetSourceOptions(std::string _sourceType) {
    setSourceType(std::move(_sourceType));
    setInputFormat(inputFormat->getDefaultValue());
    setRowLayout(rowLayout->getDefaultValue());
    setSourceFrequency(sourceFrequency->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer->getDefaultValue());
    setPhysicalStreamName(physicalStreamName->getDefaultValue());
    setLogicalStreamName(logicalStreamName->getDefaultValue());
}

std::string SourceConfig::toString() {
    std::stringstream ss;
    ss << sourceType->toStringNameCurrentValue();
    ss << inputFormat->toStringNameCurrentValue();
    ss << rowLayout->toStringNameCurrentValue();
    ss << sourceFrequency->toStringNameCurrentValue();
    ss << numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << numberOfTuplesToProducePerBuffer->toStringNameCurrentValue();
    ss << physicalStreamName->toStringNameCurrentValue();
    ss << logicalStreamName->toStringNameCurrentValue();
    return ss.str();
}

StringConfigOption SourceConfig::getSourceType() const { return sourceType; }

StringConfigOption SourceConfig::getInputFormat() const { return inputFormat; }

BoolConfigOption SourceConfig::getRowLayout() const { return rowLayout; }

IntConfigOption SourceConfig::getSourceFrequency() const { return sourceFrequency; }

IntConfigOption SourceConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

IntConfigOption SourceConfig::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

StringConfigOption SourceConfig::getPhysicalStreamName() const { return physicalStreamName; }

StringConfigOption SourceConfig::getLogicalStreamName() const { return logicalStreamName; }

void SourceConfig::setSourceType(std::string sourceTypeValue) { sourceType->setValue(std::move(sourceTypeValue)); }

void SourceConfig::setSourceFrequency(uint32_t sourceFrequencyValue) { sourceFrequency->setValue(sourceFrequencyValue); }

void SourceConfig::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}

void SourceConfig::setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBufferValue) {
    numberOfTuplesToProducePerBuffer->setValue(numberOfTuplesToProducePerBufferValue);
}

void SourceConfig::setPhysicalStreamName(std::string physicalStreamNameValue) {
    physicalStreamName->setValue(std::move(physicalStreamNameValue));
}

void SourceConfig::setLogicalStreamName(std::string logicalStreamNameValue) {
    logicalStreamName->setValue(std::move(logicalStreamNameValue));
}

void SourceConfig::setInputFormat(std::string inputFormatValue) { inputFormat->setValue(std::move(inputFormatValue)); }

void SourceConfig::setRowLayout(bool rowLayoutValue) { rowLayout->setValue(rowLayoutValue); }

}// namespace Configurations
}// namespace NES