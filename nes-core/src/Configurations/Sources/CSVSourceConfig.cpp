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
#include <Configurations/Worker/PhysicalStreamConfig/CSVSourceTypeConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

CSVSourceTypeConfigPtr CSVSourceTypeConfig::create(ryml::NodeRef sourcTypeConfig) {
    return std::make_shared<CSVSourceTypeConfig>(CSVSourceTypeConfig(std::move(sourcTypeConfig)));
}

CSVSourceTypeConfigPtr CSVSourceTypeConfig::create() { return std::make_shared<CSVSourceTypeConfig>(CSVSourceTypeConfig()); }

CSVSourceTypeConfigPtr CSVSourceTypeConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<CSVSourceTypeConfig>(CSVSourceTypeConfig(std::move(sourceConfigMap)));
}

CSVSourceTypeConfig::CSVSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceTypeConfig(CSV_SOURCE_CONFIG),
      filePath(ConfigOption<std::string>::create(FILE_PATH_CONFIG, "", "file path, needed for: CSVSource, BinarySource")),
      skipHeader(ConfigOption<bool>::create(SKIP_HEADER_CONFIG, false, "Skip first line of the file.")),
      delimiter(
          ConfigOption<std::string>::create(DELIMITER_CONFIG, ",", "delimiter for distinguishing between values in a file")),
      numberOfBuffersToProduce(
          ConfigOption<uint32_t>::create(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG, 1, "Number of buffers to produce.")),
      numberOfTuplesToProducePerBuffer(ConfigOption<uint32_t>::create(NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                                      1,
                                                                      "Number of tuples to produce per buffer.")),
      sourceFrequency(ConfigOption<uint32_t>::create(SOURCE_FREQUENCY_CONFIG, 1, "Sampling frequency of the source.")),
      inputFormat(ConfigOption<std::string>::create(INPUT_FORMAT_CONFIG, "JSON", "input data format")) {
    NES_INFO("CSVSourceConfig: Init source config object.");
    if (sourceConfigMap.find(FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find(FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("CSVSourceConfig:: no filePath defined! Please define a filePath.");
    }
    if (sourceConfigMap.find(DELIMITER_CONFIG) != sourceConfigMap.end()) {
        delimiter->setValue(sourceConfigMap.find(DELIMITER_CONFIG)->second);
    }
    if (sourceConfigMap.find(SKIP_HEADER_CONFIG) != sourceConfigMap.end()) {
        skipHeader->setValue((sourceConfigMap.find(SKIP_HEADER_CONFIG)->second == "true"));
    }
    if (sourceConfigMap.find(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG) != sourceConfigMap.end()) {
        numberOfBuffersToProduce->setValue(std::stoi(sourceConfigMap.find(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)->second));
    }
    if (sourceConfigMap.find(NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG) != sourceConfigMap.end()) {
        numberOfTuplesToProducePerBuffer->setValue(std::stoi(sourceConfigMap.find(NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG)->second));
    }
    if (sourceConfigMap.find(SOURCE_FREQUENCY_CONFIG) != sourceConfigMap.end()) {
        sourceFrequency->setValue(std::stoi(sourceConfigMap.find(SOURCE_FREQUENCY_CONFIG)->second));
    }
    if (sourceConfigMap.find(INPUT_FORMAT_CONFIG) != sourceConfigMap.end()) {
        inputFormat->setValue(sourceConfigMap.find(INPUT_FORMAT_CONFIG)->second);
    }
}

CSVSourceTypeConfig::CSVSourceTypeConfig(ryml::NodeRef sourcTypeConfig)
    : SourceTypeConfig(CSV_SOURCE_CONFIG),
      filePath(ConfigOption<std::string>::create(FILE_PATH_CONFIG, "", "file path, needed for: CSVSource, BinarySource")),
      skipHeader(ConfigOption<bool>::create(SKIP_HEADER_CONFIG, false, "Skip first line of the file.")),
      delimiter(
          ConfigOption<std::string>::create(DELIMITER_CONFIG, ",", "delimiter for distinguishing between values in a file")),
      numberOfBuffersToProduce(
          ConfigOption<uint32_t>::create(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG, 1, "Number of buffers to produce.")),
      numberOfTuplesToProducePerBuffer(ConfigOption<uint32_t>::create(NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                                      1,
                                                                      "Number of tuples to produce per buffer.")),
      sourceFrequency(ConfigOption<uint32_t>::create(SOURCE_FREQUENCY_CONFIG, 1, "Sampling frequency of the source.")),
      inputFormat(ConfigOption<std::string>::create(INPUT_FORMAT_CONFIG, "JSON", "input data format")){
    NES_INFO("CSVSourceTypeConfig: Init source config object.");
    if (sourcTypeConfig.find_child(ryml::to_csubstr (FILE_PATH_CONFIG)).has_val()) {
        filePath->setValue(sourcTypeConfig.find_child(ryml::to_csubstr (FILE_PATH_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("CSVSourceTypeConfig:: no filePath defined! Please define a filePath.");
    }
    if (sourcTypeConfig.find_child(ryml::to_csubstr (DELIMITER_CONFIG)).has_val()) {
        delimiter->setValue(sourcTypeConfig.find_child(ryml::to_csubstr (DELIMITER_CONFIG)).val().str);
    }
    if (sourcTypeConfig.find_child(ryml::to_csubstr (SKIP_HEADER_CONFIG)).has_val()) {
        skipHeader->setValue((strcasecmp(sourcTypeConfig.find_child(ryml::to_csubstr(SKIP_HEADER_CONFIG)).val().str, "true")));
    }
    if (sourcTypeConfig.find_child(ryml::to_csubstr (NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)).has_val()) {
        numberOfBuffersToProduce->setValue(std::stoi(sourcTypeConfig.find_child(ryml::to_csubstr (NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)).val().str));
    }
    if (sourcTypeConfig.find_child(ryml::to_csubstr (NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG)).has_val()) {
        numberOfTuplesToProducePerBuffer->setValue(std::stoi(sourcTypeConfig.find_child(ryml::to_csubstr (NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG)).val().str));
    }
    if (sourcTypeConfig.find_child(ryml::to_csubstr (SOURCE_FREQUENCY_CONFIG)).has_val()) {
        sourceFrequency->setValue(std::stoi(sourcTypeConfig.find_child(ryml::to_csubstr (SOURCE_FREQUENCY_CONFIG)).val().str));
    }
    if (sourcTypeConfig.find_child(ryml::to_csubstr (INPUT_FORMAT_CONFIG)).has_val()) {
        inputFormat->setValue(sourcTypeConfig.find_child(ryml::to_csubstr (INPUT_FORMAT_CONFIG)).val().str);
    }
}

CSVSourceTypeConfig::CSVSourceTypeConfig()
    : SourceTypeConfig(CSV_SOURCE_CONFIG),
      filePath(ConfigOption<std::string>::create(FILE_PATH_CONFIG, "", "file path, needed for: CSVSource, BinarySource")),
      skipHeader(ConfigOption<bool>::create(SKIP_HEADER_CONFIG, false, "Skip first line of the file.")),
      delimiter(
          ConfigOption<std::string>::create(DELIMITER_CONFIG, ",", "delimiter for distinguishing between values in a file")),
      numberOfBuffersToProduce(
          ConfigOption<uint32_t>::create(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG, 1, "Number of buffers to produce.")),
      numberOfTuplesToProducePerBuffer(ConfigOption<uint32_t>::create(NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                                      1,
                                                                      "Number of tuples to produce per buffer.")),
      sourceFrequency(ConfigOption<uint32_t>::create(SOURCE_FREQUENCY_CONFIG, 1, "Sampling frequency of the source.")),
      inputFormat(ConfigOption<std::string>::create(INPUT_FORMAT_CONFIG, "JSON", "input data format")) {
    NES_INFO("CSVSourceTypeConfig: Init source config object with default values.");
}

void CSVSourceTypeConfig::resetSourceOptions() {
    setFilePath(filePath->getDefaultValue());
    setSkipHeader(skipHeader->getDefaultValue());
    setDelimiter(delimiter->getDefaultValue());
    setInputFormat(inputFormat->getDefaultValue());
    setSourceFrequency(sourceFrequency->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer->getDefaultValue());
    SourceTypeConfig::resetSourceOptions(CSV_SOURCE_CONFIG);
}

std::string CSVSourceTypeConfig::toString() {
    std::stringstream ss;
    ss << FILE_PATH_CONFIG + ":" + filePath->toStringNameCurrentValue();
    ss << SKIP_HEADER_CONFIG + ":" +  skipHeader->toStringNameCurrentValue();
    ss << DELIMITER_CONFIG + ":" +  delimiter->toStringNameCurrentValue();
    ss << INPUT_FORMAT_CONFIG + ":" +  inputFormat->toStringNameCurrentValue();
    ss << SOURCE_FREQUENCY_CONFIG + ":" +  sourceFrequency->toStringNameCurrentValue();
    ss << NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG + ":" +  numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG + ":" +  numberOfTuplesToProducePerBuffer->toStringNameCurrentValue();
    ss << SourceTypeConfig::toString();
    return ss.str();
}

bool CSVSourceTypeConfig::equal(const SourceTypeConfigPtr& other) {
    if (!other->instanceOf<CSVSourceTypeConfig>()) {
        return false;
    }
    auto otherSourceConfig = other->as<CSVSourceTypeConfig>();
    return filePath->getValue() == otherSourceConfig->filePath->getValue()
        && skipHeader->getValue() == otherSourceConfig->skipHeader->getValue()
        && delimiter->getValue() == otherSourceConfig->delimiter->getValue()
        && inputFormat->getValue() == otherSourceConfig->inputFormat->getValue()
        && sourceFrequency->getValue() == otherSourceConfig->sourceFrequency->getValue()
        && numberOfBuffersToProduce->getValue() == otherSourceConfig->numberOfBuffersToProduce->getValue()
        && numberOfTuplesToProducePerBuffer->getValue() == otherSourceConfig->numberOfTuplesToProducePerBuffer->getValue()
        && SourceTypeConfig::equal(other);
}

StringConfigOption CSVSourceTypeConfig::getFilePath() const { return filePath; }

BoolConfigOption CSVSourceTypeConfig::getSkipHeader() const { return skipHeader; }

StringConfigOption CSVSourceTypeConfig::getDelimiter() const { return delimiter; }

StringConfigOption CSVSourceTypeConfig::getInputFormat() const { return inputFormat; }

IntConfigOption CSVSourceTypeConfig::getSourceFrequency() const { return sourceFrequency; }

IntConfigOption CSVSourceTypeConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

IntConfigOption CSVSourceTypeConfig::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

void CSVSourceTypeConfig::setSkipHeader(bool skipHeaderValue) { skipHeader->setValue(skipHeaderValue); }

void CSVSourceTypeConfig::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

void CSVSourceTypeConfig::setDelimiter(std::string delimiterValue) { delimiter->setValue(std::move(delimiterValue)); }

void CSVSourceTypeConfig::setSourceFrequency(uint32_t sourceFrequencyValue) { sourceFrequency->setValue(sourceFrequencyValue); }

void CSVSourceTypeConfig::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}

void CSVSourceTypeConfig::setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBufferValue) {
    numberOfTuplesToProducePerBuffer->setValue(numberOfTuplesToProducePerBufferValue);
}

void CSVSourceTypeConfig::setInputFormat(std::string inputFormatValue) { inputFormat->setValue(std::move(inputFormatValue)); }

}// namespace Configurations
}// namespace NES