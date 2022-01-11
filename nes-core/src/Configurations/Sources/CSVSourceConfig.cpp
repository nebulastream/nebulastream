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

CSVSourceTypeConfigPtr CSVSourceTypeConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<CSVSourceTypeConfig>(CSVSourceTypeConfig(std::move(sourceConfigMap)));
}

CSVSourceTypeConfigPtr CSVSourceTypeConfig::create() { return std::make_shared<CSVSourceTypeConfig>(CSVSourceTypeConfig()); }

CSVSourceTypeConfig::CSVSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceTypeConfig(sourceConfigMap, CSV_SOURCE_CONFIG),
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
    NES_INFO("CSVSourceConfig: Init source config object.");
    /*if (sourceConfigMap.find(CSV_SOURCE_FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find(CSV_SOURCE_FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("CSVSourceConfig:: no filePath defined! Please define a filePath.");
    }
    if (sourceConfigMap.find(CSV_SOURCE_DELIMITER_CONFIG) != sourceConfigMap.end()) {
        delimiter->setValue(sourceConfigMap.find(CSV_SOURCE_DELIMITER_CONFIG)->second);
    }
    if (sourceConfigMap.find(CSV_SOURCE_SKIP_HEADER_CONFIG) != sourceConfigMap.end()) {
        skipHeader->setValue((sourceConfigMap.find(CSV_SOURCE_SKIP_HEADER_CONFIG)->second == "true"));
    }*/
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
    NES_INFO("CSVSourceConfig: Init source config object with default values.");
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
    ss << filePath->toStringNameCurrentValue();
    ss << skipHeader->toStringNameCurrentValue();
    ss << delimiter->toStringNameCurrentValue();
    ss << inputFormat->toStringNameCurrentValue();
    ss << sourceFrequency->toStringNameCurrentValue();
    ss << numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << numberOfTuplesToProducePerBuffer->toStringNameCurrentValue();
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