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

#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

CSVSourceTypePtr CSVSourceType::create(Yaml::Node yamlConfig) {
    return std::make_shared<CSVSourceType>(CSVSourceType(std::move(yamlConfig)));
}

CSVSourceTypePtr CSVSourceType::create() { return std::make_shared<CSVSourceType>(CSVSourceType()); }

CSVSourceTypePtr CSVSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<CSVSourceType>(CSVSourceType(std::move(sourceConfigMap)));
}

CSVSourceType::CSVSourceType()
    : PhysicalSourceType(CSV_SOURCE),
      filePath(Configurations::ConfigurationOption<std::string>::create(Configurations::FILE_PATH_CONFIG,
                                                                        "",
                                                                        "file path, needed for: CSVSource, BinarySource")),
      skipHeader(Configurations::ConfigurationOption<bool>::create(Configurations::SKIP_HEADER_CONFIG,
                                                                   false,
                                                                   "Skip first line of the file.")),
      delimiter(
          Configurations::ConfigurationOption<std::string>::create(Configurations::DELIMITER_CONFIG,
                                                                   ",",
                                                                   "delimiter for distinguishing between values in a file")),
      numberOfBuffersToProduce(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG,
                                                                1,
                                                                "Number of buffers to produce.")),
      numberOfTuplesToProducePerBuffer(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                                1,
                                                                "Number of tuples to produce per buffer.")),
      sourceFrequency(Configurations::ConfigurationOption<uint32_t>::create(Configurations::SOURCE_FREQUENCY_CONFIG,
                                                                            1,
                                                                            "Sampling frequency of the source.")),
      inputFormat(Configurations::ConfigurationOption<std::string>::create(Configurations::INPUT_FORMAT_CONFIG,
                                                                           "JSON",
                                                                           "input data format")) {
    NES_INFO("CSVSourceTypeConfig: Init source config object with default values.");
}

CSVSourceType::CSVSourceType(std::map<std::string, std::string> sourceConfigMap) : CSVSourceType() {
    NES_INFO("CSVSourceType: Init default CSV source config object with values from command line.");
    if (sourceConfigMap.find("--" + Configurations::FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find("--" + Configurations::FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("CSVSourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }
    if (sourceConfigMap.find("--" + Configurations::DELIMITER_CONFIG) != sourceConfigMap.end()) {
        delimiter->setValue(sourceConfigMap.find("--" + Configurations::DELIMITER_CONFIG)->second);
    }
    if (sourceConfigMap.find("--" + Configurations::SKIP_HEADER_CONFIG) != sourceConfigMap.end()) {
        skipHeader->setValue((sourceConfigMap.find("--" + Configurations::SKIP_HEADER_CONFIG)->second == "true"));
    }
    if (sourceConfigMap.find("--" + Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG) != sourceConfigMap.end()) {
        numberOfBuffersToProduce->setValue(
            std::stoi(sourceConfigMap.find("--" + Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)->second));
    }
    if (sourceConfigMap.find("--" + Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG) != sourceConfigMap.end()) {
        numberOfTuplesToProducePerBuffer->setValue(
            std::stoi(sourceConfigMap.find("--" + Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG)->second));
    }
    if (sourceConfigMap.find("--" + Configurations::SOURCE_FREQUENCY_CONFIG) != sourceConfigMap.end()) {
        sourceFrequency->setValue(std::stoi(sourceConfigMap.find("--" + Configurations::SOURCE_FREQUENCY_CONFIG)->second));
    }
    if (sourceConfigMap.find("--" + Configurations::INPUT_FORMAT_CONFIG) != sourceConfigMap.end()) {
        inputFormat->setValue(sourceConfigMap.find("--" + Configurations::INPUT_FORMAT_CONFIG)->second);
    }
}

CSVSourceType::CSVSourceType(Yaml::Node yamlConfig) : CSVSourceType() {
    NES_INFO("CSVSourceType: Init default CSV source config object with values from YAML file.");
    if (!yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>() != "\n") {
        filePath->setValue(yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("CSVSourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }
    if (!yamlConfig[Configurations::DELIMITER_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::DELIMITER_CONFIG].As<std::string>() != "\n") {
        delimiter->setValue(yamlConfig[Configurations::DELIMITER_CONFIG].As<std::string>());
    }
    if (!yamlConfig[Configurations::SKIP_HEADER_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SKIP_HEADER_CONFIG].As<std::string>() != "\n") {
        skipHeader->setValue(yamlConfig[Configurations::SKIP_HEADER_CONFIG].As<bool>());
    }
    if (!yamlConfig[Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<std::string>() != "\n") {
        numberOfBuffersToProduce->setValue(yamlConfig[Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG].As<std::string>() != "\n") {
        numberOfTuplesToProducePerBuffer->setValue(
            yamlConfig[Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::SOURCE_FREQUENCY_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SOURCE_FREQUENCY_CONFIG].As<std::string>() != "\n") {
        sourceFrequency->setValue(yamlConfig[Configurations::SOURCE_FREQUENCY_CONFIG].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>() != "\n") {
        inputFormat->setValue(yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>());
    }
}

std::string CSVSourceType::toString() {
    std::stringstream ss;
    ss << "CSVSource Type => {\n";
    ss << Configurations::FILE_PATH_CONFIG + ":" + filePath->toStringNameCurrentValue();
    ss << Configurations::SKIP_HEADER_CONFIG + ":" + skipHeader->toStringNameCurrentValue();
    ss << Configurations::DELIMITER_CONFIG + ":" + delimiter->toStringNameCurrentValue();
    ss << Configurations::INPUT_FORMAT_CONFIG + ":" + inputFormat->toStringNameCurrentValue();
    ss << Configurations::SOURCE_FREQUENCY_CONFIG + ":" + sourceFrequency->toStringNameCurrentValue();
    ss << Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG + ":" + numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG + ":"
            + numberOfTuplesToProducePerBuffer->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool CSVSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<CSVSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<CSVSourceType>();
    return filePath->getValue() == otherSourceConfig->filePath->getValue()
        && skipHeader->getValue() == otherSourceConfig->skipHeader->getValue()
        && delimiter->getValue() == otherSourceConfig->delimiter->getValue()
        && inputFormat->getValue() == otherSourceConfig->inputFormat->getValue()
        && sourceFrequency->getValue() == otherSourceConfig->sourceFrequency->getValue()
        && numberOfBuffersToProduce->getValue() == otherSourceConfig->numberOfBuffersToProduce->getValue()
        && numberOfTuplesToProducePerBuffer->getValue() == otherSourceConfig->numberOfTuplesToProducePerBuffer->getValue();
}

Configurations::StringConfigOption CSVSourceType::getFilePath() const { return filePath; }

Configurations::BoolConfigOption CSVSourceType::getSkipHeader() const { return skipHeader; }

Configurations::StringConfigOption CSVSourceType::getDelimiter() const { return delimiter; }

Configurations::StringConfigOption CSVSourceType::getInputFormat() const { return inputFormat; }

Configurations::IntConfigOption CSVSourceType::getSourceFrequency() const { return sourceFrequency; }

Configurations::IntConfigOption CSVSourceType::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

Configurations::IntConfigOption CSVSourceType::getNumberOfTuplesToProducePerBuffer() const {
    return numberOfTuplesToProducePerBuffer;
}

void CSVSourceType::setSkipHeader(bool skipHeaderValue) { skipHeader->setValue(skipHeaderValue); }

void CSVSourceType::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

void CSVSourceType::setDelimiter(std::string delimiterValue) { delimiter->setValue(std::move(delimiterValue)); }

void CSVSourceType::setSourceFrequency(uint32_t sourceFrequencyValue) { sourceFrequency->setValue(sourceFrequencyValue); }

void CSVSourceType::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}

void CSVSourceType::setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBufferValue) {
    numberOfTuplesToProducePerBuffer->setValue(numberOfTuplesToProducePerBufferValue);
}

void CSVSourceType::setInputFormat(std::string inputFormatValue) { inputFormat->setValue(std::move(inputFormatValue)); }

void CSVSourceType::reset() {
    setFilePath(filePath->getDefaultValue());
    setSkipHeader(skipHeader->getDefaultValue());
    setDelimiter(delimiter->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer->getDefaultValue());
    setSourceFrequency(sourceFrequency->getDefaultValue());
    setInputFormat(inputFormat->getDefaultValue());
}

}// namespace NES
