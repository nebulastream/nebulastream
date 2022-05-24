/*
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

#include <Catalogs/Source/PhysicalSourceTypes/TensorSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

TensorSourceTypePtr TensorSourceType::create(Yaml::Node yamlConfig) {
    return std::make_shared<TensorSourceType>(TensorSourceType(std::move(yamlConfig)));
}

TensorSourceTypePtr TensorSourceType::create() { return std::make_shared<TensorSourceType>(TensorSourceType()); }

TensorSourceTypePtr TensorSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<TensorSourceType>(TensorSourceType(std::move(sourceConfigMap)));
}

TensorSourceType::TensorSourceType()
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
      inputFormat(
          Configurations::ConfigurationOption<std::string>::create(Configurations::INPUT_FORMAT_CONFIG, "CSV", "input format")),
      numberOfBuffersToProduce(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG,
                                                                1,
                                                                "Number of buffers to produce.")),
      numberOfTuplesToProducePerBuffer(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                                1,
                                                                "Number of tuples to produce per buffer.")) {
    NES_INFO("CSVSourceTypeConfig: Init source config object with default values.");
}

TensorSourceType::TensorSourceType(std::map<std::string, std::string> sourceConfigMap) : TensorSourceType() {
    NES_INFO("TensorSourceType: Init default tensor source config object with values from command line.");
    if (sourceConfigMap.find(Configurations::FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find(Configurations::FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("CSVSourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }
    if (sourceConfigMap.find(Configurations::DELIMITER_CONFIG) != sourceConfigMap.end()) {
        delimiter->setValue(sourceConfigMap.find(Configurations::DELIMITER_CONFIG)->second);
    }
    if (sourceConfigMap.find(Configurations::SKIP_HEADER_CONFIG) != sourceConfigMap.end()) {
        skipHeader->setValue((sourceConfigMap.find(Configurations::SKIP_HEADER_CONFIG)->second == "true"));
    }
    if (sourceConfigMap.find(Configurations::INPUT_FORMAT_CONFIG) != sourceConfigMap.end()) {
        inputFormat->setValue((sourceConfigMap.find(Configurations::INPUT_FORMAT_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG) != sourceConfigMap.end()) {
        numberOfBuffersToProduce->setValue(
            std::stoi(sourceConfigMap.find(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG) != sourceConfigMap.end()) {
        numberOfTuplesToProducePerBuffer->setValue(
            std::stoi(sourceConfigMap.find(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG)->second));
    }
}

TensorSourceType::TensorSourceType(Yaml::Node yamlConfig) : TensorSourceType() {
    NES_INFO("TensorSourceType: Init default tensor source config object with values from YAML file.");
    if (!yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>() != "\n") {
        filePath->setValue(yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("TensorSourceType:: no filePath defined! Please define a filePath using "
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
    if (!yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>() != "\n") {
        inputFormat->setValue(yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>());
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
}

std::string TensorSourceType::toString() {
    std::stringstream ss;
    ss << "CSVSource Type => {\n";
    ss << Configurations::FILE_PATH_CONFIG + ":" + filePath->toStringNameCurrentValue();
    ss << Configurations::SKIP_HEADER_CONFIG + ":" + skipHeader->toStringNameCurrentValue();
    ss << Configurations::DELIMITER_CONFIG + ":" + delimiter->toStringNameCurrentValue();
    ss << Configurations::INPUT_FORMAT_CONFIG + ":" + inputFormat->toStringNameCurrentValue();
    ss << Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG + ":" + numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG + ":"
            + numberOfTuplesToProducePerBuffer->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool TensorSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<TensorSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<TensorSourceType>();
    return filePath->getValue() == otherSourceConfig->filePath->getValue()
        && skipHeader->getValue() == otherSourceConfig->skipHeader->getValue()
        && delimiter->getValue() == otherSourceConfig->delimiter->getValue()
        && inputFormat->getValue() == otherSourceConfig->inputFormat->getValue()
        && numberOfBuffersToProduce->getValue() == otherSourceConfig->numberOfBuffersToProduce->getValue()
        && numberOfTuplesToProducePerBuffer->getValue() == otherSourceConfig->numberOfTuplesToProducePerBuffer->getValue();
}

Configurations::StringConfigOption TensorSourceType::getFilePath() const { return filePath; }

Configurations::BoolConfigOption TensorSourceType::getSkipHeader() const { return skipHeader; }

Configurations::StringConfigOption TensorSourceType::getDelimiter() const { return delimiter; }

Configurations::StringConfigOption TensorSourceType::getInputFormat() const { return inputFormat; }

Configurations::IntConfigOption TensorSourceType::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

Configurations::IntConfigOption TensorSourceType::getNumberOfTuplesToProducePerBuffer() const {
    return numberOfTuplesToProducePerBuffer;
}

void TensorSourceType::setSkipHeader(bool skipHeaderValue) { skipHeader->setValue(skipHeaderValue); }

void TensorSourceType::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

void TensorSourceType::setDelimiter(std::string delimiterValue) { delimiter->setValue(std::move(delimiterValue)); }

void TensorSourceType::setInputFormat(std::string inputFormatValue) { inputFormat->setValue(std::move(inputFormatValue)); }

void TensorSourceType::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}

void TensorSourceType::setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBufferValue) {
    numberOfTuplesToProducePerBuffer->setValue(numberOfTuplesToProducePerBufferValue);
}

void TensorSourceType::reset() {
    setFilePath(filePath->getDefaultValue());
    setSkipHeader(skipHeader->getDefaultValue());
    setDelimiter(delimiter->getDefaultValue());
    setInputFormat(inputFormat->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer->getDefaultValue());
}

}// namespace NES
