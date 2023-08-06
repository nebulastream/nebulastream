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

#include <Catalogs/Source/PhysicalSourceTypes/ArrowSourceType.hpp>
#include <Util/Logger/Logger.hpp>

#include <string>
#include <utility>

namespace NES {

ArrowSourceTypePtr ArrowSourceType::create(Yaml::Node yamlConfig) {
    return std::make_shared<ArrowSourceType>(ArrowSourceType(std::move(yamlConfig)));
}

ArrowSourceTypePtr ArrowSourceType::create() { return std::make_shared<ArrowSourceType>(ArrowSourceType()); }

ArrowSourceTypePtr ArrowSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<ArrowSourceType>(ArrowSourceType(std::move(sourceConfigMap)));
}

ArrowSourceType::ArrowSourceType()
    : PhysicalSourceType(SourceType::ARROW_SOURCE),
      filePath(Configurations::ConfigurationOption<std::string>::create(Configurations::FILE_PATH_CONFIG,
                                                                        "",
                                                                        "file path, needed for: ArrowSource.")),
      numberOfBuffersToProduce(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG,
                                                                0,
                                                                "Number of buffers to produce.")),
      numberOfTuplesToProducePerBuffer(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                                0,
                                                                "Number of tuples to produce per buffer.")),
      sourceGatheringInterval(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::SOURCE_GATHERING_INTERVAL_CONFIG,
                                                                0,
                                                                "Gathering interval of the source.")),
      gatheringMode(Configurations::ConfigurationOption<GatheringMode>::create(Configurations::SOURCE_GATHERING_MODE_CONFIG,
                                                                               GatheringMode::INTERVAL_MODE,
                                                                               "Gathering mode of the source.")) {
    NES_INFO("ArrowSourceTypeConfig: Init source config object with default values.");
}

ArrowSourceType::ArrowSourceType(std::map<std::string, std::string> sourceConfigMap) : ArrowSourceType() {
    NES_INFO("ArrowSourceType: Init default Arrow source config object with values from command line.");
    if (sourceConfigMap.find(Configurations::FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find(Configurations::FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("ArrowSourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }
    if (sourceConfigMap.find(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG) != sourceConfigMap.end()) {
        numberOfBuffersToProduce->setValue(
            std::stoi(sourceConfigMap.find(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG) != sourceConfigMap.end()) {
        numberOfTuplesToProducePerBuffer->setValue(
            std::stoi(sourceConfigMap.find(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::SOURCE_GATHERING_INTERVAL_CONFIG) != sourceConfigMap.end()) {
        sourceGatheringInterval->setValue(
            std::stoi(sourceConfigMap.find(Configurations::SOURCE_GATHERING_INTERVAL_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::SOURCE_GATHERING_MODE_CONFIG) != sourceConfigMap.end()) {
        gatheringMode->setValue(
            magic_enum::enum_cast<GatheringMode>(sourceConfigMap.find(Configurations::SOURCE_GATHERING_MODE_CONFIG)->second)
                .value());
    }
}

ArrowSourceType::ArrowSourceType(Yaml::Node yamlConfig) : ArrowSourceType() {
    NES_INFO("ArrowSourceType: Init default Arrow source config object with values from YAML file.");
    if (!yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>() != "\n") {
        filePath->setValue(yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("ArrowSourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
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
    if (!yamlConfig[Configurations::SOURCE_GATHERING_INTERVAL_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SOURCE_GATHERING_INTERVAL_CONFIG].As<std::string>() != "\n") {
        sourceGatheringInterval->setValue(yamlConfig[Configurations::SOURCE_GATHERING_INTERVAL_CONFIG].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::SOURCE_GATHERING_MODE_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SOURCE_GATHERING_MODE_CONFIG].As<std::string>() != "\n") {
        gatheringMode->setValue(
            magic_enum::enum_cast<GatheringMode>(yamlConfig[Configurations::SOURCE_GATHERING_MODE_CONFIG].As<std::string>())
                .value());
    }
}

std::string ArrowSourceType::toString() {
    std::stringstream ss;
    ss << "ArrowSource Type => {\n";
    ss << Configurations::FILE_PATH_CONFIG + ":" + filePath->toStringNameCurrentValue();
    ss << Configurations::SOURCE_GATHERING_INTERVAL_CONFIG + ":" + sourceGatheringInterval->toStringNameCurrentValue();
    ss << Configurations::SOURCE_GATHERING_MODE_CONFIG + ":" + std::string(magic_enum::enum_name(gatheringMode->getValue()))
       << "\n";
    ss << Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG + ":" + numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG + ":"
            + numberOfTuplesToProducePerBuffer->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool ArrowSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<ArrowSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<ArrowSourceType>();
    return filePath->getValue() == otherSourceConfig->filePath->getValue()
        && sourceGatheringInterval->getValue() == otherSourceConfig->sourceGatheringInterval->getValue()
        && gatheringMode->getValue() == otherSourceConfig->gatheringMode->getValue()
        && numberOfBuffersToProduce->getValue() == otherSourceConfig->numberOfBuffersToProduce->getValue()
        && numberOfTuplesToProducePerBuffer->getValue() == otherSourceConfig->numberOfTuplesToProducePerBuffer->getValue();
}

Configurations::StringConfigOption ArrowSourceType::getFilePath() const { return filePath; }

Configurations::IntConfigOption ArrowSourceType::getGatheringInterval() const { return sourceGatheringInterval; }

Configurations::IntConfigOption ArrowSourceType::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

Configurations::IntConfigOption ArrowSourceType::getNumberOfTuplesToProducePerBuffer() const {
    return numberOfTuplesToProducePerBuffer;
}

Configurations::GatheringModeConfigOption ArrowSourceType::getGatheringMode() const { return gatheringMode; }

void ArrowSourceType::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

void ArrowSourceType::setGatheringInterval(uint32_t sourceGatheringIntervalValue) {
    sourceGatheringInterval->setValue(sourceGatheringIntervalValue);
}

void ArrowSourceType::setNumberOfBuffersToProduce(const uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}

void ArrowSourceType::setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBufferValue) {
    numberOfTuplesToProducePerBuffer->setValue(numberOfTuplesToProducePerBufferValue);
}

void ArrowSourceType::setGatheringMode(std::string inputGatheringMode) {
    gatheringMode->setValue(magic_enum::enum_cast<GatheringMode>(inputGatheringMode).value());
}

void ArrowSourceType::setGatheringMode(GatheringMode inputGatheringMode) { gatheringMode->setValue(inputGatheringMode); }

void ArrowSourceType::reset() {
    setFilePath(filePath->getDefaultValue());
    setGatheringMode(gatheringMode->getDefaultValue());
    setGatheringInterval(sourceGatheringInterval->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer->getDefaultValue());
}

}// namespace NES
