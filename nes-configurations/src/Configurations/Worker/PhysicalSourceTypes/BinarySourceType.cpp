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

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/BinarySourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

BinarySourceTypePtr
BinarySourceType::create(const std::string& logicalSourceName, const std::string& physicalSourceName, Yaml::Node yamlConfig) {
    return std::make_shared<BinarySourceType>(BinarySourceType(logicalSourceName, physicalSourceName, yamlConfig));
}

BinarySourceType::BinarySourceType(const std::string& logicalSourceName,
                                   const std::string& physicalSourceName,
                                   Yaml::Node yamlConfig)
    : BinarySourceType(logicalSourceName, physicalSourceName) {
    NES_INFO("CSVSourceType: Init default CSV source config object with values from YAML.");
    if (!yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>() != "\n") {
        filePath->setValue(yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("BinarySourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }

    if (!yamlConfig[Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<std::string>() != "\n") {
        numberOfBuffersToProduce->setValue(yamlConfig[Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<uint32_t>());
        NES_ERROR("numberOfBuffersToProduce {}", numberOfBuffersToProduce.get()->getValue());
    }
}

BinarySourceTypePtr BinarySourceType::create(const std::string& logicalSourceName,
                                             const std::string& physicalSourceName,
                                             std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<BinarySourceType>(
        BinarySourceType(logicalSourceName, physicalSourceName, std::move(sourceConfigMap)));
}

BinarySourceType::BinarySourceType(const std::string& logicalSourceName,
                                   const std::string& physicalSourceName,
                                   std::map<std::string, std::string> sourceConfigMap)
    : BinarySourceType(logicalSourceName, physicalSourceName) {
    NES_INFO("CSVSourceType: Init default CSV source config object with values from command line.");
    if (sourceConfigMap.find("--" + Configurations::FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find("--" + Configurations::FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("BinarySourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }

    if (sourceConfigMap.find(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG) != sourceConfigMap.end()) {
            numberOfBuffersToProduce->setValue(
                std::stoi(sourceConfigMap.find(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)->second));
        NES_ERROR("numberOfBuffersToProduce {}", numberOfBuffersToProduce.get()->getValue());
    }
}

BinarySourceTypePtr BinarySourceType::create(const std::string& logicalSourceName, const std::string& physicalSourceName) {
    return std::make_shared<BinarySourceType>(BinarySourceType(logicalSourceName, physicalSourceName));
}

BinarySourceType::BinarySourceType(const std::string& logicalSourceName, const std::string& physicalSourceName)
    : PhysicalSourceType(logicalSourceName, physicalSourceName, SourceType::BINARY_SOURCE),
      filePath(Configurations::ConfigurationOption<std::string>::create(Configurations::FILE_PATH_CONFIG,
                                                                        "",
                                                                        "file path, needed for: CSVSource, BinarySource")),
      numberOfBuffersToProduce(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG,
                                                                0,
                                                                "Number of buffers to produce.")) {
    NES_INFO("BinarySourceTypeConfig: Init source config object with default params.");
}

std::string BinarySourceType::toString() {
    std::stringstream ss;
    ss << "BinarySource = {\n";
    ss << Configurations::FILE_PATH_CONFIG + ":" + filePath->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool BinarySourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<BinarySourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<BinarySourceType>();
    return filePath->getValue() == otherSourceConfig->filePath->getValue();
}

uint32_t BinarySourceType::getNumberOfTuples() { return numberOfBuffersToProduce->getValue(); }

Configurations::StringConfigOption BinarySourceType::getFilePath() const { return filePath; }

void BinarySourceType::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

void BinarySourceType::reset() { setFilePath(filePath->getDefaultValue()); }

}// namespace NES
