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
#ifdef ENABLE_PARQUET_BUILD
#include <Catalogs/Source/PhysicalSourceTypes/ParquetSourceType.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES{
ParquetSourceTypePtr ParquetSourceType::create(std::map<std::string, std::string> sourceConfig) {
    return std::make_shared<ParquetSourceType>(ParquetSourceType(std::move(sourceConfig)));
}

ParquetSourceTypePtr ParquetSourceType::create() {
    return std::make_shared<ParquetSourceType>(ParquetSourceType()); }

ParquetSourceType::ParquetSourceType(std::map<std::string, std::string> sourceConfigMap) : ParquetSourceType() {
    NES_INFO("ParquetSourceType: Init default CSV source config object with values from command line.");
    if (sourceConfigMap.find(Configurations::FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find(Configurations::FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("ParquetSourceType:: no filePath defined! Please define a filePath using "
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
}
ParquetSourceType::ParquetSourceType() : PhysicalSourceType(PARQUET_SOURCE),
                                         filePath(Configurations::ConfigurationOption<std::string>::create(Configurations::FILE_PATH_CONFIG,
                                                                                                           "",
                                                                                                           "file path, needed for: CSVSource, BinarySource, ParquetSource")),
                                         numberOfBuffersToProduce(
                                             Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG,
                                                                                                   1,
                                                                                                   "Number of buffers to produce.")),
                                         numberOfTuplesToProducePerBuffer(
                                             Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                                                                   1,
                                                                                                   "Number of tuples to produce per buffer."))
{
        NES_INFO("ParquetSourceType: Init source config object with default values.");
}

void ParquetSourceType::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

void ParquetSourceType::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}

void ParquetSourceType::setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBufferValue) {
    numberOfTuplesToProducePerBuffer->setValue(numberOfTuplesToProducePerBufferValue);
}
std::string ParquetSourceType::toString() {
    std::stringstream ss;
    ss << "ParquetSource Type => {\n";
    ss << Configurations::FILE_PATH_CONFIG + ":" + filePath->toStringNameCurrentValue();
    ss << Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG + ":" + numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG + ":"
            + numberOfTuplesToProducePerBuffer->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}
bool ParquetSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<ParquetSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<ParquetSourceType>();
    return filePath->getValue() == otherSourceConfig->filePath->getValue()
        && numberOfBuffersToProduce->getValue() == otherSourceConfig->numberOfBuffersToProduce->getValue()
        && numberOfTuplesToProducePerBuffer->getValue() == otherSourceConfig->numberOfTuplesToProducePerBuffer->getValue();
}
void ParquetSourceType::reset() {
    setFilePath(filePath->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer->getDefaultValue());
}
std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> ParquetSourceType::getNumberOfTuplesToProducePerBuffer() const {
    return numberOfTuplesToProducePerBuffer;
}
std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> ParquetSourceType::getNumberOfBuffersToProduce() const {
    return numberOfBuffersToProduce;
}
std::shared_ptr<Configurations::ConfigurationOption<std::string>> ParquetSourceType::getFilePath() const {
    return filePath;
}
}//namespace NES
#endif