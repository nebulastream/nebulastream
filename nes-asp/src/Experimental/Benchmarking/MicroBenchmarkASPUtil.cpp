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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Experimental/Parsing/SynopsisConfiguration.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Common.hpp>
#include <Util/yaml/Yaml.hpp>
#include <cmath>
#include <filesystem>
#include <fstream>

namespace NES::ASP::Util {

std::vector<Parsing::SynopsisConfigurationPtr> parseSynopsisConfigurations(const Yaml::Node& synopsesNode) {
    std::vector<Parsing::SynopsisConfigurationPtr> retVector;
    for (auto entry = synopsesNode.Begin(); entry != synopsesNode.End(); entry++) {
        auto node = (*entry).second;
        retVector.emplace_back(Parsing::SynopsisConfiguration::createArgumentsFromYamlNode(node));
    }
    return retVector;
}

std::vector<std::pair<Parsing::SynopsisAggregationConfig, std::string>>
parseAggregations(const Yaml::Node& aggregationsNode, const std::filesystem::path& data) {
    std::vector<std::pair<Parsing::SynopsisAggregationConfig, std::string>> parsedAggregations;
    for (auto entry = aggregationsNode.Begin(); entry != aggregationsNode.End(); entry++) {
        auto node = (*entry).second;
        parsedAggregations.emplace_back(Parsing::SynopsisAggregationConfig::createAggregationFromYamlNode(node, data));
    }
    return parsedAggregations;
}

std::string parseCsvFileFromYaml(const std::string& yamlFileName) {
    Yaml::Node configFile;
    Yaml::Parse(configFile, yamlFileName.c_str());
    if (configFile.IsNone() || configFile["csvFile"].IsNone()) {
        NES_THROW_RUNTIME_ERROR("Could not get csv file from yaml file!");
    }
    return configFile["csvFile"].As<std::string>();
}

uint64_t parseReps(const Yaml::Node& repsNode) { return repsNode.As<uint64_t>(); }

std::vector<uint32_t> parseBufferSizes(const Yaml::Node& bufferSizesNode) {
    return NES::Util::splitWithStringDelimiter<uint32_t>(bufferSizesNode.As<std::string>(), ",");
}

std::vector<uint32_t> parseNumberOfBuffers(const Yaml::Node& numberOfBuffersNode, const uint32_t defaultValue) {
    if (numberOfBuffersNode.IsNone()) {
        return {defaultValue};
    }
    return NES::Util::splitWithStringDelimiter<uint32_t>(numberOfBuffersNode.As<std::string>(), ",");
}

std::vector<size_t> parseWindowSizes(const Yaml::Node& windowSizesNode) {
    return NES::Util::splitWithStringDelimiter<size_t>(windowSizesNode.As<std::string>(), ",");
}

double calculateRelativeError(Runtime::MemoryLayouts::DynamicField& approxField,
                              Runtime::MemoryLayouts::DynamicField& exactField) {
    if (approxField.getPhysicalType()->type != exactField.getPhysicalType()->type) {
        return std::numeric_limits<double>::max();
    }

    auto physicalField = DefaultPhysicalTypeFactory().getPhysicalType(approxField.getPhysicalType()->type);
    auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalField);
    switch (basicType->nativeType) {
        case BasicPhysicalType::NativeType::UINT_8:
            return (std::abs(approxField.read<uint8_t>() - exactField.read<uint8_t>()) / (double)exactField.read<uint8_t>());
        case BasicPhysicalType::NativeType::UINT_16:
            return (std::abs(approxField.read<uint16_t>() - exactField.read<uint16_t>()) / (double)exactField.read<uint16_t>());
        case BasicPhysicalType::NativeType::UINT_32:
            return (std::abs((double)(approxField.read<uint32_t>() - exactField.read<uint32_t>())) / exactField.read<uint32_t>());
        case BasicPhysicalType::NativeType::UINT_64:
            return (std::abs((double)(approxField.read<uint64_t>() - exactField.read<uint64_t>())) / exactField.read<uint64_t>());
        case BasicPhysicalType::NativeType::INT_8:
            return (std::abs(approxField.read<int8_t>() - exactField.read<int8_t>()) / (double)exactField.read<int8_t>());
        case BasicPhysicalType::NativeType::INT_16:
            return (std::abs(approxField.read<int16_t>() - exactField.read<int16_t>()) / (double)exactField.read<int16_t>());
        case BasicPhysicalType::NativeType::INT_32:
            return (std::abs(approxField.read<int32_t>() - exactField.read<int32_t>()) / (double)exactField.read<int32_t>());
        case BasicPhysicalType::NativeType::INT_64:
            return (std::abs(approxField.read<int64_t>() - exactField.read<int64_t>()) / (double)exactField.read<int64_t>());
        case BasicPhysicalType::NativeType::FLOAT:
            return (std::abs(approxField.read<float_t>() - exactField.read<float_t>()) / (double)exactField.read<float_t>());
        case BasicPhysicalType::NativeType::DOUBLE:
            return (std::abs(approxField.read<double_t>() - exactField.read<double_t>()) / (double)exactField.read<double_t>());
        default: NES_THROW_RUNTIME_ERROR("Can not calculate relative error for " << approxField.getPhysicalType()->type);
    }
}
}