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
#include <API/Schema.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Benchmarking/Parsing/SynopsisArguments.hpp>
#include <Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <fstream>

namespace NES::ASP::Util {

void writeHeaderToCsvFile(const std::string& csvFileName, const std::string& header) {
    std::ofstream ofstream(csvFileName, std::ios::trunc | std::ios::out);
    ofstream << header << std::endl;
    ofstream.close();
}

void writeRowToCsvFile(const std::string& csvFileName, const std::string& row) {
    std::ofstream ofstream(csvFileName, std::ios::app | std::ios::out);
    ofstream << row << std::endl;
    ofstream.close();
}

std::vector<SynopsisArguments> parseSynopsisArguments(const Yaml::Node& synopsesNode) {
    std::vector<SynopsisArguments> retVector;

    for (auto entry = synopsesNode.Begin(); entry != synopsesNode.End(); entry++) {
        auto node = (*entry).second;
        retVector.emplace_back(SynopsisArguments::createArgumentsFromYamlNode(node));
    }

    return retVector;
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

Runtime::Execution::MemoryProvider::MemoryProviderPtr createMemoryProvider(const uint64_t bufferSize, const SchemaPtr schema) {
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        auto rowMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferSize);
        return std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(rowMemoryLayout);
    } else if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        auto columnMemoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bufferSize);
        return std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(columnMemoryLayout);
    } else {
        NES_NOT_IMPLEMENTED();
    }

    return NES::Runtime::Execution::MemoryProvider::MemoryProviderPtr();
}

}