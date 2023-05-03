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
#include <API/AttributeField.hpp>
#include <Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Benchmarking/Parsing/SynopsisArguments.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
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

std::vector<Benchmarking::YamlAggregation> parseAggregations(const Yaml::Node& aggregationsNode,
                                                             const std::filesystem::path& data) {
    std::vector<Benchmarking::YamlAggregation> parsedAggregations;

    for (auto entry = aggregationsNode.Begin(); entry != aggregationsNode.End(); entry++) {
        auto node = (*entry).second;
        parsedAggregations.emplace_back(Benchmarking::YamlAggregation::createAggregationFromYamlNode(node, data));
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

Runtime::Execution::MemoryProvider::MemoryProviderPtr createMemoryProvider(const uint64_t bufferSize, const SchemaPtr schema) {
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        auto rowMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferSize);
        return std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(rowMemoryLayout);
    } else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        auto columnMemoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bufferSize);
        return std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(columnMemoryLayout);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

Runtime::MemoryLayouts::DynamicTupleBuffer createDynamicTupleBuffer(Runtime::TupleBuffer buffer, const SchemaPtr& schema) {
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
        return Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    } else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, buffer.getBufferSize());
        return Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

std::vector<PhysicalTypePtr> getPhysicalTypes(SchemaPtr schema) {
    std::vector<PhysicalTypePtr> retVector;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        retVector.push_back(physicalField);
    }

    return retVector;
}

std::vector<Runtime::TupleBuffer> createBuffersFromCSVFile(const std::string& csvFile, const SchemaPtr& schema,
                                                                       Runtime::BufferManagerPtr bufferManager,
                                                                       const std::string& timeStampFieldName,
                                                                       uint64_t lastTimeStamp) {
    std::vector<Runtime::TupleBuffer> recordBuffers;
    NES_ASSERT2_FMT(std::filesystem::exists(std::filesystem::path(csvFile)), "CSVFile " << csvFile << " does not exist!!!");

    // Creating everything for the csv parser
    std::ifstream file(csvFile);
    std::istream_iterator<std::string> beginIt(file);
    std::istream_iterator<std::string> endIt;
    const std::string delimiter = ",";
    auto parser = std::make_shared<CSVParser>(schema->fields.size(), getPhysicalTypes(schema), delimiter);


    // Do-while loop for checking, if we have another line to parse from the inputFile
    const auto maxTuplesPerBuffer = bufferManager->getBufferSize() / schema->getSchemaSizeInBytes();
    auto it = beginIt;
    auto tupleCount = 0UL;
    auto buffer = bufferManager->getBufferBlocking();
    do {

        std::string line = *it;
        auto dynamicTupleBuffer = ASP::Util::createDynamicTupleBuffer(buffer, schema);
        parser->writeInputTupleToTupleBuffer(line, tupleCount, dynamicTupleBuffer, schema, bufferManager);
        ++tupleCount;

        // If we have read enough tuples from the csv file, then stop iterating over it
        auto curTimeStamp = dynamicTupleBuffer[tupleCount - 1][timeStampFieldName].read<uint64_t>();
        if (curTimeStamp >= lastTimeStamp) {
            break;
        }


        if (tupleCount >= maxTuplesPerBuffer) {
            buffer.setNumberOfTuples(tupleCount);
            recordBuffers.emplace_back(buffer);
            buffer = bufferManager->getBufferBlocking();
            tupleCount = 0UL;
        }
        ++it;
    } while(it != endIt);

    if (tupleCount > 0) {
        buffer.setNumberOfTuples(tupleCount);
        recordBuffers.emplace_back(buffer);
    }

    return recordBuffers;
}
} // namespace NES::ASP::Util