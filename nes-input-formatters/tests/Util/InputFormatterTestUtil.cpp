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

#include <InputFormatterTestUtil.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <TestTaskQueue.hpp>

namespace NES::InputFormatterTestUtil
{

Schema createSchema(const std::vector<TestDataTypes>& testDataTypes)
{
    auto schema = Schema{};
    for (size_t fieldNumber = 1; const auto& dataType : testDataTypes)
    {
        switch (dataType)
        {
            case TestDataTypes::UINT8:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::UINT8));
                break;
            case TestDataTypes::UINT16:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::UINT16));
                break;
            case TestDataTypes::UINT32:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::UINT32));
                break;
            case TestDataTypes::UINT64:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::UINT64));
                break;
            case TestDataTypes::INT8:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::INT8));
                break;
            case TestDataTypes::INT16:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::INT16));
                break;
            case TestDataTypes::INT32:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::INT32));
                break;
            case TestDataTypes::INT64:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::INT64));
                break;
            case TestDataTypes::FLOAT32:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::FLOAT32));
                break;
            case TestDataTypes::FLOAT64:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::FLOAT64));
                break;
            case TestDataTypes::BOOLEAN:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::BOOLEAN));
                break;
            case TestDataTypes::CHAR:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::CHAR));
                break;
            case TestDataTypes::VARSIZED:
                schema.addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
                break;
        }
        ++fieldNumber;
    }
    return schema;
}

std::function<void(OriginId, Sources::SourceReturnType::SourceReturnType)>
getEmitFunction(ThreadSafeVector<NES::Memory::TupleBuffer>& resultBuffers)
{
    return [&resultBuffers](const OriginId, Sources::SourceReturnType::SourceReturnType returnType)
    {
        std::visit(
            Overloaded{
                [&](const Sources::SourceReturnType::Data& data) { resultBuffers.emplace_back(data.buffer); },
                [](const Sources::SourceReturnType::EoS&) { NES_DEBUG("Reached EoS in source"); },
                [](const Sources::SourceReturnType::Error& error) { throw error.ex; }},
            returnType);
    };
}

ParserConfig validateAndFormatParserConfig(const std::unordered_map<std::string, std::string>& parserConfig)
{
    auto validParserConfig = ParserConfig{};
    if (const auto parserType = parserConfig.find("type"); parserType != parserConfig.end())
    {
        validParserConfig.parserType = parserType->second;
    }
    else
    {
        throw InvalidConfigParameter("Parser configuration must contain: type");
    }
    if (const auto tupleDelimiter = parserConfig.find("tupleDelimiter"); tupleDelimiter != parserConfig.end())
    {
        /// TODO #651: Add full support for tuple delimiters that are larger than one byte.
        PRECONDITION(tupleDelimiter->second.size() == 1, "We currently do not support tuple delimiters larger than one byte.");
        validParserConfig.tupleDelimiter = tupleDelimiter->second;
    }
    else
    {
        NES_DEBUG("Parser configuration did not contain: tupleDelimiter, using default: \\n");
        validParserConfig.tupleDelimiter = '\n';
    }
    if (const auto fieldDelimiter = parserConfig.find("fieldDelimiter"); fieldDelimiter != parserConfig.end())
    {
        validParserConfig.fieldDelimiter = fieldDelimiter->second;
    }
    else
    {
        NES_DEBUG("Parser configuration did not contain: fieldDelimiter, using default: ,");
        validParserConfig.fieldDelimiter = ",";
    }
    return validParserConfig;
}

std::unique_ptr<Sources::SourceHandle> createFileSource(
    SourceCatalog& sourceCatalog,
    const std::string& filePath,
    const Schema& schema,
    std::shared_ptr<Memory::BufferManager> sourceBufferPool,
    const int numberOfLocalBuffersInSource)
{
    std::unordered_map<std::string, std::string> fileSourceConfiguration{
        {"filePath", filePath}, {"numberOfBuffersInLocalPool", std::to_string(numberOfLocalBuffersInSource)}};
    const auto logicalSource = sourceCatalog.addLogicalSource("TestSource", schema);
    INVARIANT(logicalSource.has_value(), "TestSource already existed");
    const auto sourceDescriptor
        = sourceCatalog.addPhysicalSource(logicalSource.value(), "File", std::move(fileSourceConfiguration), ParserConfig{});
    INVARIANT(sourceDescriptor.has_value(), "Test File Source couldn't be created");
    return Sources::SourceProvider::lower(NES::OriginId(1), sourceDescriptor.value(), std::move(sourceBufferPool), -1);
}
std::shared_ptr<InputFormatters::InputFormatterTask> createInputFormatterTask(const Schema& schema)
{
    const std::unordered_map<std::string, std::string> parserConfiguration{
        {"type", "CSV"}, {"tupleDelimiter", "\n"}, {"fieldDelimiter", "|"}};
    auto validatedParserConfiguration = validateAndFormatParserConfig(parserConfiguration);

    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter = InputFormatters::InputFormatterProvider::provideInputFormatter(
        validatedParserConfiguration.parserType,
        schema,
        validatedParserConfiguration.tupleDelimiter,
        validatedParserConfiguration.fieldDelimiter);

    return std::make_shared<InputFormatters::InputFormatterTask>(OriginId(1), std::move(inputFormatter));
}

void waitForSource(const std::vector<NES::Memory::TupleBuffer>& resultBuffers, const size_t numExpectedBuffers)
{
    /// Wait for the file source to fill all expected tuple buffers. Timeout after 1 second (it should never take that long).
    const auto timeout = std::chrono::seconds(1);
    const auto startTime = std::chrono::steady_clock::now();
    while (resultBuffers.size() < numExpectedBuffers and (std::chrono::steady_clock::now() - startTime < timeout))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

bool compareFiles(const std::filesystem::path& file1, const std::filesystem::path& file2)
{
    std::cout << fmt::format("File sizes do not match: {} vs. {}.", file1.c_str(), file2.c_str());

    if (file_size(file1) != file_size(file2))
    {
        std::cout << fmt::format(
            "File sizes do not match: {} vs. {}.", std::filesystem::file_size(file1), std::filesystem::file_size(file2));
        return false;
    }

    std::ifstream f1(file1, std::ifstream::binary);
    std::ifstream f2(file2, std::ifstream::binary);

    return std::equal(std::istreambuf_iterator(f1.rdbuf()), std::istreambuf_iterator<char>(), std::istreambuf_iterator(f2.rdbuf()));
}

TestablePipelineTask createInputFormatterTask(
    const SequenceNumber sequenceNumber,
    const WorkerThreadId workerThreadId,
    Memory::TupleBuffer taskBuffer,
    std::shared_ptr<InputFormatters::InputFormatterTask> inputFormatterTask)
{
    taskBuffer.setSequenceNumber(sequenceNumber);
    return TestablePipelineTask(workerThreadId, taskBuffer, std::move(inputFormatterTask));
}

}
