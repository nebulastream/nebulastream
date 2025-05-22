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

#include <cstddef>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Overloaded.hpp>
#include <InputFormatterTestUtil.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::InputFormatterTestUtil
{

std::shared_ptr<Schema> createSchema(const std::vector<TestDataTypes>& testDataTypes)
{
    auto schema = std::make_shared<Schema>();
    for (size_t fieldNumber = 1; const auto& dataType : testDataTypes)
    {
        switch (dataType)
        {
            case TestDataTypes::UINT8:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::UINT8));
                break;
            case TestDataTypes::UINT16:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::UINT16));
                break;
            case TestDataTypes::UINT32:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::UINT32));
                break;
            case TestDataTypes::UINT64:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::UINT64));
                break;
            case TestDataTypes::INT8:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::INT8));
                break;
            case TestDataTypes::INT16:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::INT16));
                break;
            case TestDataTypes::INT32:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::INT32));
                break;
            case TestDataTypes::INT64:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::INT64));
                break;
            case TestDataTypes::FLOAT32:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::FLOAT32));
                break;
            case TestDataTypes::FLOAT64:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::FLOAT64));
                break;
            case TestDataTypes::BOOLEAN:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::BOOLEAN));
                break;
            case TestDataTypes::CHAR:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::CHAR));
                break;
            case TestDataTypes::VARSIZED:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeProvider::provideDataType(LogicalType::VARSIZED));
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

Sources::ParserConfig validateAndFormatParserConfig(const std::unordered_map<std::string, std::string>& parserConfig)
{
    auto validParserConfig = Sources::ParserConfig{};
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
    const std::string& filePath,
    std::shared_ptr<Schema> schema,
    std::shared_ptr<Memory::BufferManager> sourceBufferPool,
    const int numberOfLocalBuffersInSource)
{
    std::unordered_map<std::string, std::string> fileSourceConfiguration{{"filePath", filePath}};
    auto validatedSourceConfiguration = Sources::SourceValidationProvider::provide("File", std::move(fileSourceConfiguration));

    const auto sourceDescriptor = Sources::SourceDescriptor(
        std::move(schema),
        "TestSource",
        "File",
        numberOfLocalBuffersInSource,
        Sources::ParserConfig{},
        std::move(validatedSourceConfiguration));

    return Sources::SourceProvider::lower(NES::OriginId(1), sourceDescriptor, std::move(sourceBufferPool), -1);
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
    if (file_size(file1) != file_size(file2))
    {
        NES_ERROR("File sizes do not match: {} vs. {}.", std::filesystem::file_size(file1), std::filesystem::file_size(file2));
        return false;
    }

    std::ifstream f1(file1, std::ifstream::binary);
    std::ifstream f2(file2, std::ifstream::binary);

    return std::equal(std::istreambuf_iterator(f1.rdbuf()), std::istreambuf_iterator<char>(), std::istreambuf_iterator(f2.rdbuf()));
}

Runtime::Execution::TestablePipelineTask createInputFormatterTask(
    const SequenceNumber sequenceNumber,
    const WorkerThreadId workerThreadId,
    Memory::TupleBuffer taskBuffer,
    std::shared_ptr<InputFormatters::InputFormatterTask> inputFormatterTask)
{
    taskBuffer.setSequenceNumber(sequenceNumber);
    return Runtime::Execution::TestablePipelineTask(workerThreadId, taskBuffer, std::move(inputFormatterTask));
}

}
