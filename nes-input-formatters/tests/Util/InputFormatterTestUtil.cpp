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
#include <numeric>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <TestTaskQueue.hpp>
#include "Util/Ranges.hpp"
#include "Identifiers/Identifier.hpp"

namespace NES::InputFormatterTestUtil
{

UnboundSchema createSchema(const std::vector<TestDataTypes>& testDataTypes)
{
    const auto fieldNamesOther = testDataTypes | NES::views::enumerate
        | std::views::transform([](const auto& idxDataTypePair) { return Identifier::parse(fmt::format("Field_{}", std::get<0>(idxDataTypePair))); })
        | std::ranges::to<std::vector>();

    return createSchema(testDataTypes, fieldNamesOther);
}

UnboundSchema createSchema(const std::vector<TestDataTypes>& testDataTypes, const std::vector<Identifier>& testFieldNames)
{
    static const std::unordered_map<TestDataTypes, DataType::Type> testDataTypeToNormalDataType
        = {{TestDataTypes::INT8, DataType::Type::INT8},
           {TestDataTypes::UINT8, DataType::Type::UINT8},
           {TestDataTypes::INT16, DataType::Type::INT16},
           {TestDataTypes::UINT16, DataType::Type::UINT16},
           {TestDataTypes::INT32, DataType::Type::INT32},
           {TestDataTypes::UINT32, DataType::Type::UINT32},
           {TestDataTypes::INT64, DataType::Type::INT64},
           {TestDataTypes::UINT64, DataType::Type::UINT64},
           {TestDataTypes::FLOAT32, DataType::Type::FLOAT32},
           {TestDataTypes::FLOAT64, DataType::Type::FLOAT64},
           {TestDataTypes::BOOLEAN, DataType::Type::BOOLEAN},
           {TestDataTypes::CHAR, DataType::Type::CHAR},
           {TestDataTypes::VARSIZED, DataType::Type::VARSIZED}};

    const auto fields = testDataTypes | NES::views::enumerate | std::views::transform(
                      [&testFieldNames](const auto& idxDataTypePair)
    {
        const auto& [fieldNumber, dataType] = idxDataTypePair;
        return UnboundField{testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(testDataTypeToNormalDataType.at(dataType))};
    });
    return UnboundSchema{fields | std::ranges::to<std::vector>()};
}

SourceReturnType::EmitFunction getEmitFunction(ThreadSafeVector<TupleBuffer>& resultBuffers)
{
    return [&resultBuffers](
               const OriginId, SourceReturnType::SourceReturnType returnType, const std::stop_token&) -> SourceReturnType::EmitResult
    {
        std::visit(
            Overloaded{
                [&](const SourceReturnType::Data& data) { resultBuffers.emplace_back(data.buffer); },
                [](const SourceReturnType::EoS&) { NES_DEBUG("Reached EoS in source"); },
                [](const SourceReturnType::Error& error) { throw error.ex; }},
            returnType);
        return SourceReturnType::EmitResult::SUCCESS;
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
    if (const auto tupleDelimiter = parserConfig.find("tuple_delimiter"); tupleDelimiter != parserConfig.end())
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
    if (const auto fieldDelimiter = parserConfig.find("field_delimiter"); fieldDelimiter != parserConfig.end())
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

std::unique_ptr<SourceHandle> createFileSource(
    SourceCatalog& sourceCatalog,
    const std::string& filePath,
    const UnboundSchema& schema,
    std::shared_ptr<BufferManager> sourceBufferPool,
    const size_t numberOfRequiredSourceBuffers)
{
    std::unordered_map<std::string, std::string> fileSourceConfiguration{
        {"file_path", filePath}, {"max_inflight_buffers", std::to_string(numberOfRequiredSourceBuffers)}};
    const auto logicalSource = sourceCatalog.addLogicalSource(Identifier::parse("TestSource"), schema);
    INVARIANT(logicalSource.has_value(), "TestSource already existed");
    const auto sourceDescriptor
        = sourceCatalog.addPhysicalSource(logicalSource.value(), "File", std::move(fileSourceConfiguration), {{"type", "CSV"}});
    INVARIANT(sourceDescriptor.has_value(), "Test File Source couldn't be created");

    const SourceProvider sourceProvider(numberOfRequiredSourceBuffers, std::move(sourceBufferPool));
    return sourceProvider.lower(NES::OriginId(1), sourceDescriptor.value());
}

std::shared_ptr<InputFormatterTaskPipeline> createInputFormatterTask(const UnboundSchema& schema, std::string formatterType)
{
    const std::unordered_map<std::string, std::string> parserConfiguration{
        {"type", std::move(formatterType)}, {"tuple_delimiter", "\n"}, {"field_delimiter", "|"}};
    const auto validatedParserConfiguration = validateAndFormatParserConfig(parserConfiguration);

    return provideInputFormatterTask(schema, validatedParserConfiguration);
}

void waitForSource(const std::vector<TupleBuffer>& resultBuffers, const size_t numExpectedBuffers)
{
    /// Wait for the file source to fill all expected tuple buffers. Timeout after 1 second (it should never take that long).
    const auto timeout = std::chrono::seconds(1);
    const auto startTime = std::chrono::steady_clock::now();
    while (resultBuffers.size() < numExpectedBuffers and (std::chrono::steady_clock::now() - startTime < timeout))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

TestPipelineTask createInputFormatterTask(
    const SequenceNumber sequenceNumber,
    const WorkerThreadId workerThreadId,
    TupleBuffer taskBuffer,
    std::shared_ptr<InputFormatterTaskPipeline> inputFormatterTask)
{
    taskBuffer.setSequenceNumber(sequenceNumber);
    return TestPipelineTask{workerThreadId, taskBuffer, std::move(inputFormatterTask)};
}

}
