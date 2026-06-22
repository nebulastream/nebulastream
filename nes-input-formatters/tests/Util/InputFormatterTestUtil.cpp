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
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <BackpressureChannel.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterTupleBufferRefProvider.hpp>
#include <InputFormatterValidationProvider.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <ScanPhysicalOperator.hpp>
#include <TestTaskQueue.hpp>

namespace NES::InputFormatterTestUtil
{

Schema createSchema(const std::vector<TestDataTypes>& testDataTypes)
{
    const auto fieldNamesOther = testDataTypes | NES::views::enumerate
        | std::views::transform([](const auto& idxDataTypePair) { return fmt::format("Field_{}", std::get<0>(idxDataTypePair)); })
        | std::ranges::to<std::vector>();

    return createSchema(testDataTypes, fieldNamesOther);
}

Schema createSchema(const std::vector<TestDataTypes>& testDataTypes, const std::vector<std::string>& testFieldNames)
{
    auto schema = Schema{};
    for (const auto& [fieldNumber, dataType] : testDataTypes | NES::views::enumerate)
    {
        switch (dataType)
        {
            case TestDataTypes::UINT8:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::UINT8));
                break;
            case TestDataTypes::UINT16:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::UINT16));
                break;
            case TestDataTypes::UINT32:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::UINT32));
                break;
            case TestDataTypes::UINT64:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::UINT64));
                break;
            case TestDataTypes::INT8:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::INT8));
                break;
            case TestDataTypes::INT16:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::INT16));
                break;
            case TestDataTypes::INT32:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::INT32));
                break;
            case TestDataTypes::INT64:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::INT64));
                break;
            case TestDataTypes::FLOAT32:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::FLOAT32));
                break;
            case TestDataTypes::FLOAT64:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::FLOAT64));
                break;
            case TestDataTypes::BOOLEAN:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::BOOLEAN));
                break;
            case TestDataTypes::CHAR:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::CHAR));
                break;
            case TestDataTypes::VARSIZED:
                schema.addField(testFieldNames.at(fieldNumber), DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
                break;
        }
    }
    return schema;
}

SourceReturnType::EmitFunction getEmitFunction(ThreadSafeVector<TupleBuffer>& resultBuffers)
{
    return [&resultBuffers](
               const OriginId, SourceReturnType::SourceReturnType returnType, const std::stop_token&) -> SourceReturnType::EmitResult
    {
        std::visit(
            Overloaded{
                [&](SourceReturnType::Data data) { resultBuffers.emplace_back(std::move(data.buffer)); },
                [](SourceReturnType::EoS) { NES_DEBUG("Reached EoS in source"); },
                [](SourceReturnType::Error error) { throw std::move(error.ex); },
                [](SourceReturnType::Execute) { throw NotImplemented("sequential execution not implemented"); }},
            std::move(returnType));
        return SourceReturnType::EmitResult::SUCCESS;
    };
}

std::pair<BackpressureController, std::unique_ptr<SourceHandle>> createFileSource(
    SourceCatalog& sourceCatalog,
    const std::string& filePath,
    const Schema& schema,
    std::shared_ptr<BufferManager> sourceBufferPool,
    const size_t numberOfRequiredSourceBuffers)
{
    std::unordered_map<std::string, std::string> fileSourceConfiguration{
        {"file_path", filePath}, {"max_inflight_buffers", std::to_string(numberOfRequiredSourceBuffers)}};
    const auto logicalSource = sourceCatalog.addLogicalSource("TestSource", schema);
    INVARIANT(logicalSource.has_value(), "TestSource already existed");
    const auto sourceDescriptor = sourceCatalog.addPhysicalSource(
        logicalSource.value(), "File", Host("localhost"), std::move(fileSourceConfiguration), {{"type", "CSV"}});
    INVARIANT(sourceDescriptor.has_value(), "Test File Source couldn't be created");
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    const SourceProvider sourceProvider(numberOfRequiredSourceBuffers, std::move(sourceBufferPool));
    return {
        std::move(backpressureController),
        sourceProvider.lower(NES::OriginId(1), backpressureListener, sourceDescriptor.value(), false, 1)};
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

std::shared_ptr<CompiledExecutablePipelineStage> createInputFormatter(
    const DescriptorConfig::Config& parserConfiguration,
    const Schema& schema,
    const MemoryLayoutType memoryLayoutType,
    const size_t sizeOfFormattedBuffers,
    const bool isCompiled,
    const std::optional<Schema>& projection)
{
    constexpr OperatorHandlerId emitOperatorHandlerId = INITIAL<OperatorHandlerId>;

    /// The formatter parses against the full input `schema`; the emit writes the (possibly narrower)
    /// output schema. Different buffers (raw input vs formatted output), so different layouts are fine.
    const Schema& outputSchema = projection.has_value() ? *projection : schema;
    auto inputProvider = LowerSchemaProvider::lowerSchema(sizeOfFormattedBuffers, schema, memoryLayoutType);
    auto outputProvider = LowerSchemaProvider::lowerSchema(sizeOfFormattedBuffers, outputSchema, memoryLayoutType);
    auto inputFormatterType = std::get<std::string>(parserConfiguration.at("type"));
    auto scanOp = ScanPhysicalOperator(
        provideInputFormatterTupleBufferRef(InputFormatterDescriptor{inputFormatterType, parserConfiguration}, inputProvider),
        schema.getFieldNames());

    PhysicalOperator child = EmitPhysicalOperator(emitOperatorHandlerId, std::move(outputProvider));
    if (projection.has_value())
    {
        /// One identity Map per projected field: Map(f, FieldAccess(f)). Only these reads reach the
        /// record, so the dropped fields' parse invokes become dead code (DCE'd under fnattr).
        for (const auto& fieldName : projection->getFieldNames())
        {
            auto mapOp = MapPhysicalOperator(fieldName, FieldAccessPhysicalFunction(fieldName));
            mapOp.setChild(child);
            child = mapOp;
        }
    }
    scanOp.setChild(child);

    auto physicalScanPipeline = std::make_shared<Pipeline>(std::move(scanOp));
    physicalScanPipeline->getOperatorHandlers().emplace(emitOperatorHandlerId, std::make_shared<EmitOperatorHandler>());

    auto nautilusOptions = nautilus::engine::Options{};
    nautilusOptions.setOption("engine.Compilation", isCompiled);
    nautilusOptions.setOption("mlir.enableMultithreading", false);
    return std::make_shared<CompiledExecutablePipelineStage>(
        physicalScanPipeline, physicalScanPipeline->getOperatorHandlers(), nautilusOptions);
}

}
