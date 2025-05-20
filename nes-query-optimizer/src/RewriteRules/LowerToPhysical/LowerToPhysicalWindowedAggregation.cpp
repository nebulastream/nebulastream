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

#include <cstdint>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/Function/AggregationFunction.hpp>
#include <Aggregation/Function/AvgAggregationFunction.hpp>
#include <Aggregation/Function/CountAggregationFunction.hpp>
#include <Aggregation/Function/MaxAggregationFunction.hpp>
#include <Aggregation/Function/MedianAggregationFunction.hpp>
#include <Aggregation/Function/MinAggregationFunction.hpp>
#include <Aggregation/Function/SumAggregationFunction.hpp>
#include <Aggregation/WindowAggregation.hpp>
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES
{

static std::pair<std::vector<Record::RecordFieldIdentifier>, std::vector<Record::RecordFieldIdentifier>>
getKeyAndValueFields(const WindowedAggregationLogicalOperator& logicalOperator)
{
    std::vector<Record::RecordFieldIdentifier> fieldKeyNames;
    std::vector<Record::RecordFieldIdentifier> fieldValueNames;

    /// Getting the key and value field names
    for (const auto& nodeAccess : logicalOperator.getGroupingKeys())
    {
        fieldKeyNames.emplace_back(nodeAccess.getFieldName());
    }
    for (const auto& descriptor : logicalOperator.getWindowAggregation())
    {
        const auto aggregationResultFieldIdentifier = descriptor->onField.getFieldName();
        fieldValueNames.emplace_back(aggregationResultFieldIdentifier);
    }
    return {fieldKeyNames, fieldValueNames};
}

static std::unique_ptr<TimeFunction> getTimeFunction(const WindowedAggregationLogicalOperator& logicalOperator)
{
    auto* const timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(logicalOperator.getWindowType().get());
    if (timeWindow == nullptr)
    {
        throw UnknownWindowType("Window type is not a time based window type");
    }

    switch (timeWindow->getTimeCharacteristic().getType())
    {
        case Windowing::TimeCharacteristic::Type::IngestionTime: {
            if (timeWindow->getTimeCharacteristic().field->getName() == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
            {
                return std::make_unique<IngestionTimeFunction>();
            }
            throw UnknownWindowType(
                "The ingestion time field of a window must be: {}", Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME);
        }
        case Windowing::TimeCharacteristic::Type::EventTime: {
            /// For event time fields, we look up the reference field name and create an expression to read the field.
            auto timeCharacteristicField = timeWindow->getTimeCharacteristic().field->getName();
            auto timeStampField = FieldAccessPhysicalFunction(timeCharacteristicField);
            return std::make_unique<EventTimeFunction>(timeStampField, timeWindow->getTimeCharacteristic().getTimeUnit());
        }
        default: {
            throw UnknownWindowType("Unknown window type: {}", magic_enum::enum_name(timeWindow->getTimeCharacteristic().getType()));
        }
    }
}

static std::vector<std::shared_ptr<AggregationFunction>>
getAggregationFunctions(const WindowedAggregationLogicalOperator& logicalOperator, const NES::Configurations::QueryOptimizerConfiguration&)
{
    std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions;
    const auto& aggregationDescriptors = logicalOperator.getWindowAggregation();
    for (const auto& descriptor : aggregationDescriptors)
    {
        const DefaultPhysicalTypeFactory physicalTypeFactory;

        auto physicalInputType = physicalTypeFactory.getPhysicalType(descriptor->getInputStamp());
        auto physicalFinalType = physicalTypeFactory.getPhysicalType(descriptor->getFinalAggregateStamp());

        auto aggregationInputExpression = QueryCompilation::FunctionProvider::lowerFunction(descriptor->onField);
        const auto aggregationResultFieldIdentifier = descriptor->asField.getFieldName();
        auto name = descriptor->getName();
        if (name == "Avg")
        {
            /// We assume that the count is a u64
            auto countType = physicalTypeFactory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::UINT64));
            aggregationFunctions.emplace_back(std::make_shared<AvgAggregationFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                std::move(aggregationInputExpression),
                aggregationResultFieldIdentifier,
                std::move(countType)));
        }
        else if (name == "Sum")
        {
            aggregationFunctions.emplace_back(std::make_shared<SumAggregationFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                std::move(aggregationInputExpression),
                aggregationResultFieldIdentifier));
        }
        else if (name == "Count")
        {
            /// We assume that a count is a u64
            auto countType = physicalTypeFactory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::UINT64));
            aggregationFunctions.emplace_back(std::make_shared<CountAggregationFunction>(
                std::move(countType),
                std::move(physicalFinalType),
                std::move(aggregationInputExpression),
                aggregationResultFieldIdentifier));
        }
        else if (name == "Max")
        {
            aggregationFunctions.emplace_back(std::make_shared<MaxAggregationFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                std::move(aggregationInputExpression),
                aggregationResultFieldIdentifier));
        }
        else if (name == "Min")
        {
            aggregationFunctions.emplace_back(std::make_shared<MinAggregationFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                std::move(aggregationInputExpression),
                aggregationResultFieldIdentifier));
        }
        else if (name == "Median")
        {
            auto layout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(
                logicalOperator.getInputSchemas()[0], NES::Configurations::DEFAULT_PAGED_VECTOR_SIZE);
            auto memoryProvider = std::make_unique<ColumnTupleBufferMemoryProvider>(layout);
            aggregationFunctions.emplace_back(std::make_shared<MedianAggregationFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                std::move(aggregationInputExpression),
                aggregationResultFieldIdentifier,
                std::move(memoryProvider)));
        }
        else
        {
            throw NotImplemented();
        }
    }
    return aggregationFunctions;
}

RewriteRuleResultSubgraph LowerToPhysicalWindowedAggregation::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<WindowedAggregationLogicalOperator>(), "Expected a WindowedAggregationLogicalOperator");
    PRECONDITION(logicalOperator.getInputOriginIds().size() == 1, "Expected one origin id vector");
    PRECONDITION(logicalOperator.getOutputOriginIds().size() == 1, "Expected one output origin id");
    PRECONDITION(logicalOperator.getInputSchemas().size() == 1, "Expected one input schema");

    auto aggregation = logicalOperator.get<WindowedAggregationLogicalOperator>();
    auto handlerId = getNextOperatorHandlerId();
    auto inputSchema = aggregation.getInputSchemas()[0];
    auto outputSchema = aggregation.getOutputSchema();
    auto inputOriginIds = aggregation.getInputOriginIds()[0];
    auto outputOriginId = aggregation.getOutputOriginIds()[0];
    auto timeFunction = getTimeFunction(aggregation);
    auto* windowType = dynamic_cast<Windowing::TimeBasedWindowType*>(aggregation.getWindowType().get());
    auto aggregationFunctions = getAggregationFunctions(aggregation, conf);

    const auto valueSize = std::accumulate(
        aggregationFunctions.begin(),
        aggregationFunctions.end(),
        0,
        [](const auto& sum, const auto& function) { return sum + function->getSizeOfStateInBytes(); });

    uint64_t keySize = 0;
    std::vector<PhysicalFunction> keyFunctions;
    for (auto& nodeFunctionKey : aggregation.getGroupingKeys())
    {
        const DefaultPhysicalTypeFactory typeFactory;
        const auto& loweredFunctionType = nodeFunctionKey.getDataType();
        keyFunctions.emplace_back(QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionKey));
        keySize += typeFactory.getPhysicalType(loweredFunctionType)->size();
    }
    const auto entrySize = sizeof(Interface::ChainedHashMapEntry) + keySize + valueSize;
    const auto numberOfBuckets = NES::Configurations::DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES;
    const auto pageSize = conf.pageSize.getValue();
    const auto entriesPerPage = pageSize / entrySize;

    const auto& [fieldKeyNames, fieldValueNames] = getKeyAndValueFields(aggregation);
    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(inputSchema, fieldKeyNames, fieldValueNames);

    const auto windowMetaData = WindowMetaData{aggregation.getWindowStartFieldName(), aggregation.getWindowEndFieldName()};

    auto windowAggregation = std::make_shared<WindowAggregation>(
        aggregationFunctions, std::make_unique<Interface::MurMur3HashFunction>(), fieldKeys, fieldValues, entriesPerPage, entrySize);

    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType->getSize().getTime(), windowType->getSlide().getTime(), inputOriginIds.size());
    auto handler = std::make_shared<AggregationOperatorHandler>(inputOriginIds, outputOriginId, std::move(sliceAndWindowStore));
    handler->setHashMapParams(keySize, valueSize, pageSize, numberOfBuckets);

    auto build = AggregationBuildPhysicalOperator(handlerId, std::move(timeFunction), keyFunctions, windowAggregation);
    auto probe = AggregationProbePhysicalOperator(windowAggregation, handlerId, windowMetaData);

    auto buildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        build, inputSchema, outputSchema, handlerId, handler, PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        probe, inputSchema, outputSchema, handlerId, handler, PhysicalOperatorWrapper::PipelineLocation::SCAN, std::vector{buildWrapper});

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), buildWrapper);
    return {.root = probeWrapper, .leafs = {leafes}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterWindowedAggregationRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalWindowedAggregation>(argument.conf);
}
}
