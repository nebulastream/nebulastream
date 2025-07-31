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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.hpp>

#include <cstdint>
#include <memory>
#include <numeric>
#include <ranges>
#include <utility>
#include <vector>

#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <magic_enum/magic_enum.hpp>
#include "Util/SchemaFactory.hpp"

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <HashMapOptions.hpp>
#include <PhysicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

// static std::pair<std::vector<Record::RecordFieldIdentifier>, std::vector<Record::RecordFieldIdentifier>>
// getKeyAndValueFields(const WindowedAggregationLogicalOperator& logicalOperator)
// {
//     std::vector<Record::RecordFieldIdentifier> fieldKeyNames;
//     std::vector<Record::RecordFieldIdentifier> fieldValueNames;
//
//     /// Getting the key and value field names
//     for (const auto& nodeAccess : logicalOperator.getGroupingKeys())
//     {
//         fieldKeyNames.emplace_back(nodeAccess.getFieldName());
//     }
//     for (const auto& descriptor : logicalOperator.getWindowAggregation())
//     {
//         const auto aggregationResultFieldIdentifier = descriptor->getOnField().getFieldName();
//         fieldValueNames.emplace_back(aggregationResultFieldIdentifier);
//     }
//     return {fieldKeyNames, fieldValueNames};
// }
//
// static std::unique_ptr<TimeFunction> getTimeFunction(const WindowedAggregationLogicalOperator& logicalOperator)
// {
//     auto* const timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(logicalOperator.getWindowType().get());
//     if (timeWindow == nullptr)
//     {
//         throw UnknownWindowType("Window type is not a time based window type");
//     }
//
//     switch (timeWindow->getTimeCharacteristic().getType())
//     {
//         case Windowing::TimeCharacteristic::Type::IngestionTime: {
//             if (timeWindow->getTimeCharacteristic().field.name == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
//             {
//                 return std::make_unique<IngestionTimeFunction>();
//             }
//             throw UnknownWindowType(
//                 "The ingestion time field of a window must be: {}", Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME);
//         }
//         case Windowing::TimeCharacteristic::Type::EventTime: {
//             /// For event time fields, we look up the reference field name and create an expression to read the field.
//             auto timeCharacteristicField = timeWindow->getTimeCharacteristic().field.name;
//             auto timeStampField = FieldAccessPhysicalFunction(timeCharacteristicField);
//             return std::make_unique<EventTimeFunction>(timeStampField, timeWindow->getTimeCharacteristic().getTimeUnit());
//         }
//         default: {
//             throw UnknownWindowType("Unknown window type: {}", magic_enum::enum_name(timeWindow->getTimeCharacteristic().getType()));
//         }
//     }
// }

namespace
{
std::vector<std::shared_ptr<AggregationPhysicalFunction>>
getAggregationPhysicalFunctions(const WindowedAggregationLogicalOperator& logicalOperator, const QueryExecutionConfiguration& configuration)
{
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    const auto& aggregationDescriptors = logicalOperator.getWindowAggregation();
    for (const auto& descriptor : aggregationDescriptors)
    {
        auto physicalInputType = DataTypeProvider::provideDataType(descriptor.function->getInputFunction().getDataType().type);
        auto physicalFinalType = DataTypeProvider::provideDataType(descriptor.function->getInputFunction().getDataType().type);

        auto aggregationInputFunction = QueryCompilation::FunctionProvider::lowerFunction(descriptor.function->getInputFunction());
        const auto resultFieldIdentifier = descriptor.name;
        const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
        PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
        const auto memoryLayoutType = memoryLayoutTypeTrait.value().memoryLayout;
        auto bufferRef = LowerSchemaProvider::lowerSchema(
            configuration.pageSize.getValue(), createPhysicalOutputSchema(logicalOperator.getTraitSet()), memoryLayoutType);
        // auto aggregationInputFunction = QueryCompilation::FunctionProvider::lowerFunction(descriptor.function->getInputFunction());
        // const auto resultFieldIdentifier = descriptor.name;
        // auto layout = std::make_shared<ColumnLayout>(configuration.pageSize.getValue(), logicalOperator.getChild().getOutputSchema().unbind<std::dynamic_extent>());
        // auto bufferRef = std::make_shared<Interface::BufferRef::ColumnTupleBufferRef>(layout);

        auto name = descriptor.function->getName();
        auto aggregationArguments = AggregationPhysicalFunctionRegistryArguments(
            std::move(physicalInputType),
            std::move(physicalFinalType),
            std::move(aggregationInputFunction),
            resultFieldIdentifier,
            bufferRef);
        if (auto aggregationPhysicalFunction
            = AggregationPhysicalFunctionRegistry::instance().create(std::string{name}, std::move(aggregationArguments)))
        {
            aggregationPhysicalFunctions.push_back(aggregationPhysicalFunction.value());
        }
        else
        {
            throw UnknownAggregationType("unknown aggregation type: {}", name);
        }
    }
    return aggregationPhysicalFunctions;
}
}

RewriteRuleResultSubgraph LowerToPhysicalWindowedAggregation::apply(LogicalOperator logicalOperator)
{
    auto aggregation = logicalOperator.getAs<WindowedAggregationLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto childTraitSet = aggregation->getChild().getTraitSet();

    auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    auto inputOriginIds = childTraitSet.get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(outputOriginIds) == 1, "Expected one output origin id");
    auto outputOriginId = outputOriginIds[0];

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait.memoryLayout;

    PRECONDITION(
        std::holds_alternative<Windowing::BoundTimeCharacteristic>(aggregation->getCharacteristic()),
        "Expected time characteristic to be bound");

    auto handlerId = getNextOperatorHandlerId();
    auto timeFunction = TimeFunction::create(std::get<Windowing::BoundTimeCharacteristic>(aggregation->getCharacteristic()));
    auto windowType = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(aggregation->getWindowType());
    INVARIANT(windowType != nullptr, "Window type must be a time-based window type");
    auto aggregationPhysicalFunctions = getAggregationPhysicalFunctions(*aggregation, conf);

    const auto physicalInputSchema = createPhysicalOutputSchema(childTraitSet);
    const auto physicalOutputSchema = createPhysicalOutputSchema(traitSet);

    const auto valueSize = std::accumulate(
        aggregationPhysicalFunctions.begin(),
        aggregationPhysicalFunctions.end(),
        0,
        [](const auto& sum, const auto& function) { return sum + function->getSizeOfStateInBytes(); });

    uint64_t keySize = 0;
    std::vector<PhysicalFunction> keyFunctions;
    std::unordered_map<IdentifierList, UnboundField> newInputFields
        = physicalInputSchema
        | std::views::transform(
              [](const auto& field)
              { return std::make_pair(field.getFullyQualifiedName(), UnboundField{field.getFullyQualifiedName(), field.getDataType()}); })
        | std::ranges::to<std::unordered_map>();
    auto groupingKeys = aggregation->getGroupingKeys();
    const auto keysAreBound = std::holds_alternative<std::vector<FieldAccessLogicalFunction>>(groupingKeys);
    PRECONDITION(keysAreBound, "Expected the grouping keys to be bound");

    // Not sure if this is 100% correct in regard to the field mappings
    auto boundGroupingKeys = std::get<std::vector<FieldAccessLogicalFunction>>(groupingKeys);
    for (auto& nodeFunctionKey : boundGroupingKeys)
    {
        auto loweredFunctionType = nodeFunctionKey.getDataType();
        if (loweredFunctionType.isType(DataType::Type::VARSIZED))
        {
            loweredFunctionType.type = DataType::Type::VARSIZED_POINTER_REP;
            auto fieldNode = newInputFields.extract(nodeFunctionKey.getField().getLastName());
            PRECONDITION(!fieldNode.empty(), "Expect to find the field {} in the input schema", nodeFunctionKey.getField().getLastName());
            fieldNode.mapped() = UnboundField{fieldNode.mapped().getFullyQualifiedName(), loweredFunctionType};
            newInputFields.insert(std::move(fieldNode));
        }
        keyFunctions.emplace_back(QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionKey));
        keySize += DataTypeProvider::provideDataType(loweredFunctionType.type).getSizeInBytes();
    }
    const auto entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
    const auto numberOfBuckets = conf.numberOfPartitions.getValue();
    const auto pageSize = conf.pageSize.getValue();
    const auto entriesPerPage = pageSize / entrySize;

    const auto fieldKeyNames
        = boundGroupingKeys | std::views::transform([](const auto& field) { return IdentifierList{field.getField().getLastName()}; });
    const auto fieldValueNames = aggregation->getWindowAggregation()
        | std::views::transform([](const auto& descriptor) { return IdentifierList{descriptor.name}; });
    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(
        physicalInputSchema, fieldKeyNames | std::ranges::to<std::vector>(), fieldValueNames | std::ranges::to<std::vector>());

    const auto windowMetaData = WindowMetaData{aggregation->getWindowStartField(), aggregation->getWindowEndField()};

    const HashMapOptions hashMapOptions(
        std::make_unique<MurMur3HashFunction>(),
        keyFunctions,
        fieldKeys,
        fieldValues,
        entriesPerPage,
        entrySize,
        keySize,
        valueSize,
        pageSize,
        numberOfBuckets);

    auto sliceAndWindowStore
        = std::make_unique<DefaultTimeBasedSliceStore>(windowType->getSize().getTime(), windowType->getSlide().getTime());
    auto handler = std::make_shared<AggregationOperatorHandler>(
        inputOriginIds | std::ranges::to<std::vector>(), outputOriginId, std::move(sliceAndWindowStore), conf.maxNumberOfBuckets);
    auto build = AggregationBuildPhysicalOperator(handlerId, std::move(timeFunction), aggregationPhysicalFunctions, hashMapOptions);
    auto probe = AggregationProbePhysicalOperator(hashMapOptions, aggregationPhysicalFunctions, handlerId, windowMetaData);

    auto buildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        build,
        physicalInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        probe,
        physicalInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{buildWrapper});

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
