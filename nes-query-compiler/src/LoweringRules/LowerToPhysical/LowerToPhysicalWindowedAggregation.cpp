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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <ranges>
#include <span>
#include <utility>
#include <variant>
#include <vector>

#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <magic_enum/magic_enum.hpp>

#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <HashMapOptions.hpp>
#include <HashMapSlice.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{
namespace
{
std::vector<std::shared_ptr<AggregationPhysicalFunction>>
getAggregationPhysicalFunctions(const WindowedAggregationLogicalOperator& logicalOperator, const QueryExecutionConfiguration& configuration)
{
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    const auto& aggregationDescriptors = logicalOperator.getWindowAggregation();

    const auto memoryLayoutTypeTrait = logicalOperator.getChild()->getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    const auto physicalInputSchema = createPhysicalOutputSchema(logicalOperator.getChild()->getTraitSet());
    auto tupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(physicalInputSchema);
    auto bufferRef = LowerSchemaProvider::lowerSchema(configuration.pageSize.getValue(), physicalInputSchema, memoryLayoutType);

    for (const auto& descriptor : aggregationDescriptors)
    {
        const auto fieldIsBound
            = std::holds_alternative<TypedLogicalFunction<FieldAccessLogicalFunction>>(descriptor.function.getInputFunction());
        PRECONDITION(fieldIsBound, "Expected the aggregation function to be bound");
        const auto fieldAccessFunction = std::get<TypedLogicalFunction<FieldAccessLogicalFunction>>(descriptor.function.getInputFunction());

        auto physicalInputType = fieldAccessFunction->getDataType();
        auto physicalFinalType = descriptor.function->getAggregateType();
        auto aggregationInputFunction = QueryCompilation::FunctionProvider::lowerFunction(
            fieldAccessFunction, *logicalOperator.getChild().getTraitSet().get<FieldMappingTrait>());
        const auto resultFieldIdentifier = descriptor.name;
        auto name = descriptor.function->getName();

        auto aggregationArguments = AggregationPhysicalFunctionRegistryArguments(
            std::move(physicalInputType),
            std::move(physicalFinalType),
            std::move(aggregationInputFunction),
            resultFieldIdentifier,
            tupleLayout,
            descriptor.function.shallIncludeNullValues());
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

LoweringRuleResultSubgraph LowerToPhysicalWindowedAggregation::apply(LogicalOperator logicalOperator)
{
    auto aggregation = logicalOperator.getAs<WindowedAggregationLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto childTraitSet = aggregation->getChild().getTraitSet();

    auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    auto inputOriginIds = childTraitSet.get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*outputOriginIds) == 1, "Expected one output origin id");
    auto outputOriginId = (*outputOriginIds)[0];

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    PRECONDITION(
        std::holds_alternative<Windowing::BoundTimeCharacteristic>(aggregation->getCharacteristic()),
        "Expected time characteristic to be bound");

    auto handlerId = getNextOperatorHandlerId();
    auto timeFunction = TimeFunction::create(std::get<Windowing::BoundTimeCharacteristic>(aggregation->getCharacteristic()));
    auto windowType = aggregation->getWindowType();
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
    auto boundGroupingKeys = aggregation->getGroupingKeys();
    for (auto& nodeFunctionKey : boundGroupingKeys)
    {
        auto loweredFunctionType = nodeFunctionKey.getDataType();
        keyFunctions.emplace_back(
            QueryCompilation::FunctionProvider::lowerFunction(
                nodeFunctionKey, *aggregation->getChild().getTraitSet().get<FieldMappingTrait>()));
        keySize += loweredFunctionType.getSizeInBytesWithNull();
    }
    const auto entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
    const auto numberOfBuckets = conf.numberOfPartitions.getValue();
    const auto pageSize = conf.pageSize.getValue();
    const auto entriesPerPage = pageSize / entrySize;

    const auto fieldKeyNames
        = boundGroupingKeys | std::views::transform([](const auto& field) { return QualifiedIdentifier{field->getField().getLastName()}; });
    const auto fieldValueNames
        = aggregation->getWindowAggregation()
        | std::views::transform(
              [](const auto& projectedAggregation) -> QualifiedIdentifier
              {
                  return std::get<TypedLogicalFunction<FieldAccessLogicalFunction>>(projectedAggregation.function->getInputFunction())
                      ->getField()
                      .getLastName();
              });
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

    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType.getSize().getTime(), windowType.getSlide().getTime(), conf.sliceCacheConfiguration);
    auto sliceStoreRef = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId) -> void*
        {
            auto& aggregationSlice = dynamic_cast<AggregationSlice&>(slice);
            auto* ptr = aggregationSlice.getHashMapPtrOrCreate(workerThreadId);
            return ptr;
        },
        /// NOLINTNEXTLINE(bugprone-exception-escape): dynamic_cast<ref> may throw std::bad_cast on bug; non-recoverable here.
        [hashMapOptions](WindowBasedOperatorHandler& handler, AbstractBufferProvider&)
        {
            auto& aggHandler = dynamic_cast<AggregationOperatorHandler&>(handler);
            const CreateNewHashMapSliceArgs hashMapSliceArgs{
                {aggHandler.cleanupStateNautilusFunction},
                hashMapOptions.keySize,
                hashMapOptions.valueSize,
                hashMapOptions.pageSize,
                hashMapOptions.numberOfBuckets};
            return handler.getCreateNewSlicesFunction(hashMapSliceArgs);
        });
    auto handler = std::make_shared<AggregationOperatorHandler>(
        *inputOriginIds | std::ranges::to<std::vector>(), outputOriginId, std::move(sliceAndWindowStore), conf.maxNumberOfBuckets);
    const AggregationBuildPhysicalOperator build{
        handlerId, std::move(timeFunction), std::move(sliceStoreRef), aggregationPhysicalFunctions, hashMapOptions};
    const AggregationProbePhysicalOperator probe{hashMapOptions, aggregationPhysicalFunctions, handlerId, windowMetaData};

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
    std::vector leaves(logicalOperator.getChildren().size(), buildWrapper);
    return {.root = probeWrapper, .leafs = {leaves}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterWindowedAggregationLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalWindowedAggregation>(argument.conf);
}
}
