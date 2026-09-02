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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalStatisticBuild.hpp>

#include <cstdint>
#include <memory>
#include <numeric>
#include <ranges>
#include <utility>
#include <variant>
#include <vector>

#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/StatisticBuildLogicalOperator.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
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
getAggregationPhysicalFunctions(const StatisticBuildLogicalOperator& logicalOperator, const QueryExecutionConfiguration& /*configuration*/)
{
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    const auto& aggregationDescriptors = logicalOperator.getWindowAggregation();

    const auto memoryLayoutTypeTrait = logicalOperator.getChild()->getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    const auto physicalInputSchema = createPhysicalOutputSchema(logicalOperator.getChild()->getTraitSet());
    auto tupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(physicalInputSchema);

    for (const auto& descriptor : aggregationDescriptors)
    {
        const auto fieldIsBound = std::holds_alternative<TypedLogicalFunction<FieldAccessLogicalFunction>>(descriptor->getInputFunction());
        PRECONDITION(fieldIsBound, "Expected the aggregation function to be bound");
        const auto fieldAccessFunction = std::get<TypedLogicalFunction<FieldAccessLogicalFunction>>(descriptor->getInputFunction());

        auto physicalInputType = fieldAccessFunction->getDataType();
        auto physicalFinalType = descriptor->getAggregateType();
        auto aggregationInputFunction = QueryCompilation::FunctionProvider::lowerFunction(
            fieldAccessFunction, *logicalOperator.getChild().getTraitSet().get<FieldMappingTrait>());
        const auto resultFieldIdentifier = Identifier::parse("result");
        auto name = descriptor->getName();

        auto aggregationArguments = AggregationPhysicalFunctionRegistryArguments(
            std::move(physicalInputType),
            std::move(physicalFinalType),
            std::move(aggregationInputFunction),
            resultFieldIdentifier,
            tupleLayout,
            descriptor.shallIncludeNullValues());
        if (const auto aggregationFactory = AggregationPhysicalFunctionRegistry::instance().find(std::string{name}))
        {
            aggregationPhysicalFunctions.push_back((*aggregationFactory)(std::move(aggregationArguments)));
        }
        else
        {
            throw UnknownAggregationType("unknown statistic aggregation type: {}", name);
        }
    }
    return aggregationPhysicalFunctions;
}
}

LoweringRuleResultSubgraph LowerToPhysicalStatisticBuild::apply(LogicalOperator logicalOperator)
{
    auto aggregation = logicalOperator.getAs<StatisticBuildLogicalOperator>();
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

    const uint64_t keySize = 0;
    std::vector<PhysicalFunction> keyFunctions;
    const auto entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
    const auto numberOfBuckets = conf.numberOfPartitions.getValue();
    const auto pageSize = conf.pageSize.getValue();

    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(
        physicalInputSchema, std::vector<QualifiedIdentifier>{}, std::vector<QualifiedIdentifier>{});

    const auto windowMetaData = WindowMetaData{aggregation->getWindowStartField(), aggregation->getWindowEndField()};

    const ChainedHashMapConfig hashMapConfig{
        .entrySize = entrySize,
        .numberOfBuckets = numberOfBuckets,
        .pageSize = pageSize,
        .bloomFilterParams = std::nullopt,
        .fieldKeys = fieldKeys,
        .fieldValues = fieldValues,
        .hashFunction = std::make_shared<MurMur3HashFunction>()};

    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType.getSize().getTime(), windowType.getSlide().getTime(), conf.sliceCacheConfiguration);
    auto sliceStoreRef = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId, AbstractBufferProvider& bufferProvider) -> const TupleBuffer*
        {
            auto& aggregationSlice = dynamic_cast<AggregationSlice&>(slice);
            return aggregationSlice.getOrCreateHashMapBufferRefForWorker(bufferProvider, workerThreadId);
        },
        /// NOLINTNEXTLINE(bugprone-exception-escape): dynamic_cast<ref> may throw std::bad_cast on bug; non-recoverable here.
        [hashMapConfig](WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
        {
            auto& aggHandler = dynamic_cast<AggregationOperatorHandler&>(handler);
            const CreateNewHashMapSliceArgs hashMapSliceArgs{hashMapConfig, &bufferProvider};
            return handler.getCreateNewSlicesFunction(hashMapSliceArgs);
        });
    const AggregationBuildPhysicalOperator build{
        handlerId, std::move(timeFunction), std::move(sliceStoreRef), aggregationPhysicalFunctions, hashMapConfig, keyFunctions};
    const AggregationProbePhysicalOperator probe{hashMapConfig, aggregationPhysicalFunctions, handlerId, windowMetaData};

    auto handler = std::make_shared<AggregationOperatorHandler>(
        *inputOriginIds | std::ranges::to<std::vector>(), outputOriginId, std::move(sliceAndWindowStore));
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

    std::vector leaves(logicalOperator.getChildren().size(), buildWrapper);
    return {.root = probeWrapper, .leaves = {leaves}};
}

}
