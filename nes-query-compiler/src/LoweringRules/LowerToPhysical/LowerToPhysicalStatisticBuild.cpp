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

#include <memory>
#include <ranges>
#include <utility>
#include <variant>
#include <vector>

#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/ChainedHashMap/FieldOffsets.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/StatisticBuildLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Statistics/ReservoirSamplePhysicalFunction.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalStatisticBuild::apply(LogicalOperator logicalOperator)
{
    auto statisticBuild = logicalOperator.getAs<StatisticBuildLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto childTraitSet = statisticBuild->getChild().getTraitSet();

    auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    auto inputOriginIds = childTraitSet.get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*outputOriginIds) == 1, "Expected one output origin id");
    auto outputOriginId = (*outputOriginIds)[0];

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    PRECONDITION(
        std::holds_alternative<Windowing::BoundTimeCharacteristic>(statisticBuild->getCharacteristic()),
        "Expected time characteristic to be bound");

    auto handlerId = getNextOperatorHandlerId();
    auto timeFunction = TimeFunction::create(std::get<Windowing::BoundTimeCharacteristic>(statisticBuild->getCharacteristic()));
    auto windowType = statisticBuild->getWindowType();

    const auto physicalInputSchema = createPhysicalOutputSchema(childTraitSet);
    const auto physicalOutputSchema = createPhysicalOutputSchema(traitSet);

    for (const auto& field : physicalInputSchema)
    {
        if (field.getDataType().nullable)
        {
            throw NotImplemented(
                "Reservoir samples do not support nullable input fields yet, but the input contains {}. "
                "Cast or filter the field to a non-nullable type before the RESERVOIR aggregation.",
                field);
        }
    }

    /// The reservoir samples whole input records; the input function of the aggregation function interface is unused.
    const auto firstInputField = *physicalInputSchema.begin();
    PhysicalFunction unusedInputFunction = FieldAccessPhysicalFunction{firstInputField.getFullyQualifiedName()};
    auto tupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(physicalInputSchema);

    const std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions{
        std::make_shared<ReservoirSamplePhysicalFunction>(
            firstInputField.getDataType(),
            DataTypeProvider::provideDataType(DataType::Type::VARSIZED),
            std::move(unusedInputFunction),
            statisticBuild->getStatisticDataField().getFullyQualifiedName(),
            tupleLayout,
            statisticBuild->getNumberOfSeenTuplesField().getFullyQualifiedName(),
            statisticBuild->getSampleSize(),
            statisticBuild->getSeed())};

    const auto valueSize = aggregationPhysicalFunctions.front()->getSizeOfStateInBytes();
    const auto entrySize = sizeof(ChainedHashMapEntry) + valueSize;
    const auto numberOfBuckets = conf.numberOfPartitions.getValue();
    const auto pageSize = conf.pageSize.getValue();

    /// The statistic build is unkeyed: no key fields or key functions. The single synthetic value entry at the
    /// start of the entry's payload is load-bearing: with neither keys nor values, getValueMemArea() would fall
    /// back to offset 0 and the aggregation state would overwrite the ChainedHashMapEntry header.
    const std::vector<PhysicalFunction> keyFunctions;
    const std::vector<FieldOffsets> fieldKeys;
    const std::vector<FieldOffsets> fieldValues{FieldOffsets{
        .fieldIdentifier = statisticBuild->getStatisticDataField().getFullyQualifiedName(),
        .type = DataTypeProvider::provideDataType(DataType::Type::UINT64),
        .fieldOffset = sizeof(ChainedHashMapEntry)}};

    const auto windowMetaData = WindowMetaData{statisticBuild->getStatisticStartField(), statisticBuild->getStatisticEndField()};

    /// Statistic build does not benefit from the ChainedHashMap's optional filter, so bloomFilter stays empty.
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

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leaves(logicalOperator.getChildren().size(), buildWrapper);
    return {.root = probeWrapper, .leaves = {leaves}};
}

}
