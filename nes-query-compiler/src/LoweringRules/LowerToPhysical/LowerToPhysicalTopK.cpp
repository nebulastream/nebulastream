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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalTopK.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <ranges>
#include <utility>
#include <variant>
#include <vector>
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/TopKAggregationPhysicalFunction.hpp>
#include <Aggregation/UnboundedAggregationOperatorHandler.hpp>
#include <Aggregation/UnboundedTopKProbePhysicalOperator.hpp>
#include <Aggregation/WindowedTopKProbePhysicalOperator.hpp>
#include <fmt/format.h>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/Record.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <LoweringRules/LowerToPhysical/TraceSourcesOfInputOrigins.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/TopKLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalTopK::apply(LogicalOperator logicalOperator)
{
    const auto topKOperator = logicalOperator.getAs<TopKLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto childTraitSet = topKOperator->getChild().getTraitSet();
    const auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*outputOriginIds) == 1, "Expected one output origin id");
    const auto outputOriginId = (*outputOriginIds)[0];
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto childFieldMapping = childTraitSet.get<FieldMappingTrait>();

    const auto characteristic = topKOperator->getCharacteristic();
    PRECONDITION(std::holds_alternative<Windowing::BoundTimeCharacteristic>(characteristic), "Expected time characteristic to be bound");
    const auto windowType = topKOperator->getWindowType();
    const bool unboundedWindow = windowType.isUnbounded();
    auto timeFunction = TimeFunction::create(std::get<Windowing::BoundTimeCharacteristic>(characteristic));
    if (unboundedWindow)
    {
        /// Like an unbounded aggregation, the slices only batch the output, so late records join the earliest open slice.
        timeFunction = std::make_unique<WatermarkClampedTimeFunction>(std::move(timeFunction));
    }
    const auto sliceSize = unboundedWindow ? conf.unboundedSliceSizeMs : windowType.getSize().getTime();
    const auto sliceSlide = unboundedWindow ? conf.unboundedSliceSizeMs : windowType.getSlide().getTime();

    const auto physicalInputSchema = createPhysicalOutputSchema(childTraitSet);
    const auto physicalOutputSchema = createPhysicalOutputSchema(traitSet);

    const auto fieldOf = [](const LogicalFunction& function)
    {
        const auto fieldAccess = function.tryGetAs<FieldAccessLogicalFunction>();
        INVARIANT(fieldAccess.has_value(), "ORDER BY ... LIMIT ... BY expects plain fields after schema inference");
        return fieldAccess.value()->getField();
    };

    std::vector<TopKAggregationPhysicalFunction::SortKey> sortKeys;
    for (const auto& sortKey : topKOperator->getSortKeys())
    {
        const auto mappedName = childFieldMapping->getMapping(fieldOf(sortKey.field).unbound());
        INVARIANT(mappedName.has_value(), "The field mapping of the child does not contain the sort key {}", sortKey.field);
        sortKeys.push_back(TopKAggregationPhysicalFunction::SortKey{.field = mappedName.value(), .descending = sortKey.descending});
    }
    const auto handlerId = getNextOperatorHandlerId();
    /// The build and the probe compile the comparator of the tree in their own pipeline, so each gets its own instance.
    const auto createTopK = [&](const std::string_view role)
    {
        return std::make_shared<TopKAggregationPhysicalFunction>(
            topKOperator->getLimit(), sortKeys, physicalInputSchema, conf.pageSize, fmt::format("topKComparator:{}:{}", handlerId, role));
    };
    const auto buildTopK = createTopK("build");
    const auto probeTopK = createTopK("probe");

    uint64_t keySize = 0;
    std::vector<PhysicalFunction> keyFunctions;
    std::vector<Record::RecordFieldIdentifier> keyNames;
    for (const auto& partitionKey : topKOperator->getPartitionKeys())
    {
        keyFunctions.emplace_back(QueryCompilation::FunctionProvider::lowerFunction(partitionKey, *childFieldMapping));
        keySize += partitionKey.getDataType().getSizeInBytesWithNull();
        keyNames.emplace_back(fieldOf(partitionKey).getLastName());
    }
    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(physicalInputSchema, keyNames, {});
    const ChainedHashMapConfig hashMapConfig{
        .entrySize = sizeof(ChainedHashMapEntry) + keySize + buildTopK->getSizeOfStateInBytes(),
        .numberOfBuckets = conf.numberOfPartitions,
        .pageSize = conf.pageSize,
        .bloomFilterParams = std::nullopt,
        .fieldKeys = fieldKeys,
        .fieldValues = fieldValues,
        .hashFunction = std::make_shared<MurMur3HashFunction>()};

    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(sliceSize, sliceSlide, conf.sliceCacheConfiguration);
    auto sliceStoreRef = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId, AbstractBufferProvider& bufferProvider) -> const TupleBuffer*
        {
            auto& aggregationSlice = dynamic_cast<AggregationSlice&>(slice);
            return aggregationSlice.getOrCreateHashMapBufferRefForWorker(bufferProvider, workerThreadId);
        },
        /// NOLINTNEXTLINE(bugprone-exception-escape): dynamic_cast<ref> may throw std::bad_cast on bug; non-recoverable here.
        [hashMapConfig](WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
        {
            const CreateNewHashMapSliceArgs hashMapSliceArgs{hashMapConfig, bufferProvider};
            return handler.getCreateNewSlicesFunction(hashMapSliceArgs);
        });

    const AggregationBuildPhysicalOperator build{
        handlerId,
        std::move(timeFunction),
        std::move(sliceStoreRef),
        std::vector<std::shared_ptr<AggregationPhysicalFunction>>{buildTopK},
        hashMapConfig,
        keyFunctions,
        std::nullopt,
        /*writeArrivalOrder=*/true};
    const WindowMetaData windowMetaData{topKOperator->getWindowStartField(), topKOperator->getWindowEndField()};
    auto sourcesOfInputOrigins = traceThrottleableSourcesOfInputOrigins(logicalOperator, windowsWithoutThrottling);
    const auto [probe, handler] = [&]() -> std::pair<PhysicalOperator, std::shared_ptr<AggregationOperatorHandler>>
    {
        if (unboundedWindow)
        {
            return {
                UnboundedTopKProbePhysicalOperator{hashMapConfig, probeTopK, handlerId, windowMetaData},
                std::make_shared<UnboundedAggregationOperatorHandler>(
                    std::move(sourcesOfInputOrigins), outputOriginId, std::move(sliceAndWindowStore))};
        }
        return {
            WindowedTopKProbePhysicalOperator{hashMapConfig, probeTopK, handlerId, windowMetaData},
            std::make_shared<AggregationOperatorHandler>(std::move(sourcesOfInputOrigins), outputOriginId, std::move(sliceAndWindowStore))};
    }();

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
