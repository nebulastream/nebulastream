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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalSample.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <variant>
#include <vector>
#include <Aggregation/AggregationSlice.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SampleLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Sample/SampleFields.hpp>
#include <Sample/SampleIngestPhysicalOperator.hpp>
#include <Sample/SampleOperatorHandler.hpp>
#include <Sample/SampleProbePhysicalOperator.hpp>
#include <Sample/SampleTriggerPhysicalOperator.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ErrorHandling.hpp>
#include <HashMapOptions.hpp>
#include <HashMapSlice.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <SampleAggregationPhysicalFunctionRegistry.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalSample::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<SampleLogicalOperator>(), "Expected a SampleLogicalOperator");
    const auto sampleOp = logicalOperator.getAs<SampleLogicalOperator>();

    const auto children = sampleOp->getChildren();
    PRECONDITION(children.size() == 2, "SampleLogicalOperator must have exactly 2 children after SampleTickSourceBindingRule");

    const auto traitSet = logicalOperator.getTraitSet();
    const auto memoryLayoutTypeTrait = traitSet.tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    const auto inputSchema = createPhysicalOutputSchema(children[0].getTraitSet());
    /// The order of the children is set by us during Binding, therefore we can be sure that this order holds.
    const auto tickSchema = createPhysicalOutputSchema(children[1].getTraitSet());
    const auto outputSchema = createPhysicalOutputSchema(traitSet);

    const std::vector<QualifiedUnboundField> outputFields(outputSchema.begin(), outputSchema.end());
    const std::vector<QualifiedUnboundField> tickFields(tickSchema.begin(), tickSchema.end());
    PRECONDITION(tickFields.size() == 1, "The SAMPLE tick source's schema is expected to have exactly one field");

    PRECONDITION(
        std::holds_alternative<Windowing::BoundTimeCharacteristic>(sampleOp->getCharacteristic()),
        "Expected time characteristic to be bound");
    const auto boundCharacteristic = std::get<Windowing::BoundTimeCharacteristic>(sampleOp->getCharacteristic());
    if (std::holds_alternative<Windowing::BoundEventTimeCharacteristic>(boundCharacteristic))
    {
        /// Clock alignment between the tick source and event time is not possible at the moment.
        throw SampleEventTimeNotSupported(
            "SAMPLE with an explicit event-time field is not yet supported; omit it to use processing/ingestion time");
    }
    /// from here on we assume ingestion time to be the time field to trigger windows on
    /// Future extensions require branching here
    /// Note: since there is no output field anchored to this timestamp, an empty slot emits an all-NULL row.
    const auto timestampFieldName = Identifier::parse(SAMPLE_PROC_TS_FIELD);

    std::vector<QualifiedUnboundField> stateFields(outputFields);
    stateFields.emplace_back(Identifier::parse(SAMPLE_PROC_TS_FIELD), DataType::Type::UINT64);
    const Schema<QualifiedUnboundField, Ordered> stateSchema{stateFields};

    const auto strategy = sampleOp->getStrategy();
    /// @Yannik, is this where we should catch misspecified grammer?
    const auto maybeFunction = SampleAggregationPhysicalFunctionRegistry::instance().create(
        strategy, SampleAggregationPhysicalFunctionRegistryArguments{stateSchema, timestampFieldName});
    PRECONDITION(maybeFunction.has_value(), "unknown SAMPLE strategy: {}", strategy);
    const auto function = maybeFunction.value();
    const auto valueSize = function->getSizeOfStateInBytes();

    constexpr uint64_t keySize = 0;
    constexpr uint64_t numberOfBuckets = 1;
    const auto pageSize = conf.pageSize.getValue();
    const auto entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
    const auto entriesPerPage = pageSize / entrySize;
    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(stateSchema, {}, {});
    const HashMapOptions hashMapOptions(
        std::make_unique<MurMur3HashFunction>(),
        std::vector<PhysicalFunction>{},
        fieldKeys,
        fieldValues,
        entriesPerPage,
        entrySize,
        keySize,
        valueSize,
        pageSize,
        numberOfBuckets);

    const auto slotDurationMs = sampleOp->getSlotDurationMs();
    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(slotDurationMs, slotDurationMs, conf.sliceCacheConfiguration);

    const auto extractor = [](Slice& slice, const WorkerThreadId workerThreadId) -> void*
    {
        auto& aggregationSlice = dynamic_cast<AggregationSlice&>(slice);
        return aggregationSlice.getHashMapPtrOrCreate(workerThreadId);
    };
    const auto creator = [hashMapOptions](WindowBasedOperatorHandler& handler, AbstractBufferProvider&)
    {
        auto& sampleHandler = dynamic_cast<SampleOperatorHandler&>(handler);
        const CreateNewHashMapSliceArgs hashMapSliceArgs{
            {sampleHandler.cleanupStateNautilusFunction},
            hashMapOptions.keySize,
            hashMapOptions.valueSize,
            hashMapOptions.pageSize,
            hashMapOptions.numberOfBuckets};
        return handler.getCreateNewSlicesFunction(hashMapSliceArgs);
    };
    auto ingestSliceStoreRef = sliceAndWindowStore->createSliceStoreRef(extractor, creator);
    auto triggerSliceStoreRef = sliceAndWindowStore->createSliceStoreRef(extractor, creator);

    auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*outputOriginIds) == 1, "Expected one output origin id");
    const auto outputOriginId = (*outputOriginIds)[0];

    auto tickOriginIds = children[1].getTraitSet().get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*tickOriginIds) == 1, "Expected one output origin id for the tick source");
    const auto tickOriginId = (*tickOriginIds)[0];

    const auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<SampleOperatorHandler>(
        std::vector{tickOriginId}, outputOriginId, std::move(sliceAndWindowStore), valueSize, pageSize);

    auto ingestTimeFunction = std::make_unique<IngestionTimeFunction>();
    auto triggerTimeFunction = std::make_unique<IngestionTimeFunction>();

    auto ingestWrapper = std::make_shared<PhysicalOperatorWrapper>(
        SampleIngestPhysicalOperator{handlerId, std::move(ingestTimeFunction), std::move(ingestSliceStoreRef), function, hashMapOptions},
        inputSchema,
        inputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto triggerWrapper = std::make_shared<PhysicalOperatorWrapper>(
        SampleTriggerPhysicalOperator{handlerId, std::move(triggerTimeFunction), std::move(triggerSliceStoreRef)},
        tickSchema,
        tickSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    /// Required by the base class but never read by SAMPLE; a self-contained placeholder, not a real column.
    const UnqualifiedUnboundField windowMetaPlaceholder{Identifier::parse("__sample_window_meta"), DataType::Type::UINT64};
    const WindowMetaData windowMetaData{windowMetaPlaceholder, windowMetaPlaceholder};

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        SampleProbePhysicalOperator{handlerId, windowMetaData, function, outputFields, timestampFieldName, hashMapOptions},
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{ingestWrapper, triggerWrapper});

    return {.root = probeWrapper, .leaves = {ingestWrapper, triggerWrapper}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterSampleLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSample>(argument.conf);
}

}
