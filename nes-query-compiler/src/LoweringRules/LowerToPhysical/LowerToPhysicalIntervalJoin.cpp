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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalIntervalJoin.hpp>

#include <array>
#include <cstdint>
#include <memory>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Join/IntervalJoin/IntervalJoinBuildPhysicalOperator.hpp>
#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>
#include <Join/IntervalJoin/IntervalJoinProbePhysicalOperator.hpp>
#include <Join/IntervalJoin/IntervalJoinSlice.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Windows/IntervalJoinLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/IntervalSliceStore.hpp>
#include <SliceStore/IntervalSliceStoreRef.hpp>
#include <SliceStore/Slice.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

namespace
{

/// Join-key field names that resolve against the given (physical) input schema.
std::vector<Record::RecordFieldIdentifier>
getJoinFieldNames(const Schema<QualifiedUnboundField, Ordered>& inputSchema, const LogicalFunction& joinFunction)
{
    return BFSRange(joinFunction)
        | std::views::filter([](const auto& child) { return child.template tryGetAs<FieldAccessLogicalFunction>().has_value(); })
        | std::views::transform([](const auto& child) { return child.template tryGetAs<FieldAccessLogicalFunction>().value()->getField(); })
        | std::views::filter([&](const auto& field) { return inputSchema.contains(field.getLastName()); })
        | std::views::transform([](const auto& field) { return Record::RecordFieldIdentifier{field.getLastName()}; })
        | std::ranges::to<std::vector>();
}

}

LoweringRuleResultSubgraph LowerToPhysicalIntervalJoin::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<IntervalJoinLogicalOperator>(), "Expected an IntervalJoinLogicalOperator");

    auto intervalJoin = logicalOperator.getAs<IntervalJoinLogicalOperator>();
    auto children = intervalJoin->getBothChildren();
    const auto traitSet = logicalOperator.getTraitSet();

    const auto outputOriginIdsOpt = getTrait<OutputOriginIdsTrait>(traitSet);
    PRECONDITION(outputOriginIdsOpt.has_value(), "Expected the outputOriginIds trait to be set");
    const auto& outputOriginIds = outputOriginIdsOpt.value().get();
    PRECONDITION(std::ranges::size(outputOriginIds) == 1, "Expected exactly one output origin id");

    const auto memoryLayoutTypeTrait = traitSet.tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    const auto handlerId = getNextOperatorHandlerId();
    const auto anchorInputSchema = createPhysicalOutputSchema(children[0].getTraitSet());
    const auto partnerInputSchema = createPhysicalOutputSchema(children[1].getTraitSet());
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto outputOriginId = outputOriginIds[0];
    const auto logicalJoinFunction = intervalJoin->getJoinFunction();
    const auto lowerBound = intervalJoin->getLowerBound();
    const auto upperBound = intervalJoin->getUpperBound();

    /// Interval joins are inner-only (outer is rejected during binding in LogicalPlanBuilder::addIntervalJoin).
    /// Re-assert here so a bypassed binding can never lower to outer physical operators.
    INVARIANT(
        intervalJoin->getJoinType() == JoinLogicalOperator::JoinType::INNER_JOIN,
        "Interval join must be INNER at lowering; outer is rejected during binding");

    /// Per-side input origins: the handler maintains per-side watermarks.
    const auto anchorChildOrigins = getTrait<OutputOriginIdsTrait>(children[0].getTraitSet());
    const auto partnerChildOrigins = getTrait<OutputOriginIdsTrait>(children[1].getTraitSet());
    PRECONDITION(
        anchorChildOrigins.has_value() and partnerChildOrigins.has_value(), "Expected outputOriginIds trait on both child operators");
    const std::vector<OriginId> anchorInputOriginIds(anchorChildOrigins.value().get().begin(), anchorChildOrigins.value().get().end());
    const std::vector<OriginId> partnerInputOriginIds(partnerChildOrigins.value().get().begin(), partnerChildOrigins.value().get().end());

    auto combinedFieldMappingVec = intervalJoin.getChildren()
        | std::views::transform([](const auto& child)
                                { return child.getTraitSet().template get<FieldMappingTrait>()->getUnderlying() | std::views::all; })
        | std::views::join | std::views::common | std::ranges::to<std::unordered_map>();
    const FieldMappingTrait combinedFieldMapping{std::move(combinedFieldMappingVec)};

    auto joinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction, combinedFieldMapping);
    auto anchorTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(anchorInputSchema);
    auto partnerTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(partnerInputSchema);
    const uint64_t tupleSizeAnchor = anchorTupleLayout->getSchema().getSizeInBytes();
    const uint64_t tupleSizePartner = partnerTupleLayout->getSchema().getSizeInBytes();

    /// One bound time characteristic per side. Each build reads its side's timestamp field.
    const auto& joinTimeCharacteristicsVariant = intervalJoin->getJoinTimeCharacteristics();
    const auto characteristicsAreBound
        = std::holds_alternative<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);
    PRECONDITION(characteristicsAreBound, "Expected the interval-join time characteristics to be bound");
    const auto& [anchorTimeCharacteristic, partnerTimeCharacteristic]
        = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);

    /// Handler owns both stores. The lowering rule does not construct them.
    auto handler = std::make_shared<IntervalJoinOperatorHandler>(
        anchorInputOriginIds, partnerInputOriginIds, outputOriginId, lowerBound, upperBound, conf.sliceCacheConfiguration);

    /// SliceStoreRef per side, talking to the side-appropriate store on the handler.
    auto sliceStoreRefAnchor = handler->getAnchorStore().createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId) -> void*
        {
            const auto& ijSlice = dynamic_cast<IntervalJoinSlice&>(slice);
            const auto* ptr = ijSlice.getPagedVectorRef(workerThreadId);
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): the SliceStoreRef callback returns a void* token by contract.
            return const_cast<TupleBuffer*>(ptr);
        },
        [tupleSizeAnchor](IntervalJoinOperatorHandler& h, AbstractBufferProvider& bufferProvider)
        {
            const CreateNewIntervalJoinSliceArgs intervalSliceArgs{bufferProvider, tupleSizeAnchor};
            return h.getCreateNewSlicesFunction(intervalSliceArgs);
        });
    auto sliceStoreRefPartner = handler->getPartnerStore().createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId) -> void*
        {
            const auto& ijSlice = dynamic_cast<IntervalJoinSlice&>(slice);
            const auto* ptr = ijSlice.getPagedVectorRef(workerThreadId);
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): the SliceStoreRef callback returns a void* token by contract.
            return const_cast<TupleBuffer*>(ptr);
        },
        [tupleSizePartner](IntervalJoinOperatorHandler& h, AbstractBufferProvider& bufferProvider)
        {
            const CreateNewIntervalJoinSliceArgs intervalSliceArgs{bufferProvider, tupleSizePartner};
            return h.getCreateNewSlicesFunction(intervalSliceArgs);
        });

    const IntervalJoinBuildPhysicalOperator anchorBuildOperator{
        handlerId,
        IntervalJoinBuildSide::Anchor,
        TimeFunction::create(anchorTimeCharacteristic),
        anchorTupleLayout,
        std::move(sliceStoreRefAnchor)};
    const IntervalJoinBuildPhysicalOperator partnerBuildOperator{
        handlerId,
        IntervalJoinBuildSide::Partner,
        TimeFunction::create(partnerTimeCharacteristic),
        partnerTupleLayout,
        std::move(sliceStoreRefPartner)};

    const JoinSchema joinSchema{anchorInputSchema, partnerInputSchema, outputSchema};

    PhysicalOperator probeOperator = IntervalJoinProbePhysicalOperator{
        handlerId,
        joinFunction,
        WindowMetaData{intervalJoin->getStartField(), intervalJoin->getEndField()},
        joinSchema,
        TimeFunction::create(anchorTimeCharacteristic),
        TimeFunction::create(partnerTimeCharacteristic),
        lowerBound,
        upperBound,
        anchorTupleLayout,
        partnerTupleLayout,
        getJoinFieldNames(anchorInputSchema, logicalJoinFunction),
        getJoinFieldNames(partnerInputSchema, logicalJoinFunction)};

    auto anchorBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        anchorBuildOperator,
        anchorInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto partnerBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        partnerBuildOperator,
        partnerInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(probeOperator),
        outputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{anchorBuildWrapper, partnerBuildWrapper});

    return {.root = {probeWrapper}, .leaves = {anchorBuildWrapper, partnerBuildWrapper}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterIntervalJoinLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalIntervalJoin>(argument.conf);
}

}
