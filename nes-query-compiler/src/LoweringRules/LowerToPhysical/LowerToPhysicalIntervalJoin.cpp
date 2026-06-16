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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Join/IntervalJoin/IntervalJoinBuildPhysicalOperator.hpp>
#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>
#include <Join/IntervalJoin/IntervalJoinProbePhysicalOperator.hpp>
#include <Join/IntervalJoin/IntervalJoinSlice.hpp>
#include <Join/IntervalJoin/IntervalSliceStore.hpp>
#include <Join/IntervalJoin/IntervalSliceStoreRef.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/IntervalJoinLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceStoreRef.hpp>
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

/// Mirrors the NLJ helper: collects the join-key field names that resolve against the
/// given (physical) input schema.
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
    const auto pageSize = conf.pageSize.getValue();

    /// Outer interval joins null-fill the preserved side's unmatched tuples. Anchor-side unmatched tuples are
    /// null-filled inside each anchor-driven probe task; partner-side unmatched tuples are null-filled by a
    /// symmetric partner-anchored pass the handler runs at termination.
    using JoinType = JoinLogicalOperator::JoinType;
    const auto joinType = intervalJoin->getJoinType();
    const bool emitAnchorNullFill = (joinType == JoinType::OUTER_LEFT_JOIN || joinType == JoinType::OUTER_FULL_JOIN);
    const bool emitPartnerNullFill = (joinType == JoinType::OUTER_RIGHT_JOIN || joinType == JoinType::OUTER_FULL_JOIN);

    /// Side-specific input origins. The interval-join handler needs to know which
    /// origins belong to which side because it maintains per-side watermarks.
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
    auto combinedFieldMapping = FieldMappingTrait{std::move(combinedFieldMappingVec)};

    auto joinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction, combinedFieldMapping);
    auto anchorBufferRef = LowerSchemaProvider::lowerSchema(pageSize, anchorInputSchema, memoryLayoutType);
    auto partnerBufferRef = LowerSchemaProvider::lowerSchema(pageSize, partnerInputSchema, memoryLayoutType);

    /// The interval join carries one bound time characteristic per side; the anchor build reads the
    /// anchor timestamp field and the partner build reads the partner timestamp field.
    const auto& joinTimeCharacteristicsVariant = intervalJoin->getJoinTimeCharacteristics();
    const auto characteristicsAreBound
        = std::holds_alternative<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);
    PRECONDITION(characteristicsAreBound, "Expected the interval-join time characteristics to be bound");
    const auto& [anchorTimeCharacteristic, partnerTimeCharacteristic]
        = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);

    /// Handler owns both stores. The lowering rule does NOT construct stores.
    auto handler = std::make_shared<IntervalJoinOperatorHandler>(
        anchorInputOriginIds,
        partnerInputOriginIds,
        outputOriginId,
        lowerBound,
        upperBound,
        conf.sliceCacheConfiguration,
        emitAnchorNullFill,
        emitPartnerNullFill);

    /// SliceStoreRef per side, talking to the side-appropriate store on the handler.
    auto sliceStoreRefAnchor = createIntervalSliceStoreRef(
        handler->getAnchorStore(),
        [](Slice& slice, const WorkerThreadId workerThreadId) -> std::span<const std::byte>
        {
            const auto& ijSlice = dynamic_cast<IntervalJoinSlice&>(slice);
            auto* ptr = ijSlice.getPagedVectorRef(workerThreadId);
            return {reinterpret_cast<const std::byte*>(ptr), sizeof(PagedVector)};
        },
        [](IntervalJoinOperatorHandler& h) { return h.getCreateNewSlicesFunction({}); },
        conf.sliceCacheConfiguration);
    auto sliceStoreRefPartner = createIntervalSliceStoreRef(
        handler->getPartnerStore(),
        [](Slice& slice, const WorkerThreadId workerThreadId) -> std::span<const std::byte>
        {
            const auto& ijSlice = dynamic_cast<IntervalJoinSlice&>(slice);
            auto* ptr = ijSlice.getPagedVectorRef(workerThreadId);
            return {reinterpret_cast<const std::byte*>(ptr), sizeof(PagedVector)};
        },
        [](IntervalJoinOperatorHandler& h) { return h.getCreateNewSlicesFunction({}); },
        conf.sliceCacheConfiguration);

    const IntervalJoinBuildAnchorPhysicalOperator anchorBuildOperator{
        handlerId, TimeFunction::create(anchorTimeCharacteristic), anchorBufferRef, std::move(sliceStoreRefAnchor)};
    const IntervalJoinBuildPartnerPhysicalOperator partnerBuildOperator{
        handlerId, TimeFunction::create(partnerTimeCharacteristic), partnerBufferRef, std::move(sliceStoreRefPartner)};

    const JoinSchema joinSchema{anchorInputSchema, partnerInputSchema, outputSchema};

    /// Inner/cartesian joins emit matches only; any outer variant needs the null-fill-capable probe.
    PhysicalOperator probeOperator = isOuterJoin(joinType) ? PhysicalOperator{IntervalJoinProbeOuterPhysicalOperator{
                                                                 handlerId,
                                                                 joinFunction,
                                                                 WindowMetaData{intervalJoin->getStartField(), intervalJoin->getEndField()},
                                                                 joinSchema,
                                                                 TimeFunction::create(anchorTimeCharacteristic),
                                                                 TimeFunction::create(partnerTimeCharacteristic),
                                                                 lowerBound,
                                                                 upperBound,
                                                                 anchorBufferRef,
                                                                 partnerBufferRef,
                                                                 getJoinFieldNames(anchorInputSchema, logicalJoinFunction),
                                                                 getJoinFieldNames(partnerInputSchema, logicalJoinFunction),
                                                                 emitAnchorNullFill}}
                                                           : PhysicalOperator{IntervalJoinProbeInnerPhysicalOperator{
                                                                 handlerId,
                                                                 joinFunction,
                                                                 WindowMetaData{intervalJoin->getStartField(), intervalJoin->getEndField()},
                                                                 joinSchema,
                                                                 TimeFunction::create(anchorTimeCharacteristic),
                                                                 TimeFunction::create(partnerTimeCharacteristic),
                                                                 lowerBound,
                                                                 upperBound,
                                                                 anchorBufferRef,
                                                                 partnerBufferRef,
                                                                 getJoinFieldNames(anchorInputSchema, logicalJoinFunction),
                                                                 getJoinFieldNames(partnerInputSchema, logicalJoinFunction)}};

    auto anchorBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(anchorBuildOperator),
        anchorInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto partnerBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(partnerBuildOperator),
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

    return {.root = {probeWrapper}, .leafs = {anchorBuildWrapper, partnerBuildWrapper}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterIntervalJoinLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalIntervalJoin>(argument.conf);
}

}
