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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalNLJoin.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Join/JoinTriggerStrategy.hpp>
#include <Join/NestedLoopJoin/NLJBuildPhysicalOperator.hpp>
#include <Join/NestedLoopJoin/NLJInnerProbePhysicalOperator.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJOuterProbePhysicalOperator.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

namespace
{
std::vector<QualifiedIdentifier>
getJoinFieldNames(const Schema<QualifiedUnboundField, Ordered>& inputSchema, const LogicalFunction& joinFunction)
{
    return BFSRange(joinFunction)
        | std::views::filter([](const auto& child) { return child.template tryGetAs<FieldAccessLogicalFunction>().has_value(); })
        | std::views::transform([](const auto& child) { return child.template tryGetAs<FieldAccessLogicalFunction>().value()->getField(); })
        | std::views::filter([&](const auto& field) { return inputSchema.contains(field.getLastName()); })
        | std::views::transform([](const auto& field) { return QualifiedIdentifier{field.getLastName()}; })
        | std::ranges::to<std::vector>();
};
}

LoweringRuleResultSubgraph LowerToPhysicalNLJoin::apply(LogicalOperator logicalOperator)
{
    auto join = logicalOperator.getAs<JoinLogicalOperator>();
    auto children = join->getBothChildren();
    const auto traitSet = logicalOperator.getTraitSet();

    auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*outputOriginIds) == 1, "Expected one output origin id");

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    auto handlerId = getNextOperatorHandlerId();

    auto leftInputSchema = createPhysicalOutputSchema(children[0]->getTraitSet());
    auto rightInputSchema = createPhysicalOutputSchema(children[1]->getTraitSet());
    auto outputSchema = createPhysicalOutputSchema(traitSet);
    auto outputOriginId = (*outputOriginIds)[0];
    auto logicalJoinFunction = join->getJoinFunction();
    auto windowType = join->getWindowType();

    const auto inputOriginIds
        = join.getChildren()
        | std::views::transform(
              [](const auto& child)
              {
                  auto childOutputOriginIds = getTrait<OutputOriginIdsTrait>(child.getTraitSet());
                  PRECONDITION(childOutputOriginIds.has_value(), "Expected the outputOriginIds trait of the child to be set");
                  return *childOutputOriginIds.value();
              })
        | std::views::join | std::ranges::to<std::vector<OriginId>>();

    auto combinedFieldMappingVec = join->getChildren()
        | std::views::transform([](const auto& child)
                                { return child.getTraitSet().template get<FieldMappingTrait>()->getUnderlying() | std::views::all; })
        | std::views::join | std::views::common | std::ranges::to<std::unordered_map>();
    auto combinedFieldMapping = FieldMappingTrait{std::move(combinedFieldMappingVec)};

    auto joinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction, combinedFieldMapping);
    auto leftTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(leftInputSchema);
    auto rightTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(rightInputSchema);
    const uint64_t tupleSizeLeft = leftTupleLayout->getSchema().getSizeInBytes();
    const uint64_t tupleSizeRight = rightTupleLayout->getSchema().getSizeInBytes();

    const auto& joinTimeCharacteristicsVariant = join->getJoinTimeCharacteristics();
    auto characteristicsAreBound
        = std::holds_alternative<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);
    PRECONDITION(characteristicsAreBound, "Expected the join time characteristics to be bound");
    const auto& [timeStampFieldLeft, timeStampFieldRight]
        = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);

    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType.getSize().getTime(), windowType.getSlide().getTime(), conf.sliceCacheConfiguration);
    auto sliceStoreRefLeft = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId) -> void*
        {
            const auto& newNLJSlice = dynamic_cast<NLJSlice&>(slice);
            const auto* ptr = newNLJSlice.getPagedVectorTupleBufferRef(workerThreadId, JoinBuildSideType::Left);
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): the SliceStoreRef callback returns a void* token by contract.
            return const_cast<TupleBuffer*>(ptr);
        },
        [tupleSizeLeft, tupleSizeRight](const WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
        {
            const CreateNewNLJSliceArgs nljSliceArgs{bufferProvider, tupleSizeLeft, tupleSizeRight};
            return handler.getCreateNewSlicesFunction(nljSliceArgs);
        });
    auto sliceStoreRefRight = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId) -> void*
        {
            const auto& newNLJSlice = dynamic_cast<NLJSlice&>(slice);
            const auto* ptr = newNLJSlice.getPagedVectorTupleBufferRef(workerThreadId, JoinBuildSideType::Right);
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): the SliceStoreRef callback returns a void* token by contract.
            return const_cast<TupleBuffer*>(ptr);
        },
        [tupleSizeLeft, tupleSizeRight](const WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
        {
            const CreateNewNLJSliceArgs nljSliceArgs{bufferProvider, tupleSizeLeft, tupleSizeRight};
            return handler.getCreateNewSlicesFunction(nljSliceArgs);
        });

    const auto currentJoinType = join->getJoinType();
    const auto leftKeyFieldNames = getJoinFieldNames(leftInputSchema, logicalJoinFunction);
    const auto rightKeyFieldNames = getJoinFieldNames(rightInputSchema, logicalJoinFunction);

    /// Create trigger strategy based on join type
    using JT = JoinLogicalOperator::JoinType;
    auto createTriggerStrategy = [&]() -> JoinTriggerStrategy
    {
        switch (currentJoinType)
        {
            case JT::OUTER_LEFT_JOIN:
                return OuterJoinTriggerStrategy<true, false>{};
            case JT::OUTER_RIGHT_JOIN:
                return OuterJoinTriggerStrategy<false, true>{};
            case JT::OUTER_FULL_JOIN:
                return OuterJoinTriggerStrategy<true, true>{};
            case JT::CARTESIAN_PRODUCT:
            case JT::INNER_JOIN:
                return InnerJoinTriggerStrategy{};
        }
        std::unreachable();
    };

    auto handler
        = std::make_shared<NLJOperatorHandler>(inputOriginIds, outputOriginId, std::move(sliceAndWindowStore), createTriggerStrategy());

    const NLJBuildPhysicalOperator leftBuildOperator{
        handlerId, JoinBuildSideType::Left, TimeFunction::create(timeStampFieldLeft), leftTupleLayout, std::move(sliceStoreRefLeft)};
    const NLJBuildPhysicalOperator rightBuildOperator{
        handlerId, JoinBuildSideType::Right, TimeFunction::create(timeStampFieldRight), rightTupleLayout, std::move(sliceStoreRefRight)};

    auto joinSchema = JoinSchema(leftInputSchema, rightInputSchema, outputSchema);

    auto leftBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(leftBuildOperator),
        leftInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto rightBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(rightBuildOperator),
        rightInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    /// Select probe operator based on join type
    static_assert(JoinProbeOperator<NLJInnerProbePhysicalOperator>);
    static_assert(JoinProbeOperator<NLJOuterProbePhysicalOperator>);

    auto createProbeWrapper = [&](const auto& probeOperator)
    {
        return std::make_shared<PhysicalOperatorWrapper>(
            std::move(probeOperator),
            outputSchema,
            outputSchema,
            memoryLayoutType,
            memoryLayoutType,
            handlerId,
            handler,
            PhysicalOperatorWrapper::PipelineLocation::SCAN,
            std::vector{leftBuildWrapper, rightBuildWrapper});
    };

    std::shared_ptr<PhysicalOperatorWrapper> probeWrapper;
    if (isOuterJoin(currentJoinType))
    {
        PRECONDITION(
            NLJOuterProbePhysicalOperator::supportsJoinType(currentJoinType), "NLJOuterProbePhysicalOperator does not support join type");
        probeWrapper = createProbeWrapper(NLJOuterProbePhysicalOperator(
            handlerId,
            joinFunction,
            WindowMetaData{join->getStartField(), join->getEndField()},
            joinSchema,
            leftTupleLayout,
            rightTupleLayout,
            leftKeyFieldNames,
            rightKeyFieldNames));
    }
    else
    {
        PRECONDITION(
            NLJInnerProbePhysicalOperator::supportsJoinType(currentJoinType), "NLJInnerProbePhysicalOperator does not support join type");
        probeWrapper = createProbeWrapper(NLJInnerProbePhysicalOperator(
            handlerId,
            joinFunction,
            WindowMetaData{join->getStartField(), join->getEndField()},
            joinSchema,
            leftTupleLayout,
            rightTupleLayout,
            leftKeyFieldNames,
            rightKeyFieldNames));
    }

    return {.root = {probeWrapper}, .leafs = {leftBuildWrapper, rightBuildWrapper}};
};

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterNLJoinLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalNLJoin>(argument.conf);
}

}
