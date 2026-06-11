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
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
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
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Watermark/TimeFunction.hpp>
#include <Watermark/TimestampField.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

namespace
{

/// Mirrors the NLJ helper but on a logical join function for either side schema.
std::vector<Record::RecordFieldIdentifier> getJoinFieldNames(const Schema& inputSchema, const LogicalFunction& joinFunction)
{
    return BFSRange(joinFunction)
        | std::views::filter([](const auto& child) { return child.template tryGetAs<FieldAccessLogicalFunction>().has_value(); })
        | std::views::transform([](const auto& child)
                                { return child.template tryGetAs<FieldAccessLogicalFunction>()->get().getFieldName(); })
        | std::views::filter([&](const auto& fieldName) { return inputSchema.contains(fieldName); })
        | std::ranges::to<std::vector<Record::RecordFieldIdentifier>>();
}

/// Variant of TimestampField::getTimestampLeftAndRight that takes a TimeCharacteristic
/// directly (no WindowType wrapper). The interval-join logical operator carries the
/// TimeCharacteristic standalone.
std::pair<std::unique_ptr<TimeFunction>, std::unique_ptr<TimeFunction>>
buildTimeFunctions(const Windowing::TimeCharacteristic& timeCharacteristic, const Schema& leftSchema, const Schema& rightSchema)
{
    if (timeCharacteristic.getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
    {
        return {TimestampField::ingestionTime().toTimeFunction(), TimestampField::ingestionTime().toTimeFunction()};
    }

    const auto fieldName = timeCharacteristic.field.getUnqualifiedName();
    const auto leftField = leftSchema.getFieldByName(fieldName);
    const auto rightField = rightSchema.getFieldByName(fieldName);
    INVARIANT(
        leftField.has_value() and rightField.has_value(), "Interval-join timestamp field {} must exist in both input schemas", fieldName);

    return {
        TimestampField::eventTime(leftField.value().name, timeCharacteristic.getTimeUnit()).toTimeFunction(),
        TimestampField::eventTime(rightField.value().name, timeCharacteristic.getTimeUnit()).toTimeFunction()};
}

}

LoweringRuleResultSubgraph LowerToPhysicalIntervalJoin::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<IntervalJoinLogicalOperator>(), "Expected an IntervalJoinLogicalOperator");
    PRECONDITION(std::ranges::size(logicalOperator.getChildren()) == 2, "Expected two children");

    const auto outputOriginIdsOpt = getTrait<OutputOriginIdsTrait>(logicalOperator.getTraitSet());
    PRECONDITION(outputOriginIdsOpt.has_value(), "Expected the outputOriginIds trait to be set");
    const auto& outputOriginIds = outputOriginIdsOpt.value().get();
    PRECONDITION(std::ranges::size(outputOriginIds) == 1, "Expected exactly one output origin id");
    PRECONDITION(logicalOperator.getInputSchemas().size() == 2, "Expected two input schemas");

    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    auto intervalJoin = logicalOperator.getAs<IntervalJoinLogicalOperator>();
    const auto handlerId = getNextOperatorHandlerId();
    const auto leftInputSchema = intervalJoin->getLeftSchema();
    const auto rightInputSchema = intervalJoin->getRightSchema();
    const auto outputSchema = intervalJoin.getOutputSchema();
    const auto outputOriginId = outputOriginIds[0];
    const auto logicalJoinFunction = intervalJoin->getJoinFunction();
    const auto lowerBound = intervalJoin->getLowerBound();
    const auto upperBound = intervalJoin->getUpperBound();
    const auto pageSize = conf.pageSize.getValue();

    /// Side-specific input origins. The interval-join handler needs to know which
    /// origins belong to which side because it maintains per-side watermarks.
    const auto children = intervalJoin.getChildren();
    const auto leftChildOrigins = getTrait<OutputOriginIdsTrait>(children[0].getTraitSet());
    const auto rightChildOrigins = getTrait<OutputOriginIdsTrait>(children[1].getTraitSet());
    PRECONDITION(leftChildOrigins.has_value() and rightChildOrigins.has_value(), "Expected outputOriginIds trait on both child operators");
    const std::vector<OriginId> leftInputOriginIds(leftChildOrigins.value().get().begin(), leftChildOrigins.value().get().end());
    const std::vector<OriginId> rightInputOriginIds(rightChildOrigins.value().get().begin(), rightChildOrigins.value().get().end());

    auto joinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction);
    auto leftBufferRef = LowerSchemaProvider::lowerSchema(pageSize, leftInputSchema, memoryLayoutType);
    auto rightBufferRef = LowerSchemaProvider::lowerSchema(pageSize, rightInputSchema, memoryLayoutType);

    auto [leftTimeFunctionBuild, rightTimeFunctionBuild]
        = buildTimeFunctions(intervalJoin->getTimeCharacteristic(), leftInputSchema, rightInputSchema);
    auto [leftTimeFunctionProbe, rightTimeFunctionProbe]
        = buildTimeFunctions(intervalJoin->getTimeCharacteristic(), leftInputSchema, rightInputSchema);

    /// Handler owns both stores. The lowering rule does NOT construct stores.
    auto handler = std::make_shared<IntervalJoinOperatorHandler>(
        leftInputOriginIds, rightInputOriginIds, outputOriginId, lowerBound, upperBound, conf.sliceCacheConfiguration);

    /// SliceStoreRef per side, talking to the side-appropriate store on the handler.
    auto sliceStoreRefLeft = createIntervalSliceStoreRef(
        handler->getLeftStore(),
        [](Slice& slice, const WorkerThreadId workerThreadId) -> std::span<const std::byte>
        {
            const auto& ijSlice = dynamic_cast<IntervalJoinSlice&>(slice);
            auto* ptr = ijSlice.getPagedVectorRef(workerThreadId);
            return {reinterpret_cast<const std::byte*>(ptr), sizeof(PagedVector)};
        },
        [](IntervalJoinOperatorHandler& h) { return h.getCreateNewSlicesFunction({}); },
        conf.sliceCacheConfiguration);
    auto sliceStoreRefRight = createIntervalSliceStoreRef(
        handler->getRightStore(),
        [](Slice& slice, const WorkerThreadId workerThreadId) -> std::span<const std::byte>
        {
            const auto& ijSlice = dynamic_cast<IntervalJoinSlice&>(slice);
            auto* ptr = ijSlice.getPagedVectorRef(workerThreadId);
            return {reinterpret_cast<const std::byte*>(ptr), sizeof(PagedVector)};
        },
        [](IntervalJoinOperatorHandler& h) { return h.getCreateNewSlicesFunction({}); },
        conf.sliceCacheConfiguration);

    const IntervalJoinBuildPhysicalOperator leftBuildOperator{
        handlerId, JoinBuildSideType::Left, std::move(leftTimeFunctionBuild), leftBufferRef, std::move(sliceStoreRefLeft)};
    const IntervalJoinBuildPhysicalOperator rightBuildOperator{
        handlerId, JoinBuildSideType::Right, std::move(rightTimeFunctionBuild), rightBufferRef, std::move(sliceStoreRefRight)};

    const auto joinSchema = JoinSchema(leftInputSchema, rightInputSchema, outputSchema);
    auto probeOperator = IntervalJoinProbePhysicalOperator(
        handlerId,
        joinFunction,
        intervalJoin->getWindowMetaData(),
        joinSchema,
        std::move(leftTimeFunctionProbe),
        std::move(rightTimeFunctionProbe),
        lowerBound,
        upperBound,
        leftBufferRef,
        rightBufferRef,
        getJoinFieldNames(leftInputSchema, logicalJoinFunction),
        getJoinFieldNames(rightInputSchema, logicalJoinFunction));

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

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(probeOperator),
        outputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{leftBuildWrapper, rightBuildWrapper});

    return {.root = {probeWrapper}, .leafs = {leftBuildWrapper, rightBuildWrapper}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterIntervalJoinLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalIntervalJoin>(argument.conf);
}

}
