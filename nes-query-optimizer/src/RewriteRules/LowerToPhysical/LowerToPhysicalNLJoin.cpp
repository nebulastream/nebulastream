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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalNLJoin.hpp>

#include <memory>
#include <ranges>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Join/NestedLoopJoin/NLJBuildPhysicalOperator.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/TimeFunction.hpp>
#include <Watermark/TimestampField.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

static auto getJoinFieldNames(const Schema& inputSchema, const LogicalFunction& joinFunction)
{
    return BFSRange(joinFunction)
        | std::views::filter([](const auto& child) { return child.template tryGet<FieldAccessLogicalFunction>().has_value(); })
        | std::views::transform([](const auto& child) { return child.template tryGet<FieldAccessLogicalFunction>()->getFieldName(); })
        | std::views::filter([&](const auto& fieldName) { return inputSchema.contains(fieldName); })
        | std::ranges::to<std::vector<std::string>>();
};

RewriteRuleResultSubgraph LowerToPhysicalNLJoin::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<JoinLogicalOperator>(), "Expected a JoinLogicalOperator");
    PRECONDITION(std::ranges::size(logicalOperator.getChildren()) == 2, "Expected two children");
    auto outputOriginIdsOpt = getTrait<OutputOriginIdsTrait>(logicalOperator.getTraitSet());
    PRECONDITION(outputOriginIdsOpt.has_value(), "Expected the outputOriginIds trait to be set");
    auto& outputOriginIds = outputOriginIdsOpt.value();
    PRECONDITION(std::ranges::size(outputOriginIdsOpt.value()) == 1, "Expected one output origin id");
    PRECONDITION(logicalOperator.getInputSchemas().size() == 2, "Expected two input schemas");

    auto join = logicalOperator.getAs<JoinLogicalOperator>();
    auto handlerId = getNextOperatorHandlerId();

    auto leftInputSchema = join->getLeftSchema();
    auto rightInputSchema = join->getRightSchema();
    auto outputSchema = join.getOutputSchema();
    auto outputOriginId = outputOriginIds[0];
    auto logicalJoinFunction = join->getJoinFunction();
    auto windowType = NES::as<Windowing::TimeBasedWindowType>(join->getWindowType());
    const auto pageSize = conf.pageSize.getValue();


    const auto inputOriginIds
        = join.getChildren()
        | std::views::transform(
              [](const auto& child)
              {
                  auto childOutputOriginIds = getTrait<OutputOriginIdsTrait>(child.getTraitSet());
                  PRECONDITION(childOutputOriginIds.has_value(), "Expected the outputOriginIds trait of the child to be set");
                  return childOutputOriginIds.value();
              })
        | std::views::join | std::ranges::to<std::vector<OriginId>>();

    auto joinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction);
    auto leftBufferRef = TupleBufferRef::create(pageSize, leftInputSchema);
    leftBufferRef->getMemoryLayout()->setKeyFieldNames(getJoinFieldNames(leftInputSchema, logicalJoinFunction));
    auto rightBufferRef = TupleBufferRef::create(pageSize, rightInputSchema);
    rightBufferRef->getMemoryLayout()->setKeyFieldNames(getJoinFieldNames(rightInputSchema, logicalJoinFunction));

    auto [timeStampFieldLeft, timeStampFieldRight] = TimestampField::getTimestampLeftAndRight(*join, windowType);

    auto leftBuildOperator
        = NLJBuildPhysicalOperator(handlerId, JoinBuildSideType::Left, timeStampFieldLeft.toTimeFunction(), leftBufferRef);

    auto rightBuildOperator
        = NLJBuildPhysicalOperator(handlerId, JoinBuildSideType::Right, timeStampFieldRight.toTimeFunction(), rightBufferRef);

    auto joinSchema = JoinSchema(leftInputSchema, rightInputSchema, outputSchema);
    auto probeOperator
        = NLJProbePhysicalOperator(handlerId, joinFunction, join->getWindowMetaData(), joinSchema, leftBufferRef, rightBufferRef);

    auto sliceAndWindowStore
        = std::make_unique<DefaultTimeBasedSliceStore>(windowType->getSize().getTime(), windowType->getSlide().getTime());
    auto handler = std::make_shared<NLJOperatorHandler>(inputOriginIds, outputOriginId, std::move(sliceAndWindowStore));

    auto leftBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(leftBuildOperator), leftInputSchema, outputSchema, handlerId, handler, PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto rightBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(rightBuildOperator), rightInputSchema, outputSchema, handlerId, handler, PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(probeOperator),
        outputSchema,
        outputSchema,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{leftBuildWrapper, rightBuildWrapper});

    return {.root = {probeWrapper}, .leafs = {leftBuildWrapper, rightBuildWrapper}};
};

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterNLJoinRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalNLJoin>(argument.conf);
}

}
