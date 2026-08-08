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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalINLJoin.hpp>

#include <array>
#include <cstdint>
#include <memory>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Interface/BTree/BTree.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Join/IndexNestedLoopJoin/INLJBuildPhysicalOperator.hpp>
#include <Join/IndexNestedLoopJoin/INLJInnerProbePhysicalOperator.hpp>
#include <Join/IndexNestedLoopJoin/INLJJoinCondition.hpp>
#include <Join/IndexNestedLoopJoin/INLJOperatorHandler.hpp>
#include <Join/IndexNestedLoopJoin/INLJSlice.hpp>
#include <Join/JoinTriggerStrategy.hpp>
#include <Iterators/BFSIterator.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
namespace
{
INLJPredicate getPredicate(const LogicalFunction& function)
{
    if (function.tryGetAs<LessLogicalFunction>().has_value())
    {
        return INLJPredicate::LESS;
    }
    if (function.tryGetAs<LessEqualsLogicalFunction>().has_value())
    {
        return INLJPredicate::LESS_EQUALS;
    }
    if (function.tryGetAs<GreaterLogicalFunction>().has_value())
    {
        return INLJPredicate::GREATER;
    }
    if (function.tryGetAs<GreaterEqualsLogicalFunction>().has_value())
    {
        return INLJPredicate::GREATER_EQUALS;
    }
    throw UnknownJoinStrategy("Unsupported INLJ predicate {}", function);
}

INLJPredicate invertPredicate(const INLJPredicate predicate)
{
    switch (predicate)
    {
        case INLJPredicate::LESS:
            return INLJPredicate::GREATER;
        case INLJPredicate::LESS_EQUALS:
            return INLJPredicate::GREATER_EQUALS;
        case INLJPredicate::GREATER:
            return INLJPredicate::LESS;
        case INLJPredicate::GREATER_EQUALS:
            return INLJPredicate::LESS_EQUALS;
    }
    std::unreachable();
}

INLJJoinCondition getCondition(
    const LogicalFunction& function, const LogicalOperator& leftChild, const LogicalOperator& rightChild)
{
    const auto children = function.getChildren();
    if (children.size() != 2)
    {
        throw UnknownJoinStrategy("INLJ comparison must have two children");
    }
    const auto lhs = children[0].tryGetAs<FieldAccessLogicalFunction>();
    const auto rhs = children[1].tryGetAs<FieldAccessLogicalFunction>();
    if (not lhs.has_value() or not rhs.has_value())
    {
        throw UnknownJoinStrategy("INLJ comparison children must be field accesses");
    }
    auto predicate = getPredicate(function);
    auto lhsField = lhs.value()->getField();
    auto rhsField = rhs.value()->getField();
    const auto isProducedBy = [](const Field& field, const LogicalOperator& input)
    {
        return std::ranges::any_of(
            BFSRange<LogicalOperator>(input), [&](const auto& producer) { return producer == field.getProducedBy(); });
    };
    if (isProducedBy(lhsField, rightChild))
    {
        if (not isProducedBy(rhsField, leftChild))
        {
            throw UnknownJoinStrategy("INLJ predicate must compare the two join inputs");
        }
        std::swap(lhsField, rhsField);
        predicate = invertPredicate(predicate);
    }
    else
    {
        if (not isProducedBy(lhsField, leftChild) or not isProducedBy(rhsField, rightChild))
        {
            throw UnknownJoinStrategy("INLJ predicate must compare the two join inputs");
        }
    }
    return {
        .leftField = QualifiedIdentifier{lhsField.getLastName()},
        .rightField = QualifiedIdentifier{rhsField.getLastName()},
        .predicate = predicate};
}
}

LoweringRuleResultSubgraph LowerToPhysicalINLJoin::apply(LogicalOperator logicalOperator)
{
    const auto join = logicalOperator.getAs<JoinLogicalOperator>();
    const auto children = join->getBothChildren();
    const auto traitSet = logicalOperator.getTraitSet();
    PRECONDITION(join->getJoinType() == JoinLogicalOperator::JoinType::INNER_JOIN, "The INLJ prototype supports inner joins only");

    const auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*outputOriginIds) == 1, "Expected one output origin id");
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto handlerId = getNextOperatorHandlerId();
    const auto leftInputSchema = createPhysicalOutputSchema(children[0]->getTraitSet());
    const auto rightInputSchema = createPhysicalOutputSchema(children[1]->getTraitSet());
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto logicalJoinFunction = join->getJoinFunction();
    const auto condition = getCondition(logicalJoinFunction, children[0], children[1]);

    const auto inputOriginIds
        = join.getChildren()
        | std::views::transform(
              [](const auto& child)
              {
                  const auto origins = getTrait<OutputOriginIdsTrait>(child.getTraitSet());
                  PRECONDITION(origins.has_value(), "Expected output origin ids on INLJ input");
                  return *origins.value();
              })
        | std::views::join | std::ranges::to<std::vector<OriginId>>();
    auto combinedFieldMappingVec = join->getChildren()
        | std::views::transform([](const auto& child)
                                { return child.getTraitSet().template get<FieldMappingTrait>()->getUnderlying() | std::views::all; })
        | std::views::join | std::views::common | std::ranges::to<std::unordered_map>();
    const auto combinedFieldMapping = FieldMappingTrait{std::move(combinedFieldMappingVec)};
    auto joinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction, combinedFieldMapping);

    auto leftTupleLayout = std::make_shared<DefaultBTreeTupleLayout>(leftInputSchema);
    auto rightTupleLayout = std::make_shared<DefaultBTreeTupleLayout>(rightInputSchema);
    const auto tupleSizeLeft = leftTupleLayout->getSizeInBytes();
    const auto tupleSizeRight = rightTupleLayout->getSizeInBytes();
    const auto& timeCharacteristics = join->getJoinTimeCharacteristics();
    const auto timeCharacteristicsAreBound
        = std::holds_alternative<std::array<Windowing::BoundTimeCharacteristic, 2>>(timeCharacteristics);
    PRECONDITION(timeCharacteristicsAreBound, "Expected bound INLJ time characteristics");
    const auto& [timestampFieldLeft, timestampFieldRight]
        = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(timeCharacteristics);

    const auto windowType = join->getWindowType();
    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType.getSize().getTime(), windowType.getSlide().getTime(), conf.sliceCacheConfiguration);
    const auto createSliceStoreRef = [&](const JoinBuildSideType side)
    {
        return sliceAndWindowStore->createSliceStoreRef(
            [side](Slice& slice, const WorkerThreadId workerThreadId, AbstractBufferProvider&)
            {
                const auto& inljSlice = dynamic_cast<const INLJSlice&>(slice);
                return inljSlice.getBTreeBuffer(workerThreadId.getRawValue(), side);
            },
            [tupleSizeLeft, tupleSizeRight](const WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
            {
                const CreateNewINLJSliceArgs args{bufferProvider, tupleSizeLeft, tupleSizeRight};
                return handler.getCreateNewSlicesFunction(args);
            });
    };
    auto leftSliceStoreRef = createSliceStoreRef(JoinBuildSideType::Left);
    auto rightSliceStoreRef = createSliceStoreRef(JoinBuildSideType::Right);
    auto handler = std::make_shared<INLJOperatorHandler>(
        inputOriginIds, (*outputOriginIds)[0], std::move(sliceAndWindowStore), InnerJoinTriggerStrategy{});

    const INLJBuildPhysicalOperator leftBuild{
        handlerId,
        JoinBuildSideType::Left,
        TimeFunction::create(timestampFieldLeft),
        leftTupleLayout,
        condition.leftField,
        std::move(leftSliceStoreRef)};
    const INLJBuildPhysicalOperator rightBuild{
        handlerId,
        JoinBuildSideType::Right,
        TimeFunction::create(timestampFieldRight),
        rightTupleLayout,
        condition.rightField,
        std::move(rightSliceStoreRef)};
    const auto joinSchema = JoinSchema(leftInputSchema, rightInputSchema, outputSchema);

    auto leftBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(leftBuild),
        leftInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);
    auto rightBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(rightBuild),
        rightInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    static_assert(JoinProbeOperator<INLJInnerProbePhysicalOperator>);
    const INLJInnerProbePhysicalOperator probe{
        handlerId,
        std::move(joinFunction),
        WindowMetaData{join->getStartField(), join->getEndField()},
        joinSchema,
        leftTupleLayout,
        rightTupleLayout,
        condition};
    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(probe),
        outputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{leftBuildWrapper, rightBuildWrapper});
    return {.root = {probeWrapper}, .leaves = {leftBuildWrapper, rightBuildWrapper}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterIndexNLJoinLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalINLJoin>(argument.conf);
}

}
