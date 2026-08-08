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

#include <Join/IndexNestedLoopJoin/INLJProbePhysicalOperatorBase.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Join/IndexNestedLoopJoin/INLJOperatorHandler.hpp>
#include <Join/IndexNestedLoopJoin/INLJSlice.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <fmt/format.h>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{
namespace
{
INLJSlice* getINLJSlice(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "INLJ operator handler must not be null");
    const auto* opHandler = dynamic_cast<INLJOperatorHandler*>(ptrOpHandler);
    auto slice = opHandler->getSliceAndWindowStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(slice.has_value(), "Could not find INLJ slice for slice end {}", sliceEnd);
    return dynamic_cast<INLJSlice*>(slice.value().get());
}

uint64_t getNumberOfTrees(const INLJSlice* slice, const JoinBuildSideType side)
{
    PRECONDITION(slice != nullptr, "INLJ slice must not be null");
    return slice->getNumberOfBTrees(side);
}

const TupleBuffer* getTreeBuffer(const INLJSlice* slice, const uint64_t workerIndex, const JoinBuildSideType side)
{
    PRECONDITION(slice != nullptr, "INLJ slice must not be null");
    return slice->getBTreeBuffer(workerIndex, side);
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

std::pair<nautilus::val<uint64_t>, nautilus::val<uint64_t>> findRange(
    const BTreeRef& tree, const Record& searchRecord, const BTreeComparator& comparator, const INLJPredicate predicate)
{
    nautilus::val<uint64_t> begin = 0;
    nautilus::val<uint64_t> end = tree.size();
    switch (predicate)
    {
        case INLJPredicate::LESS:
            begin = tree.upperBound(searchRecord, comparator);
            break;
        case INLJPredicate::LESS_EQUALS:
            begin = tree.lowerBound(searchRecord, comparator);
            break;
        case INLJPredicate::GREATER:
            end = tree.lowerBound(searchRecord, comparator);
            break;
        case INLJPredicate::GREATER_EQUALS:
            end = tree.upperBound(searchRecord, comparator);
            break;
    }
    return {begin, end};
}
}

INLJProbePhysicalOperatorBase::INLJProbePhysicalOperatorBase(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<BTreeTupleLayout> leftTupleLayout,
    std::shared_ptr<BTreeTupleLayout> rightTupleLayout,
    INLJJoinCondition condition,
    const bool needsLeftComparator)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), joinSchema)
    , leftTupleLayout(std::move(leftTupleLayout))
    , rightTupleLayout(std::move(rightTupleLayout))
    , condition(std::move(condition))
    , leftFields(getOrderedFieldNames(joinSchema.leftSchema))
    , rightFields(getOrderedFieldNames(joinSchema.rightSchema))
    , needsLeftComparator(needsLeftComparator)
{
}

void INLJProbePhysicalOperatorBase::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    auto makeComparator = [&](const std::shared_ptr<BTreeTupleLayout>& layout,
                              const Record::RecordFieldIdentifier& keyField,
                              const std::string_view side)
    {
        return std::make_shared<BTreeComparator>(
            compilationContext,
            layout,
            fmt::format("inljProbeComparator:{}:{}", id.getRawValue(), side),
            [keyField](const Record& lhs, const Record& rhs) -> nautilus::val<bool>
            { return (lhs.read(keyField) < rhs.read(keyField)).getRawValueAs<nautilus::val<bool>>(); });
    };

    if (needsLeftComparator)
    {
        auto leftComparator = makeComparator(leftTupleLayout, condition.leftField, "left");
        activeLeftComparator = leftComparator.get();
        comparators.emplace_back(std::move(leftComparator));
    }
    auto rightComparator = makeComparator(rightTupleLayout, condition.rightField, "right");
    activeRightComparator = rightComparator.get();
    comparators.emplace_back(std::move(rightComparator));
    StreamJoinProbePhysicalOperator::setup(executionCtx, compilationContext);
}

void INLJProbePhysicalOperatorBase::performMatchPairsProbe(
    const nautilus::val<SliceEnd>& leftSliceEnd,
    const nautilus::val<SliceEnd>& rightSliceEnd,
    const nautilus::val<OperatorHandler*>& operatorHandler,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    PRECONDITION(activeRightComparator != nullptr, "INLJ right comparator must be set up before probing");
    const auto leftSlice = nautilus::invoke(getINLJSlice, operatorHandler, leftSliceEnd);
    const auto rightSlice = nautilus::invoke(getINLJSlice, operatorHandler, rightSliceEnd);
    const auto leftTreeCount = nautilus::invoke(getNumberOfTrees, leftSlice, nautilus::val<JoinBuildSideType>{JoinBuildSideType::Left});
    const auto rightTreeCount
        = nautilus::invoke(getNumberOfTrees, rightSlice, nautilus::val<JoinBuildSideType>{JoinBuildSideType::Right});

    for (nautilus::val<uint64_t> leftTreeIndex = 0; leftTreeIndex < leftTreeCount; leftTreeIndex = leftTreeIndex + 1)
    {
        const auto leftBuffer
            = nautilus::invoke(getTreeBuffer, leftSlice, leftTreeIndex, nautilus::val<JoinBuildSideType>{JoinBuildSideType::Left});
        const BTreeRef leftTree{BorrowedNautilusBuffer::from(leftBuffer), leftTupleLayout};
        for (auto leftIterator = leftTree.begin(); leftIterator != leftTree.end(); ++leftIterator)
        {
            const auto leftRecord = *leftIterator;
            Record searchRecord;
            searchRecord.write(condition.rightField, leftRecord.read(condition.leftField));
            for (nautilus::val<uint64_t> rightTreeIndex = 0; rightTreeIndex < rightTreeCount; rightTreeIndex = rightTreeIndex + 1)
            {
                const auto rightBuffer = nautilus::invoke(
                    getTreeBuffer, rightSlice, rightTreeIndex, nautilus::val<JoinBuildSideType>{JoinBuildSideType::Right});
                const BTreeRef rightTree{BorrowedNautilusBuffer::from(rightBuffer), rightTupleLayout};
                const auto [begin, end] = findRange(rightTree, searchRecord, *activeRightComparator, condition.predicate);
                for (auto rightIterator = rightTree.iteratorAt(begin); rightIterator != rightTree.end(end); ++rightIterator)
                {
                    const auto rightRecord = *rightIterator;
                    const auto keyRecord = createJoinedRecord(
                        leftRecord,
                        rightRecord,
                        windowStart,
                        windowEnd,
                        std::vector{condition.leftField},
                        std::vector{condition.rightField});
                    if (joinFunction.execute(keyRecord, executionCtx.pipelineMemoryProvider.arena))
                    {
                        auto joinedRecord = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
                        executeChild(executionCtx, joinedRecord);
                    }
                }
            }
        }
    }
}

void INLJProbePhysicalOperatorBase::performNullFillProbe(
    const nautilus::val<SliceEnd>& preservedSliceEnd,
    const JoinBuildSideType preservedSide,
    nautilus::val<SliceEnd::Underlying*> innerSliceEnds,
    const nautilus::val<uint64_t> innerNumberOfSliceEnds,
    ExecutionContext& executionCtx,
    const nautilus::val<OperatorHandler*>& operatorHandler,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    PRECONDITION(activeLeftComparator != nullptr, "INLJ left comparator must be set up before probing");
    PRECONDITION(activeRightComparator != nullptr, "INLJ right comparator must be set up before probing");
    const auto preservingLeft = preservedSide == JoinBuildSideType::Left;
    const auto innerSide = preservingLeft ? JoinBuildSideType::Right : JoinBuildSideType::Left;
    const auto& preservedLayout = preservingLeft ? leftTupleLayout : rightTupleLayout;
    const auto& innerLayout = preservingLeft ? rightTupleLayout : leftTupleLayout;
    const auto& preservedField = preservingLeft ? condition.leftField : condition.rightField;
    const auto& innerField = preservingLeft ? condition.rightField : condition.leftField;
    const auto& preservedFields = preservingLeft ? leftFields : rightFields;
    const auto& nullSideSchema = preservingLeft ? joinSchema.rightSchema : joinSchema.leftSchema;
    const auto predicate = preservingLeft ? condition.predicate : invertPredicate(condition.predicate);
    const auto* comparator = preservingLeft ? activeRightComparator : activeLeftComparator;

    const auto preservedSlice = nautilus::invoke(getINLJSlice, operatorHandler, preservedSliceEnd);
    const auto preservedTreeCount
        = nautilus::invoke(getNumberOfTrees, preservedSlice, nautilus::val<JoinBuildSideType>{preservedSide});
    for (nautilus::val<uint64_t> preservedTreeIndex = 0; preservedTreeIndex < preservedTreeCount;
         preservedTreeIndex = preservedTreeIndex + 1)
    {
        const auto preservedBuffer = nautilus::invoke(
            getTreeBuffer, preservedSlice, preservedTreeIndex, nautilus::val<JoinBuildSideType>{preservedSide});
        const BTreeRef preservedTree{BorrowedNautilusBuffer::from(preservedBuffer), preservedLayout};
        for (auto preservedIterator = preservedTree.begin(); preservedIterator != preservedTree.end(); ++preservedIterator)
        {
            const auto preservedRecord = *preservedIterator;
            Record searchRecord;
            searchRecord.write(innerField, preservedRecord.read(preservedField));
            nautilus::val<bool> matched = false;
            for (nautilus::val<uint64_t> innerSliceIndex = 0; innerSliceIndex < innerNumberOfSliceEnds; innerSliceIndex = innerSliceIndex + 1)
            {
                const nautilus::val<SliceEnd> innerSliceEnd{innerSliceEnds[innerSliceIndex]};
                const auto innerSlice = nautilus::invoke(getINLJSlice, operatorHandler, innerSliceEnd);
                const auto innerTreeCount
                    = nautilus::invoke(getNumberOfTrees, innerSlice, nautilus::val<JoinBuildSideType>{innerSide});
                for (nautilus::val<uint64_t> innerTreeIndex = 0; innerTreeIndex < innerTreeCount; innerTreeIndex = innerTreeIndex + 1)
                {
                    const auto innerBuffer
                        = nautilus::invoke(getTreeBuffer, innerSlice, innerTreeIndex, nautilus::val<JoinBuildSideType>{innerSide});
                    const BTreeRef innerTree{BorrowedNautilusBuffer::from(innerBuffer), innerLayout};
                    const auto [begin, end] = findRange(innerTree, searchRecord, *comparator, predicate);
                    if (begin < end)
                    {
                        matched = true;
                        break;
                    }
                }
                if (matched)
                {
                    break;
                }
            }
            if (not matched)
            {
                auto nullRecord
                    = createNullFilledJoinedRecord(preservedRecord, windowStart, windowEnd, preservedFields, nullSideSchema);
                executeChild(executionCtx, nullRecord);
            }
        }
    }
}

}
