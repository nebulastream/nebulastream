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

#include <Join/IndexNestedLoopJoin/INLJInnerProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/IndexNestedLoopJoin/INLJJoinCondition.hpp>
#include <Join/IndexNestedLoopJoin/INLJOperatorHandler.hpp>
#include <Join/IndexNestedLoopJoin/INLJSlice.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <fmt/format.h>
#include <function.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
INLJSlice* getINLJSlice(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "Operator handler must not be null");
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
}

INLJInnerProbePhysicalOperator::INLJInnerProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<BTreeTupleLayout> leftTupleLayout,
    std::shared_ptr<BTreeTupleLayout> rightTupleLayout,
    INLJJoinCondition condition)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), joinSchema)
    , leftTupleLayout(std::move(leftTupleLayout))
    , rightTupleLayout(std::move(rightTupleLayout))
    , condition(std::move(condition))
    , leftFields(getOrderedFieldNames(joinSchema.leftSchema))
    , rightFields(getOrderedFieldNames(joinSchema.rightSchema))
{
}

void INLJInnerProbePhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    auto comparator = std::make_shared<BTreeComparator>(
        compilationContext,
        rightTupleLayout,
        fmt::format("inljProbeComparator:{}", id.getRawValue()),
        [keyField = condition.rightField](const Record& lhs, const Record& rhs) -> nautilus::val<bool>
        { return (lhs.read(keyField) < rhs.read(keyField)).getRawValueAs<nautilus::val<bool>>(); });
    activeRightComparator = comparator.get();
    comparators.emplace_back(std::move(comparator));
    StreamJoinProbePhysicalOperator::setup(executionCtx, compilationContext);
}

void INLJInnerProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    PRECONDITION(activeRightComparator != nullptr, "INLJ probe comparator must be set up before open");
    StreamJoinProbePhysicalOperator::open(executionCtx, recordBuffer);

    const auto triggerRef = static_cast<nautilus::val<EmittedINLJWindowTrigger*>>(recordBuffer.getMemArea());
    const auto windowInfoRef = getMemberRef(triggerRef, &EmittedINLJWindowTrigger::windowInfo);
    const auto windowStart = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const auto windowEnd = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};
    auto leftSliceEnds = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedINLJWindowTrigger::leftSliceEnds));
    auto rightSliceEnds = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedINLJWindowTrigger::rightSliceEnds));
    const nautilus::val<SliceEnd> leftSliceEnd{leftSliceEnds[0]};
    const nautilus::val<SliceEnd> rightSliceEnd{rightSliceEnds[0]};

    const auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto leftSlice = nautilus::invoke(getINLJSlice, operatorHandler, leftSliceEnd);
    const auto rightSlice = nautilus::invoke(getINLJSlice, operatorHandler, rightSliceEnd);
    const auto leftTreeCount = nautilus::invoke(getNumberOfTrees, leftSlice, nautilus::val<JoinBuildSideType>{JoinBuildSideType::Left});
    const auto rightTreeCount
        = nautilus::invoke(getNumberOfTrees, rightSlice, nautilus::val<JoinBuildSideType>{JoinBuildSideType::Right});

    for (nautilus::val<uint64_t> leftTreeIndex = 0; leftTreeIndex < leftTreeCount; leftTreeIndex = leftTreeIndex + 1)
    {
        const auto leftBuffer = nautilus::invoke(
            getTreeBuffer, leftSlice, leftTreeIndex, nautilus::val<JoinBuildSideType>{JoinBuildSideType::Left});
        const BTreeRef leftTree{BorrowedNautilusBuffer::from(leftBuffer), leftTupleLayout};
        for (nautilus::val<uint64_t> leftIndex = 0; leftIndex < leftTree.size(); leftIndex = leftIndex + 1)
        {
            const auto leftRecord = leftTree.at(leftIndex);
            Record searchRecord;
            searchRecord.write(condition.rightField, leftRecord.read(condition.leftField));
            for (nautilus::val<uint64_t> rightTreeIndex = 0; rightTreeIndex < rightTreeCount; rightTreeIndex = rightTreeIndex + 1)
            {
                const auto rightBuffer = nautilus::invoke(
                    getTreeBuffer, rightSlice, rightTreeIndex, nautilus::val<JoinBuildSideType>{JoinBuildSideType::Right});
                const BTreeRef rightTree{BorrowedNautilusBuffer::from(rightBuffer), rightTupleLayout};
                nautilus::val<uint64_t> begin = 0;
                nautilus::val<uint64_t> end = rightTree.size();
                switch (condition.predicate)
                {
                    case INLJPredicate::LESS:
                        begin = rightTree.upperBound(searchRecord, *activeRightComparator);
                        break;
                    case INLJPredicate::LESS_EQUALS:
                        begin = rightTree.lowerBound(searchRecord, *activeRightComparator);
                        break;
                    case INLJPredicate::GREATER:
                        end = rightTree.lowerBound(searchRecord, *activeRightComparator);
                        break;
                    case INLJPredicate::GREATER_EQUALS:
                        end = rightTree.upperBound(searchRecord, *activeRightComparator);
                        break;
                }
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
                        auto joinedRecord
                            = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
                        executeChild(executionCtx, joinedRecord);
                    }
                }
            }
        }
    }
}

}
