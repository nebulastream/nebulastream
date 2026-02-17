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

#include <DeltaPhysicalOperator.hpp>

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <nautilus/val.hpp>
#include <DeltaOperatorHandler.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>
#include "Nautilus/Util.hpp"

namespace NES
{

/// Local operator state for the delta operator.
/// Stores the first-record flag and a Record holding the previous tuple's values.
class DeltaState : public OperatorState
{
public:
    nautilus::val<bool> isFirstRecord = true;
    Record previousRecord;
};

namespace
{

bool loadPredecessorProxy(OperatorHandler* handler, SequenceNumber seqNum, int8_t* destPtr)
{
    PRECONDITION(handler != nullptr, "handler should not be null");
    return dynamic_cast<DeltaOperatorHandler*>(handler)->loadPredecessor(seqNum, reinterpret_cast<std::byte*>(destPtr));
}

bool storeLastAndCheckPendingProxy(OperatorHandler* handler, SequenceNumber seqNum, int8_t* srcPtr, int8_t* destPtr)
{
    PRECONDITION(handler != nullptr, "handler should not be null");
    return dynamic_cast<DeltaOperatorHandler*>(handler)->storeLastAndCheckPending(
        seqNum, reinterpret_cast<const std::byte*>(srcPtr), reinterpret_cast<std::byte*>(destPtr));
}

bool storePendingAndCheckPredecessorProxy(OperatorHandler* handler, SequenceNumber seqNum, int8_t* fullRecordSrc, int8_t* predecessorDest)
{
    PRECONDITION(handler != nullptr, "handler should not be null");
    return dynamic_cast<DeltaOperatorHandler*>(handler)->storePendingAndCheckPredecessor(
        seqNum, reinterpret_cast<const std::byte*>(fullRecordSrc), reinterpret_cast<std::byte*>(predecessorDest));
}

}

DeltaPhysicalOperator::DeltaPhysicalOperator(
    std::vector<PhysicalDeltaExpression> deltaExpressions,
    OperatorHandlerId operatorHandlerId,
    std::vector<DeltaFieldLayoutEntry> deltaFieldLayout,
    size_t deltaFieldsEntrySize,
    std::vector<DeltaFieldLayoutEntry> fullRecordLayout,
    size_t fullRecordEntrySize)
    : deltaExpressions(std::move(deltaExpressions))
    , operatorHandlerId(operatorHandlerId)
    , deltaFieldLayout(std::move(deltaFieldLayout))
    , deltaFieldsEntrySize(deltaFieldsEntrySize)
    , fullRecordLayout(std::move(fullRecordLayout))
    , fullRecordEntrySize(fullRecordEntrySize)
{
}

void DeltaPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    ctx.setLocalOperatorState(id, std::make_unique<DeltaState>());
    auto* state = dynamic_cast<DeltaState*>(ctx.getLocalState(id));

    /// Initialize previous record with min values for each delta field.
    for (const auto& expr : nautilus::static_iterable(deltaExpressions))
    {
        state->previousRecord.write(expr.targetField, createNautilusMinValue(expr.targetDataType.type));
    }

    /// Try to load predecessor's last delta field values from the handler.
    auto handlerPtr = ctx.getGlobalOperatorHandler(operatorHandlerId);
    auto scratch = ctx.allocateMemory(nautilus::val<size_t>(deltaFieldsEntrySize));
    auto found = nautilus::invoke(loadPredecessorProxy, handlerPtr, ctx.sequenceNumber, scratch);

    if (found)
    {
        /// Deserialize delta field values from scratch into previousRecord.
        auto ptr = scratch;
        for (const auto& entry : nautilus::static_iterable(deltaFieldLayout))
        {
            auto val = VarVal::readVarValFromMemory(ptr, entry.type);
            state->previousRecord.write(entry.fieldName, val);
            ptr = ptr + nautilus::val<size_t>(DataType(entry.type).getSizeInBytes());
        }
        state->isFirstRecord = false;
    }

    openChild(ctx, recordBuffer);
}

void DeltaPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto* state = dynamic_cast<DeltaState*>(ctx.getLocalState(id));

    if (state->isFirstRecord)
    {
        /// First record: store current values in previousRecord.
        for (const auto& expr : nautilus::static_iterable(deltaExpressions))
        {
            auto currentValue = expr.readFunction.execute(record, ctx.pipelineMemoryProvider.arena);
            state->previousRecord.exchange(expr.targetField, currentValue);
        }

        /// Serialize ALL record fields and try to store as pending.
        /// Also checks if the predecessor already closed (three-way handshake).
        auto handlerPtr = ctx.getGlobalOperatorHandler(operatorHandlerId);
        auto fullScratch = ctx.allocateMemory(nautilus::val<size_t>(fullRecordEntrySize));
        {
            auto ptr = fullScratch;
            for (const auto& entry : nautilus::static_iterable(fullRecordLayout))
            {
                record.read(entry.fieldName).writeToMemory(ptr);
                ptr = ptr + nautilus::val<size_t>(DataType(entry.type).getSizeInBytes());
            }
        }
        auto predScratch = ctx.allocateMemory(nautilus::val<size_t>(deltaFieldsEntrySize));
        auto predecessorFound
            = nautilus::invoke(storePendingAndCheckPredecessorProxy, handlerPtr, ctx.sequenceNumber, fullScratch, predScratch);

        if (predecessorFound)
        {
            /// Predecessor closed before we processed our first record.
            /// Compute delta against predecessor's last values and emit.
            auto ptr = predScratch;
            for (const auto& entry : nautilus::static_iterable(deltaFieldLayout))
            {
                auto predVal = VarVal::readVarValFromMemory(ptr, entry.type);
                auto currentValue = state->previousRecord.read(entry.fieldName);
                record.write(entry.fieldName, currentValue - predVal);
                ptr = ptr + nautilus::val<size_t>(DataType(entry.type).getSizeInBytes());
            }
            executeChild(ctx, record);
        }

        state->isFirstRecord = false;
    }
    else
    {
        /// Subsequent records: compute delta, update previous values, and forward the record.
        for (const auto& expr : nautilus::static_iterable(deltaExpressions))
        {
            /// Read the current value before creating/overwriting the target field.
            auto currentValue = expr.readFunction.execute(record, ctx.pipelineMemoryProvider.arena);
            auto previousValue = state->previousRecord.exchange(expr.targetField, currentValue);
            /// Create the target field (needed for alias fields not present in the input record).
            record.write(expr.targetField, currentValue - previousValue);
        }
        executeChild(ctx, record);
    }
}

void DeltaPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    auto* state = dynamic_cast<DeltaState*>(ctx.getLocalState(id));

    if (!state->isFirstRecord)
    {
        auto handlerPtr = ctx.getGlobalOperatorHandler(operatorHandlerId);

        /// Serialize previousRecord's delta field values to scratch.
        auto lastScratch = ctx.allocateMemory(nautilus::val<size_t>(deltaFieldsEntrySize));
        {
            auto ptr = lastScratch;
            for (const auto& entry : nautilus::static_iterable(deltaFieldLayout))
            {
                state->previousRecord.read(entry.fieldName).writeToMemory(ptr);
                ptr = ptr + nautilus::val<size_t>(DataType(entry.type).getSizeInBytes());
            }
        }

        /// Allocate scratch for a potential pending first record from the successor.
        auto pendingScratch = ctx.allocateMemory(nautilus::val<size_t>(fullRecordEntrySize));
        auto found = nautilus::invoke(storeLastAndCheckPendingProxy, handlerPtr, ctx.sequenceNumber, lastScratch, pendingScratch);

        if (found)
        {
            /// Deserialize all fields from the pending first record.
            Record firstRecord;
            {
                auto ptr = pendingScratch;
                for (const auto& entry : nautilus::static_iterable(fullRecordLayout))
                {
                    firstRecord.write(entry.fieldName, VarVal::readVarValFromMemory(ptr, entry.type));
                    ptr = ptr + nautilus::val<size_t>(DataType(entry.type).getSizeInBytes());
                }
            }

            /// Compute delta for each expression: firstValue - lastValue.
            for (const auto& expr : nautilus::static_iterable(deltaExpressions))
            {
                auto firstValue = expr.readFunction.execute(firstRecord, ctx.pipelineMemoryProvider.arena);
                auto previousValue = state->previousRecord.read(expr.targetField);
                firstRecord.write(expr.targetField, firstValue - previousValue);
            }

            executeChild(ctx, firstRecord);
        }
    }

    closeChild(ctx, recordBuffer);
}

std::optional<PhysicalOperator> DeltaPhysicalOperator::getChild() const
{
    return child;
}

void DeltaPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
