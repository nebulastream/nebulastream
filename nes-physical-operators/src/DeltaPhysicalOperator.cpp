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

    openChild(ctx, recordBuffer);
}

void DeltaPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto* state = dynamic_cast<DeltaState*>(ctx.getLocalState(id));

    if (state->isFirstRecord)
    {
        /// First record of this buffer: store current values and drop the record.
        for (const auto& expr : nautilus::static_iterable(deltaExpressions))
        {
            auto currentValue = expr.readFunction.execute(record, ctx.pipelineMemoryProvider.arena);
            state->previousRecord.exchange(expr.targetField, currentValue);
        }
        state->isFirstRecord = false;
    }
    else
    {
        /// Subsequent records: compute delta, update previous values, and forward.
        for (const auto& expr : nautilus::static_iterable(deltaExpressions))
        {
            auto currentValue = expr.readFunction.execute(record, ctx.pipelineMemoryProvider.arena);
            auto previousValue = state->previousRecord.exchange(expr.targetField, currentValue);
            record.write(expr.targetField, currentValue - previousValue);
        }
        executeChild(ctx, record);
    }
}

void DeltaPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
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
