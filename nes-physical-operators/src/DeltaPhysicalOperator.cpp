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
    /// TODO: implement open().
    /// 1. Create and register a DeltaState as local operator state.
    /// 2. Initialize previousRecord with min values for each delta field.
    /// 3. Call openChild().
    openChild(ctx, recordBuffer);
}

void DeltaPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// TODO: implement execute().
    /// First record of buffer: store current values in previousRecord, drop the record.
    /// Subsequent records: for each delta expression read current value, exchange with
    /// previous value, compute delta (current - previous), write to target field, and
    /// forward via executeChild.
    executeChild(ctx, record);
}

void DeltaPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    /// TODO: implement close().
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
