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

void DeltaPhysicalOperator::open(ExecutionContext&, RecordBuffer&) const
{
    /// TODO step-4: open the delta operator.
    ///   * Allocate a DeltaState as local operator state.
    ///   * Initialize previousRecord by writing the min value for each delta expression's target field.
    ///   * Forward to openChild.
    throw NotImplemented("TODO step-4: implement DeltaPhysicalOperator::open");
}

void DeltaPhysicalOperator::execute(ExecutionContext&, Record&) const
{
    /// TODO step-4: execute the delta operator on a single record.
    ///   * On the first record of the buffer: store the current values into previousRecord and drop the record.
    ///   * On subsequent records: compute current - previous for each delta expression, update previousRecord,
    ///     write the delta into record, and forward to executeChild.
    throw NotImplemented("TODO step-4: implement DeltaPhysicalOperator::execute");
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
