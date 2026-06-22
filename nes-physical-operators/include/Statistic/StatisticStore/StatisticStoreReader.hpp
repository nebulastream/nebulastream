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

#pragma once

#include <optional>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Interface/Record.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// Lean PoC physical operator: reads a scalar statistic value back from the shared StatisticStore.
/// Schema passthrough — reads (statisticId, start, end) from each record, looks the value up in the store
/// (Statistic::numberOfSeenTuples, or 0 if absent), overwrites the value field, and forwards the record.
class StatisticStoreReader final : public PhysicalOperatorConcept
{
public:
    StatisticStoreReader(
        OperatorHandlerId operatorHandlerId,
        Record::RecordFieldIdentifier statisticIdField,
        Record::RecordFieldIdentifier startField,
        Record::RecordFieldIdentifier endField,
        Record::RecordFieldIdentifier valueField);

    void execute(ExecutionContext& ctx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    OperatorHandlerId operatorHandlerId;
    Record::RecordFieldIdentifier statisticIdField;
    Record::RecordFieldIdentifier startField;
    Record::RecordFieldIdentifier endField;
    Record::RecordFieldIdentifier valueField;
    std::optional<PhysicalOperator> child;
};

}
