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

#include <cstdint>
#include <optional>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Interface/Record.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// Lean PoC physical operator: inserts a scalar statistic value into the shared StatisticStore.
/// Schema passthrough — reads the start/end/value fields of each record and inserts
/// (statisticId, start, end, value) into the store (value held in Statistic::numberOfSeenTuples),
/// then forwards the record to the child operator.
class StatisticStoreWriter final : public PhysicalOperatorConcept
{
public:
    StatisticStoreWriter(
        OperatorHandlerId operatorHandlerId,
        uint64_t statisticId,
        Record::RecordFieldIdentifier startField,
        Record::RecordFieldIdentifier endField,
        Record::RecordFieldIdentifier valueField);

    void execute(ExecutionContext& ctx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    OperatorHandlerId operatorHandlerId;
    uint64_t statisticId;
    Record::RecordFieldIdentifier startField;
    Record::RecordFieldIdentifier endField;
    Record::RecordFieldIdentifier valueField;
    std::optional<PhysicalOperator> child;
};

}
