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
#include <Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// The physical UNNEST, a 1→N operator: it decodes the JSON array in `input` once per record into a block in the
/// pipeline arena, then calls its child once per element, on a copy of the record with the element in `output`. The
/// copy keeps the elements independent of each other, even if an operator above writes a field in place. Rows are
/// pushed one at a time, like a join probe does, so the emit at the end of the pipeline spills into new buffers when one
/// is full; nothing else is needed from the pipeline.
class UnnestPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    UnnestPhysicalOperator(Record::RecordFieldIdentifier input, Record::RecordFieldIdentifier output);
    void execute(ExecutionContext& ctx, Record& record) const override;
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    Record::RecordFieldIdentifier input;
    Record::RecordFieldIdentifier output;
    std::optional<PhysicalOperator> child;
};

}
