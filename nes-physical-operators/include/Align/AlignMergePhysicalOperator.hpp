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

#include <memory>
#include <optional>
#include <vector>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

class AlignMergePhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit AlignMergePhysicalOperator(std::shared_ptr<TupleBufferRef> bufferRef);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator childOperator) override;

private:
    std::shared_ptr<TupleBufferRef> bufferRef;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::optional<PhysicalOperator> child;
};

}
