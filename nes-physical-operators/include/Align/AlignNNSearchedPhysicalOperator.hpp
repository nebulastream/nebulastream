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
#include <CompilationContext.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Watermark/TimeFunction.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

class AlignNNSearchedPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    AlignNNSearchedPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        std::unique_ptr<TimeFunction> leftTimeFunction,
        std::unique_ptr<TimeFunction> rightTimeFunction,
        std::shared_ptr<PagedVectorTupleLayout> pendingLeftLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightLogLayout,
        std::shared_ptr<TupleBufferRef> outputBufferRef);
    AlignNNSearchedPhysicalOperator(const AlignNNSearchedPhysicalOperator& other);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    void execute(ExecutionContext& executionCtx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator childOperator) override;

private:
    OperatorHandlerId operatorHandlerId;
    std::unique_ptr<TimeFunction> leftTimeFunction;
    std::unique_ptr<TimeFunction> rightTimeFunction;
    std::shared_ptr<PagedVectorTupleLayout> pendingLeftLayout;
    std::shared_ptr<PagedVectorTupleLayout> rightLogLayout;
    std::shared_ptr<TupleBufferRef> outputBufferRef;
    std::optional<PhysicalOperator> child;
};

}
