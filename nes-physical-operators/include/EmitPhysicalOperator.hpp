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
#include <memory>
#include <optional>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// @brief Basic emit operator that receives records from an upstream operator and
/// writes them to a tuple buffer according to a memory layout.
class EmitPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit EmitPhysicalOperator(OperatorHandlerId operatorHandlerId, std::shared_ptr<TupleBufferRef> bufferRef);

    void setup(ExecutionContext& ctx, CompilationContext& compilationCtx) const override;
    void terminate(ExecutionContext& ctx) const override;

    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

    [[nodiscard]] OperatorHandlerId getOperatorHandlerId() const { return operatorHandlerId; }

    [[nodiscard]] const std::shared_ptr<TupleBufferRef>& getBufferRef() const { return bufferRef; }

private:
    [[nodiscard]] uint64_t getMaxRecordsPerBuffer() const;

    std::optional<PhysicalOperator> child;
    std::shared_ptr<TupleBufferRef> bufferRef;
    OperatorHandlerId operatorHandlerId;
};

}
