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
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <nautilus/val.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// @brief Basic emit operator that receives records from an upstream operator and
/// writes them to a tuple buffer according to a memory layout.
class EmitPhysicalOperator final
{
public:
    explicit EmitPhysicalOperator(OperatorHandlerId operatorHandlerId, std::shared_ptr<TupleBufferRef> bufferRef);

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const;
    [[nodiscard]] EmitPhysicalOperator withChild(PhysicalOperator newChild) const;

    void setup(ExecutionContext&, CompilationContext&) const { /*noop*/ }

    void terminate(ExecutionContext&) const { /*noop*/ }

    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const;
    void execute(ExecutionContext& ctx, Record& record) const;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const;
    void emitRecordBuffer(
        ExecutionContext& ctx,
        RecordBuffer& recordBuffer,
        const nautilus::val<uint64_t>& numRecords,
        const nautilus::val<bool>& potentialLastChunk) const;

    OperatorId id = INVALID_OPERATOR_ID;

protected:
    /// Helper classes to propagate to the child
    void setupChild(ExecutionContext& executionCtx, CompilationContext& compilationContext) const;
    void openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void executeChild(ExecutionContext& executionCtx, Record& record) const;
    void terminateChild(ExecutionContext& executionCtx) const;

private:
    [[nodiscard]] uint64_t getMaxRecordsPerBuffer() const;

    std::optional<PhysicalOperator> child;
    std::shared_ptr<TupleBufferRef> bufferRef;
    OperatorHandlerId operatorHandlerId;
};

static_assert(PhysicalOperatorConcept<EmitPhysicalOperator>);

}
