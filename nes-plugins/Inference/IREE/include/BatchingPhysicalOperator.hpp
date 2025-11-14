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
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <PhysicalOperator.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{

class BatchingPhysicalOperator final : public WindowBuildPhysicalOperator
{
public:
    explicit BatchingPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        std::shared_ptr<Interface::BufferRef::TupleBufferRef> tupleBufferRef);

    BatchingPhysicalOperator(const BatchingPhysicalOperator& other) = default;

    void execute(ExecutionContext& executionCtx, Record& record) const override;

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer&) const override;

    void setup(ExecutionContext&, CompilationContext&) const override { /*noop*/ };
    void terminate(ExecutionContext&) const override { /*noop*/ };

protected:
    const std::shared_ptr<Interface::BufferRef::TupleBufferRef> tupleBufferRef;
};

}
