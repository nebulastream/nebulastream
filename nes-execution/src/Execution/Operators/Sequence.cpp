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

#include <Execution/Operators/Sequence.hpp>

#include <cstdint>
#include <utility>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/SequenceOperatorHandler.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>

namespace NES::Runtime::Execution::Operators
{

void Sequence::open(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer) const
{
    auto buffer = nautilus::invoke(
        +[](OperatorHandler* handler, uint8_t* bufferMemory) -> Memory::TupleBuffer*
        { return dynamic_cast<SequenceOperatorHandler*>(handler)->getNextBuffer(bufferMemory).value_or(nullptr); },
        executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
        recordBuffer.getBuffer());

    while (buffer)
    {
        RecordBuffer nextBufferInSequence{buffer};

        scan->open(executionCtx, nextBufferInSequence);
        scan->close(executionCtx, nextBufferInSequence);

        buffer = nautilus::invoke(
            +[](OperatorHandler* handler, Memory::TupleBuffer* tupleBuffer) -> Memory::TupleBuffer*
            { return dynamic_cast<SequenceOperatorHandler*>(handler)->markBufferAsDone(tupleBuffer).value_or(nullptr); },
            executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
            buffer);
    }
}
void Sequence::setup(ExecutionContext& executionCtx) const
{
    scan->setChild(child);
    nautilus::invoke(
        +[](OperatorHandler* handler, PipelineExecutionContext* ctx) { handler->start(*ctx, 0); },
        executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
        executionCtx.pipelineContext);
    scan->setup(executionCtx);
}
void Sequence::terminate(ExecutionContext& executionCtx) const
{
    scan->terminate(executionCtx);
    nautilus::invoke(
        +[](OperatorHandler* handler, PipelineExecutionContext* ctx) { handler->stop(QueryTerminationType::Graceful, *ctx); },
        executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
        executionCtx.pipelineContext);
}
}
