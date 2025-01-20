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

#include <cstdint>
#include <memory>
#include <utility>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Execution/Operators/Streaming/WindowOperatorBuild.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>

namespace NES::Runtime::Execution::Operators
{



WindowOperatorBuild::WindowOperatorBuild(const uint64_t operatorHandlerIndex, std::unique_ptr<TimeFunction> timeFunction)
    : operatorHandlerIndex(operatorHandlerIndex), timeFunction(std::move(timeFunction))
{
}

void WindowOperatorBuild::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Giving the chance for any child operator to initialize its state
    ExecutableOperator::open(executionCtx, recordBuffer);

    /// Initializing the time function
    timeFunction->open(executionCtx, recordBuffer);
}

void WindowOperatorBuild::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Giving the chance for any child operator to close its state
    ExecutableOperator::close(executionCtx, recordBuffer);

    /// Passing the record buffer to the trigger operator
    auto newBuffer = executionCtx.allocateBuffer();
    RecordBuffer recordBufferCopy(newBuffer);
    recordBufferCopy.setChunkNumber(recordBuffer.getChunkNumber());
    recordBufferCopy.setSequenceNumber(recordBuffer.getSequenceNumber());
    recordBufferCopy.setOriginId(recordBuffer.getOriginId());
    recordBufferCopy.setWatermarkTs(recordBuffer.getWatermarkTs());
    recordBufferCopy.setLastChunk(recordBuffer.isLastChunk());
    recordBufferCopy.setCreationTs(recordBuffer.getCreatingTs());
    executionCtx.emitBuffer(recordBufferCopy);
}
}
