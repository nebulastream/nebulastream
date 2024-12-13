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

#include <Util/Execution.hpp>
#include <Execution/Operators/Watermark/LatenessProcessor.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <nautilus/function.hpp>

namespace NES::Runtime::Execution::Operators
{
void LatenessProcessorOperatorHandler::updateSeenWatermarks(const BufferMetaData& bufferMetaData) const
{
    watermarkProcessor->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
}

Timestamp LatenessProcessorOperatorHandler::getCurrentWatermarkTimestamp() const
{
    return watermarkProcessor->getCurrentWatermark();
}

void LatenessProcessor::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    ExecutableOperator::open(executionCtx, recordBuffer);
}

void LatenessProcessor::execute(ExecutionContext& ctx, Record& record) const
{
}

void LatenessProcessor::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    nautilus::invoke(
        +[](
        OperatorHandler* ptrOpHandler,
        const Timestamp watermarkTs,
        const SequenceNumber sequenceNumber,
        const ChunkNumber chunkNumber,
        const bool lastChunk,
        const OriginId originId)
        {
            PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");

            const auto* opHandler = dynamic_cast<LatenessProcessorOperatorHandler*>(ptrOpHandler);
            const BufferMetaData bufferMetaData(watermarkTs, SequenceData(sequenceNumber, chunkNumber, lastChunk), originId);
            opHandler->updateSeenWatermarks(bufferMetaData);
        },
        operatorHandlerMemRef,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);
    ExecutableOperator::close(executionCtx, recordBuffer);
}
}