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
#include <vector>
#include <Execution/Operators/DelayShuffle/ShuffleTuples.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <function.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Runtime::Execution::Operators
{
ShuffleTuplesOperatorHandler::ShuffleTuplesOperatorHandler(const float unorderedness)
    : unorderedness(unorderedness), gen(std::mt19937(std::random_device()()))
{
}

void ShuffleTuplesOperatorHandler::setup(const uint64_t numberOfWorkerThreads)
{
    this->numberOfWorkerThreads = numberOfWorkerThreads;
    emitIndicesForWorker.resize(numberOfWorkerThreads, {});
}

void ShuffleTuplesOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
    ((void)unorderedness);
}

void ShuffleTuplesOperatorHandler::stop(Runtime::QueryTerminationType, PipelineExecutionContext&)
{
    emitIndicesForWorker.clear();
}

void ShuffleTuplesOperatorHandler::createEmitIndicesForInputBuffer(const WorkerThreadId workerThreadId, const uint64_t numberOfTuples)
{
    auto& emitIndices = emitIndicesForWorker[workerThreadId % numberOfWorkerThreads];
    emitIndices.clear();
    emitIndices.resize(numberOfTuples, 0);
    /// Generate indices from 0 - numberOfTuples-1
    std::iota(emitIndices.begin(), emitIndices.end(), 0);

    /// Shuffle indices randomly but leave some elements in place to have an unorderedness factor
    const auto numberOfInPlaceItems = static_cast<uint64_t>(numberOfTuples * (1 - unorderedness));
    std::shuffle(emitIndices.begin() + numberOfInPlaceItems, emitIndices.end(), gen);
}

std::vector<uint64_t> ShuffleTuplesOperatorHandler::getEmitIndicesForWorker(const WorkerThreadId workerThreadId)
{
    return emitIndicesForWorker[workerThreadId % numberOfWorkerThreads];
}

ShuffleTuples::ShuffleTuples(
    std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider, const uint64_t operatorHandlerIndex)
    : memoryProvider(std::move(memoryProvider))
    , operatorHandlerIndex(operatorHandlerIndex)
    , projections(this->memoryProvider->getMemoryLayout()->getSchema()->getFieldNames())
{
}

void setupOpHandlerProxy(OperatorHandler* ptrOpHandler, const PipelineExecutionContext* pipelineExecutionContext)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");

    auto* opHandler = dynamic_cast<ShuffleTuplesOperatorHandler*>(ptrOpHandler);
    opHandler->setup(pipelineExecutionContext->getNumberOfWorkerThreads());
}

void ShuffleTuples::setup(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    nautilus::invoke(setupOpHandlerProxy, operatorHandlerMemRef, executionCtx.pipelineContext);
}

void ShuffleTuples::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto workerThreadId = executionCtx.getWorkerThreadId();

    /// Create new result buffer
    auto resultBuffer = RecordBuffer(executionCtx.allocateBuffer());

    /// initialize global state variables to keep track of the watermark ts and the origin id
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();

    /// Create emit indices
    const auto numberOfRecords = recordBuffer.getNumRecords();
    nautilus::invoke(
        +[](OperatorHandler* opHandler, const WorkerThreadId threadId, const uint64_t numberOfTuples)
        {
            PRECONDITION(opHandler != nullptr, "opHandler context should not be null!");
            dynamic_cast<ShuffleTuplesOperatorHandler*>(opHandler)->createEmitIndicesForInputBuffer(threadId, numberOfTuples);
        },
        operatorHandlerMemRef,
        workerThreadId,
        numberOfRecords);

    /// Iterate over all tuples and write record to the output buffer at the position specified by emitIndices
    for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
    {
        auto record = memoryProvider->readRecord(projections, recordBuffer, i);

        nautilus::val<uint64_t> outputIndex = nautilus::invoke(
            +[](OperatorHandler* opHandler, const WorkerThreadId threadId, const uint64_t index)
            {
                PRECONDITION(opHandler != nullptr, "opHandler context should not be null!");
                return dynamic_cast<ShuffleTuplesOperatorHandler*>(opHandler)->getEmitIndicesForWorker(threadId)[index];
            },
            operatorHandlerMemRef,
            workerThreadId,
            i);
        memoryProvider->writeRecord(outputIndex, resultBuffer, record);
    }

    /// Set the metadata and emit buffer
    resultBuffer.setNumRecords(numberOfRecords);
    resultBuffer.setWatermarkTs(executionCtx.watermarkTs);
    resultBuffer.setOriginId(executionCtx.originId);
    resultBuffer.setSequenceNumber(executionCtx.sequenceNumber);
    resultBuffer.setChunkNumber(executionCtx.chunkNumber);
    resultBuffer.setLastChunk(executionCtx.lastChunk);
    resultBuffer.setCreationTs(executionCtx.currentTs);
    executionCtx.emitBuffer(resultBuffer);
}
}