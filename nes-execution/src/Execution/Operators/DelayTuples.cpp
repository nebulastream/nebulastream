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
#include <Execution/Operators/DelayTuples.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Runtime::Execution::Operators
{

DelayTuplesOperatorHandler::DelayTuplesOperatorHandler(float unorderedness, uint64_t minDelay, uint64_t maxDelay)
    : unorderedness(unorderedness), unorderednessDistrib(0, 1), delayDistrib(minDelay, maxDelay)
{
}

void DelayTuplesOperatorHandler::setup(uint64_t numberOfWorkerThreads)
{
    this->numberOfWorkerThreads = numberOfWorkerThreads;
    emitIndicesForWorker.resize(numberOfWorkerThreads, {});
}

void DelayTuplesOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
    ((void)unorderedness);
}

void DelayTuplesOperatorHandler::stop(Runtime::QueryTerminationType, PipelineExecutionContext&)
{
    emitIndicesForWorker.clear();
}

void DelayTuplesOperatorHandler::createEmitIndicesForInputBuffer(WorkerThreadId workerThreadId, uint64_t numberOfTuples)
{
    auto& emitIndices = emitIndicesForWorker[workerThreadId % numberOfWorkerThreads];
    emitIndices.clear();
    emitIndices.resize(numberOfTuples, 0);
    // Generate indices from 0 - numberOfTuples-1
    std::iota(emitIndices.begin(), emitIndices.end(), 0);
    // Shuffle indices randomly
    if (unorderedness != 0)
    {
        auto gen = std::mt19937(std::random_device()());
        std::shuffle(emitIndices.begin(), emitIndices.end(), gen);
    }
}

std::vector<uint64_t> DelayTuplesOperatorHandler::getEmitIndicesForWorker(WorkerThreadId workerThreadId)
{
    return emitIndicesForWorker[workerThreadId % numberOfWorkerThreads];
}

DelayTuples::DelayTuples(
    std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider, const uint64_t operatorHandlerIndex)
    : memoryProvider(std::move(memoryProvider))
    , operatorHandlerIndex(operatorHandlerIndex)
    , projections(this->memoryProvider->getMemoryLayoutPtr()->getSchema()->getFieldNames())
{
}

void setupOpHandlerProxy(OperatorHandler* ptrOpHandler, const PipelineExecutionContext* pipelineExecutionContext)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");

    auto* opHandler = dynamic_cast<DelayTuplesOperatorHandler*>(ptrOpHandler);
    opHandler->setup(pipelineExecutionContext->getNumberOfWorkerThreads());
}

void DelayTuples::setup(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    nautilus::invoke(setupOpHandlerProxy, operatorHandlerMemRef, executionCtx.pipelineContext);

    nautilus::invoke(
        +[](const PipelineExecutionContext* pipelineExecutionContext)
        {
            std::ofstream pipelinesFile;
            pipelinesFile.open("pipelines.txt", std::ios_base::app);
            if (pipelinesFile.is_open())
            {
                pipelinesFile << "DelayTuples pipelineId: " << pipelineExecutionContext->getPipelineId() << std::endl;
                pipelinesFile.flush();
            }
        },
        executionCtx.pipelineContext);
}

void DelayTuples::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();

    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto workerThreadId = executionCtx.getWorkerThreadId();

    // Create new result buffer
    auto resultBuffer = RecordBuffer(executionCtx.allocateBuffer());

    // Create emit indices
    const auto numberOfRecords = recordBuffer.getNumRecords();
    nautilus::invoke(
        +[](OperatorHandler* opHandler, WorkerThreadId threadId, uint64_t numberOfTuples)
        {
            PRECONDITION(opHandler != nullptr, "opHandler context should not be null!");
            dynamic_cast<DelayTuplesOperatorHandler*>(opHandler)->createEmitIndicesForInputBuffer(threadId, numberOfTuples);
        },
        operatorHandlerMemRef,
        workerThreadId,
        numberOfRecords);

    // Iterate over all tuples and write record to the output buffer at the position specified by emitIndices
    for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
    {
        nautilus::val<uint64_t> outputIndex = nautilus::invoke(
            +[](OperatorHandler* opHandler, WorkerThreadId threadId, const uint64_t index)
            {
                PRECONDITION(opHandler != nullptr, "opHandler context should not be null!");
                return dynamic_cast<DelayTuplesOperatorHandler*>(opHandler)->getEmitIndicesForWorker(threadId)[index];
            },
            operatorHandlerMemRef,
            workerThreadId,
            i);
        memoryProvider->writeRecord(outputIndex, resultBuffer, memoryProvider->readRecord(projections, recordBuffer, i));
    }

    // Set the metadata and emit buffer
    emitRecordBuffer(executionCtx, recordBuffer, resultBuffer);
}

void DelayTuples::close(ExecutionContext&, RecordBuffer&) const
{
}

void DelayTuples::emitRecordBuffer(ExecutionContext& ctx, RecordBuffer& inputBuffer, RecordBuffer& ouputBuffer) const
{
    ouputBuffer.setNumRecords(inputBuffer.getNumRecords());
    ouputBuffer.setWatermarkTs(inputBuffer.getWatermarkTs());
    ouputBuffer.setOriginId(inputBuffer.getOriginId());
    ouputBuffer.setSequenceNumber(inputBuffer.getSequenceNumber());
    ouputBuffer.setChunkNumber(inputBuffer.getChunkNumber());
    ouputBuffer.setLastChunk(inputBuffer.isLastChunk());
    ouputBuffer.setCreationTs(inputBuffer.getCreatingTs());
    ctx.emitBuffer(ouputBuffer);
}
} // namespace NES::Runtime::Execution::Operators