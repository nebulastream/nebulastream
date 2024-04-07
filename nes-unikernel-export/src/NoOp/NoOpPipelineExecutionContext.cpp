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

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <utility>

namespace NES::Runtime::Execution {
PipelineExecutionContext::PipelineExecutionContext(uint64_t pipelineId,
                                                   DecomposedQueryPlanId queryId,
                                                   Runtime::BufferManagerPtr bufferProvider,
                                                   size_t numberOfWorkerThreads,
                                                   std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunction,
                                                   std::function<void(TupleBuffer&)>&& emitToQueryManagerFunctionHandler,
                                                   std::vector<OperatorHandlerPtr> operatorHandlers)
    : pipelineId(pipelineId), queryId(queryId), emitFunctionHandler(std::move(emitFunction)),
      emitToQueryManagerFunctionHandler(std::move(emitToQueryManagerFunctionHandler)),
      operatorHandlers(std::move(operatorHandlers)), bufferProvider(bufferProvider),
      numberOfWorkerThreads(numberOfWorkerThreads) {}

void PipelineExecutionContext::emitBuffer(TupleBuffer& buffer, WorkerContextRef workerContext) { NES_NOT_IMPLEMENTED(); }

void PipelineExecutionContext::dispatchBuffer(TupleBuffer buffer) { NES_NOT_IMPLEMENTED(); }

std::vector<OperatorHandlerPtr> PipelineExecutionContext::getOperatorHandlers() { NES_NOT_IMPLEMENTED(); }

std::string PipelineExecutionContext::toString() const { NES_NOT_IMPLEMENTED(); }
uint64_t PipelineExecutionContext::getNumberOfWorkerThreads() const { NES_NOT_IMPLEMENTED(); }
Runtime::BufferManagerPtr PipelineExecutionContext::getBufferManager() const { NES_NOT_IMPLEMENTED(); }

uint64_t PipelineExecutionContext::getNextChunkNumber(const SeqNumberOriginId seqNumberOriginId) { NES_NOT_IMPLEMENTED(); }

void PipelineExecutionContext::removeSequenceState(const SeqNumberOriginId seqNumberOriginId) { NES_NOT_IMPLEMENTED(); }
bool PipelineExecutionContext::isLastChunk(const SeqNumberOriginId seqNumberOriginId,
                                           const uint64_t chunkNumber,
                                           const bool isLastChunk) {
    NES_NOT_IMPLEMENTED();
}
}// namespace NES::Runtime::Execution
