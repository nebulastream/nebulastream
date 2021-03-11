/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/LocalBufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <utility>

namespace NES::NodeEngine::Execution {

static constexpr auto NUM_BUFFERS_PER_PIPELINE = 64;

PipelineExecutionContext::PipelineExecutionContext(QuerySubPlanId queryId, QueryManagerPtr queryManager,
                                                   BufferManagerPtr bufferManager,
                                                   std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunction,
                                                   std::function<void(TupleBuffer&)>&& emitToQueryManagerFunctionHandler,
                                                   std::vector<OperatorHandlerPtr> operatorHandlers)
    : queryId(queryId), queryManager(queryManager),
      localBufferPool(bufferManager->createLocalBufferManager(NUM_BUFFERS_PER_PIPELINE)),
      emitFunctionHandler(std::move(emitFunction)),
      emitToQueryManagerFunctionHandler(std::move(emitToQueryManagerFunctionHandler)),
      operatorHandlers(std::move(operatorHandlers)) {
    NES_DEBUG("Created PipelineExecutionContext() " << toString());
}

PipelineExecutionContext::PipelineExecutionContext(QuerySubPlanId queryId, QueryManagerPtr queryManager,
                                                   LocalBufferManagerPtr bufferManager,
                                                   std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunction,
                                                   std::function<void(TupleBuffer&)>&& emitToQueryManagerFunctionHandler,
                                                   std::vector<OperatorHandlerPtr> operatorHandlers)
    : queryId(queryId), queryManager(queryManager),
      localBufferPool(bufferManager),
      emitFunctionHandler(std::move(emitFunction)),
      emitToQueryManagerFunctionHandler(std::move(emitToQueryManagerFunctionHandler)),
      operatorHandlers(std::move(operatorHandlers)) {
    NES_DEBUG("Created PipelineExecutionContext() " << toString());
}

PipelineExecutionContext::~PipelineExecutionContext() {
    NES_DEBUG("~PipelineExecutionContext() " << toString());
}

TupleBuffer PipelineExecutionContext::allocateTupleBuffer() { return localBufferPool->getBuffer(); }

void PipelineExecutionContext::emitBuffer(TupleBuffer& buffer, WorkerContextRef workerContext) {
    // call the function handler
    emitFunctionHandler(buffer, workerContext);
}

void PipelineExecutionContext::dispatchBuffer(TupleBuffer& buffer) {
    // call the function handler
    emitToQueryManagerFunctionHandler(buffer);
}

std::vector<OperatorHandlerPtr> PipelineExecutionContext::getOperatorHandlers() { return operatorHandlers; }

std::string PipelineExecutionContext::toString() { return "PipelineContext(queryID:" + std::to_string(queryId); }

}// namespace NES::NodeEngine::Execution