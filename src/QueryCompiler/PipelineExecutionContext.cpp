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
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <utility>

namespace NES {

PipelineExecutionContext::PipelineExecutionContext(
    QuerySubPlanId queryId,
    BufferManagerPtr bufferManager,
    std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunction,
    Windowing::AbstractWindowHandlerPtr windowHandler)
    : queryId(queryId), bufferManager(std::move(bufferManager)), emitFunctionHandler(std::move(emitFunction)), windowHandler(windowHandler) {
    // nop
}

TupleBuffer PipelineExecutionContext::allocateTupleBuffer() {
    return bufferManager->getBufferBlocking();
}

void PipelineExecutionContext::emitBuffer(TupleBuffer& outputBuffer, WorkerContextRef workerContext) {
    // call the function handler
    emitFunctionHandler(outputBuffer, workerContext);
}

Windowing::LogicalWindowDefinitionPtr PipelineExecutionContext::getWindowDef() {
    return windowDef;
}
void PipelineExecutionContext::setWindowDef(Windowing::LogicalWindowDefinitionPtr windowDef) {
    this->windowDef = windowDef;
}
SchemaPtr PipelineExecutionContext::getInputSchema() {
    return inputSchema;
}
void PipelineExecutionContext::setInputSchema(SchemaPtr inputSchema) {
    this->inputSchema = inputSchema;
}

}// namespace NES