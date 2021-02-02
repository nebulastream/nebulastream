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

#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipelineStage.hpp>
#include <Util/Logger.hpp>
namespace NES {

// TODO this might change across OS
#if defined(__linux__)
static constexpr auto MANGELED_ENTRY_POINT = "_ZN3NES6createEv";
#else
#error "unsupported platform/OS"
#endif

typedef NodeEngine::Execution::ExecutablePipelineStagePtr (*CreateFunctionPtr)();

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(CompiledCodePtr compiledCode, PipelineStageArity arity)
    : base(arity), compiledCode(compiledCode), currentExecutionStage(NotInitialized) {
    auto createFunction = compiledCode->getFunctionPointer<CreateFunctionPtr>(MANGELED_ENTRY_POINT);
    this->executablePipelineStage = (*createFunction)();
}

NodeEngine::Execution::ExecutablePipelineStagePtr CompiledExecutablePipelineStage::create(CompiledCodePtr compiledCode,
                                                                                          PipelineStageArity arity) {
    return std::make_shared<CompiledExecutablePipelineStage>(compiledCode, arity);
}

CompiledExecutablePipelineStage::~CompiledExecutablePipelineStage() {
    // First we have to destroy the pipeline stage only afterwards we can remove the associated code.
    NES_DEBUG("~CompiledExecutablePipelineStage()");
    this->executablePipelineStage.reset();
    this->compiledCode.reset();
}

uint32_t CompiledExecutablePipelineStage::setup(NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext) {
    const std::lock_guard<std::mutex> lock(executionStageLock);
    if (currentExecutionStage != NotInitialized) {
        NES_FATAL_ERROR("CompiledExecutablePipelineStage: The pipeline stage, is already initialized."
                        "It is not allowed to call setup multiple times.");
        return -1;
    }
    currentExecutionStage = Initialized;
    return executablePipelineStage->setup(pipelineExecutionContext);
}

uint32_t CompiledExecutablePipelineStage::start(NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext) {
    const std::lock_guard<std::mutex> lock(executionStageLock);
    if (currentExecutionStage != Initialized) {
        NES_FATAL_ERROR("CompiledExecutablePipelineStage: The pipeline stage, is not initialized."
                        "It is not allowed to call start if setup was not called.");
        return -1;
    }
    NES_DEBUG("CompiledExecutablePipelineStage: Start compiled executable pipeline stage");
    currentExecutionStage = Running;
    return executablePipelineStage->start(pipelineExecutionContext);
}

uint32_t CompiledExecutablePipelineStage::open(NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext,
                                               NodeEngine::WorkerContext& workerContext) {
    const std::lock_guard<std::mutex> lock(executionStageLock);
    if (currentExecutionStage != Running) {
        NES_FATAL_ERROR(
            "CompiledExecutablePipelineStage: The pipeline stage, was not correctly initialized and started. You must first "
            "call setup and start.");
        return -1;
    }
    return executablePipelineStage->open(pipelineExecutionContext, workerContext);
}

uint32_t CompiledExecutablePipelineStage::execute(TupleBuffer& inputTupleBuffer,
                                                  NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext,
                                                  NodeEngine::WorkerContext& workerContext) {
    // we dont get the lock here as we dont was to serialize the execution.
    // currentExecutionStage is an atomic so its still save to read it
    if (currentExecutionStage != Running) {
        NES_ERROR(
            "CompiledExecutablePipelineStage: The pipeline stage, was not correctly initialized and started. You must first "
            "call setup and start.");
        // TODO we have to assure that execute is never called after stop.
        // This is somehow not working currently.
        // return -1;
    }
    return executablePipelineStage->execute(inputTupleBuffer, pipelineExecutionContext, workerContext);
}

uint32_t CompiledExecutablePipelineStage::close(NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext,
                                                NodeEngine::WorkerContext& workerContext) {
    const std::lock_guard<std::mutex> lock(executionStageLock);
    if (currentExecutionStage != Running) {
        NES_FATAL_ERROR(
            "CompiledExecutablePipelineStage: The pipeline stage, was not correctly initialized and started. You must first "
            "call setup and start.");
        return -1;
    }
    return this->executablePipelineStage->close(pipelineExecutionContext, workerContext);
}
uint32_t CompiledExecutablePipelineStage::stop(NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext) {
    if (currentExecutionStage != Running) {
        NES_FATAL_ERROR(
            "CompiledExecutablePipelineStage: The pipeline stage, was not correctly initialized and started. You must first "
            "call setup and start.");
        return -1;
    }
    NES_DEBUG("CompiledExecutablePipelineStage: Stop compiled executable pipeline stage");
    currentExecutionStage = Stopped;
    return executablePipelineStage->stop(pipelineExecutionContext);
}

}// namespace NES