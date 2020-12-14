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

#ifndef NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlanStatus.hpp>
#include <mutex>
#include <atomic>
namespace NES {
using NodeEngine::TupleBuffer;
using NodeEngine::WorkerContext;
using NodeEngine::Execution::PipelineExecutionContext;
/**
 * @brief The CompiledExecutablePipelineStage maintains a reference to an compiled ExecutablePipelineStage.
 * To this end, it ensures that the compiled code is correctly destructed.
 */
class CompiledExecutablePipelineStage : public NodeEngine::Execution::ExecutablePipelineStage {

  public:
    explicit CompiledExecutablePipelineStage(CompiledCodePtr compiledCode);
    static NodeEngine::Execution::ExecutablePipelineStagePtr create(CompiledCodePtr compiledCode);
    ~CompiledExecutablePipelineStage();

    uint32_t setup(PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t start(PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t open(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) override;
    uint32_t execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext,
                     NodeEngine::WorkerContext& workerContext) override;
    uint32_t close(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) override;
    uint32_t stop(PipelineExecutionContext& pipelineExecutionContext) override;

  private:
    enum ExecutionStage { NotInitialized, Initialized, Running, Stopped };
    NodeEngine::Execution::ExecutablePipelineStagePtr executablePipelineStage;
    CompiledCodePtr compiledCode;
    std::mutex executionStageLock;
    std::atomic<ExecutionStage> currentExecutionStage;
};


}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
