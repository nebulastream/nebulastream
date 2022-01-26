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

#ifndef NES_INCLUDE_QUERYCOMPILER_COMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_COMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlanStatus.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <atomic>
#include <mutex>
namespace NES {

namespace Compiler {
class DynamicObject;
}

using Runtime::TupleBuffer;
using Runtime::WorkerContext;
using Runtime::Execution::PipelineExecutionContext;
/**
 * @brief The CompiledExecutablePipelineStage maintains a reference to an compiled ExecutablePipelineStage.
 * To this end, it ensures that the compiled code is correctly destructed.
 */
class CompiledExecutablePipelineStage : public Runtime::Execution::ExecutablePipelineStage {
    using base = Runtime::Execution::ExecutablePipelineStage;

  public:
    /**
     * @brief This constructs a compiled pipeline
     * @param compiledCode pointer to compiled code
     * @param arity of the pipeline, e.g., binary or unary
     * @param sourceCode as string
     */
    explicit CompiledExecutablePipelineStage(std::shared_ptr<Compiler::DynamicObject> dynamicObject,
                                             PipelineStageArity arity,
                                             std::string sourceCode);
    static Runtime::Execution::ExecutablePipelineStagePtr
    create(std::shared_ptr<Compiler::DynamicObject> dynamicObject, PipelineStageArity arity, const std::string& sourceCode = "");
    ~CompiledExecutablePipelineStage();

    uint32_t setup(PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t start(PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t open(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) override;
    ExecutionResult execute(TupleBuffer& inputTupleBuffer,
                            PipelineExecutionContext& pipelineExecutionContext,
                            Runtime::WorkerContext& workerContext) override;

    std::string getCodeAsString() override;

    uint32_t close(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) override;
    uint32_t stop(PipelineExecutionContext& pipelineExecutionContext) override;

  private:
    enum ExecutionStage { NotInitialized, Initialized, Running, Stopped };
    Runtime::Execution::ExecutablePipelineStagePtr executablePipelineStage;
    std::shared_ptr<Compiler::DynamicObject> dynamicObject;
    std::mutex executionStageLock;
    std::atomic<ExecutionStage> currentExecutionStage;
    std::string sourceCode;
};

}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_COMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
