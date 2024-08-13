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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_PIPELINES_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_PIPELINES_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <nautilus/Engine.hpp>
#include <Util/Timer.hpp>
#include <future>

namespace NES {
class DumpHelper;
}
namespace NES::Nautilus::Backends {
class Executable;
}
namespace NES::Nautilus::IR {
class IRGraph;
}
namespace NES::Runtime::Execution {
class PhysicalOperatorPipeline;

/**
 * @brief A compiled executable pipeline stage uses nautilus to compile a pipeline to a code snippet.
 */
class CompiledExecutablePipelineStage : public NautilusExecutablePipelineStage {
  public:
    CompiledExecutablePipelineStage(const std::shared_ptr<PhysicalOperatorPipeline>& physicalOperatorPipeline,
                                    const std::string& compilationBackend,
                                    const nautilus::engine::Options& options);
    uint32_t setup(PipelineExecutionContext& pipelineExecutionContext) override;
    ExecutionResult execute(TupleBuffer& inputTupleBuffer,
                            PipelineExecutionContext& pipelineExecutionContext,
                            WorkerContext& workerContext) override;

  private:
    auto compilePipeline();
    std::string compilationBackend;
    const nautilus::engine::Options options;
    nautilus::engine::CallableFunction<void, nautilus::val<int8_t*>, nautilus::val<int8_t*>, nautilus::val<int8_t*>> pipelineFunction;
    std::shared_ptr<nautilus::engine::NautilusEngine> engine;
};

}// namespace NES::Runtime::Execution

#endif// NES_EXECUTION_INCLUDE_EXECUTION_PIPELINES_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
