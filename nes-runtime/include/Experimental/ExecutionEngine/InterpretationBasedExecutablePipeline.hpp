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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_INTERPRETATIONBASEDEXECUTABLEPIPELINE_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_INTERPRETATIONBASEDEXECUTABLEPIPELINE_HPP_
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>

namespace NES::ExecutionEngine::Experimental {

class InterpretationBasedExecutablePipeline : public ExecutablePipeline {
  public:
    InterpretationBasedExecutablePipeline(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> pipelineContext,
                                          std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline);
    void setup() override;
    void execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer) override;
};

}// namespace NES::ExecutionEngine::Experimental

#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_INTERPRETATIONBASEDEXECUTABLEPIPELINE_HPP_
