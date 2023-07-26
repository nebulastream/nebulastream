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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_RUNTIME_RUNTIMEPIPELINECONTEXT_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_RUNTIME_RUNTIMEPIPELINECONTEXT_HPP_

#include <Runtime/TupleBuffer.hpp>
#include <memory>
#include <unordered_map>

namespace NES::ExecutionEngine::Experimental {
class ExecutablePipeline;
}
namespace NES::Nautilus {
class Operator;
};
namespace NES::Runtime::Execution {

/**
 * @brief The PipelineExecutionContext is passed to a compiled pipeline and offers basic functionality to interact with the Runtime.
 * Via the context, the compile code is able to allocate new TupleBuffers and to emit tuple buffers to the Runtime.
 */
class RuntimePipelineContext : public std::enable_shared_from_this<RuntimePipelineContext> {
  public:
    using OperatorStateTag = uint32_t;
    RuntimePipelineContext::OperatorStateTag registerGlobalOperatorState(int64_t op,
                                                                         std::unique_ptr<Nautilus::OperatorState> operatorState);
    __attribute__((always_inline)) Nautilus::OperatorState* getGlobalOperatorState(int64_t tag);
    void dispatchBuffer(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& tb);
    void addSuccessorPipeline(std::shared_ptr<ExecutionEngine::Experimental::ExecutablePipeline> pipeline);

  private:
    std::unordered_map<int64_t, std::unique_ptr<Nautilus::OperatorState>> operatorStates;
    std::vector<std::shared_ptr<ExecutionEngine::Experimental::ExecutablePipeline>> successors;
};

}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_RUNTIME_RUNTIMEPIPELINECONTEXT_HPP_
