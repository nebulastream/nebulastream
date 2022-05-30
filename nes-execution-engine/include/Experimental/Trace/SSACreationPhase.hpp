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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_SSACREATIONPHASE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_SSACREATIONPHASE_HPP_

#include <set>

namespace NES::ExecutionEngine::Experimental::Trace {

class ExecutionTrace;
class Block;
class ValueRef;
class BlockRef;

/**
 * @brief This phase converts a execution trace to SSA form.
 * This implies that, each value is only assigned and that all parameters to a basic block are passed by block arguments.
 */
class SSACreationPhase {
  public:
    std::shared_ptr<ExecutionTrace> apply(std::shared_ptr<ExecutionTrace> trace);

  private:
    class SSACreationPhaseContext {
      public:
        SSACreationPhaseContext(std::shared_ptr<ExecutionTrace> trace);
        std::shared_ptr<ExecutionTrace> process();

      private:
        bool isLocalValueRef(Block& block, ValueRef& type, uint32_t operationIndex);
        void processVariableRef(Block& block, ValueRef& type, uint32_t operationIndex);
        void processBlockRef(Block& block, BlockRef& blockRef, uint32_t operationIndex);
        void removeAssignments();
        void processBlock(Block& block);

      private:
        std::shared_ptr<ExecutionTrace> trace;
        std::set<uint32_t> processedBlocks;
    };
};

}// namespace NES::ExecutionEngine::Experimental::Trace
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_SSACREATIONPHASE_HPP_
