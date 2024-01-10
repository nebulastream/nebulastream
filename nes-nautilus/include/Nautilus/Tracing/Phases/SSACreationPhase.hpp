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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_PHASES_SSACREATIONPHASE_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_PHASES_SSACREATIONPHASE_HPP_

#include <memory>
#include <set>

namespace NES::Nautilus::Tracing {

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
    /**
     * @brief Applies the phase on a execution trace
     * @param trace
     * @return the modified execution trace.
     */
    std::shared_ptr<ExecutionTrace> apply(std::shared_ptr<ExecutionTrace> trace);

  private:
    /**
     * @brief The SSACreationPhaseContext maintains local state, which is required to calculate the ssa from.
     */
    class SSACreationPhaseContext {
      public:
        SSACreationPhaseContext(std::shared_ptr<ExecutionTrace> trace);
        /*
         * Starts the conversion of the trace to SSA from
         */
        std::shared_ptr<ExecutionTrace> process();

      private:
        /**
         * @brief Converts a single basic block to SSA form
         * @param block reference to the basic block
         */
        void processBlock(Block& block);

        /**
         * @brief Checks if an ValueRef is defined in a specific block by an argument or an operation before the current operationIndex
         * @param block reference to the current basic block
         * @param valRef reference to the the value ref we are looking fore
         * @param operationIndex the operation index, which accesses the ValueRef
         * @return true if Value Ref is defined locally.
         */
        bool isLocalValueRef(Block& block, ValueRef& valRef, uint32_t operationIndex);
        void processValueRef(Block& block, ValueRef& type, uint32_t operationIndex);
        void processBlockRef(Block& block, BlockRef& blockRef, uint32_t operationIndex);

        /**
         * @brief Removes the assignment operations from all blocks.
         * Assignment operations are only required to infer SSA form.
         */
        void removeAssignOperations();

        /**
         * @brief In this step we finalise the block arguments of all blocks and create unique variances of them.
         */
        void makeBlockArgumentsUnique();

      private:
        std::shared_ptr<ExecutionTrace> trace;
        std::set<uint32_t> processedBlocks;
    };
};

}// namespace NES::Nautilus::Tracing
#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_PHASES_SSACREATIONPHASE_HPP_
