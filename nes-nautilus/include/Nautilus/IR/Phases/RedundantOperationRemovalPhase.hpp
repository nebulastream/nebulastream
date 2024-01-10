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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_PHASES_REDUNDANTOPERATIONREMOVALPHASE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_PHASES_REDUNDANTOPERATIONREMOVALPHASE_HPP_

#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <memory>
#include <stack>
#include <unordered_map>
#include <unordered_set>

namespace NES::Nautilus::IR {

/**
 * @brief This phase removes branch-only-blocks from a given IR graph.
 */
class RedundantOperationRemovalPhase {
  public:
    /**
     * @brief Applies the RedundantOperationRemovalPhase to the supplied IR graph.
     * @param IR graph that the RedundantOperationRemovalPhase is applied to.
     */
    void apply(std::shared_ptr<IR::IRGraph> ir);

  private:
    /**
     * @brief Internal context object contains phase logic and state.
     */
    class RedundantOperationRemovalPhaseContext {
      public:
        /**
         * @brief Constructor for the context of the remove br-only-blocks phase.
         * 
         * @param ir: IRGraph to which the remove br-only-blocks phase will be applied.
         */
        RedundantOperationRemovalPhaseContext(std::shared_ptr<IR::IRGraph> ir) : ir(ir){};

        /**
         * @brief Actually applies the remove br-only-blocks phase to the IR.
         */
        void process();

      private:
        /**
         * @brief Adds predecessor information for all blocks in the IR.
         * 
         * @param currentBlock: Initially will be the body-block of the root operation.
         */
        // void addPredecessors(IR::BasicBlockPtr currentBlock);
        void removeRedundantOperations(IR::BasicBlockPtr currentBlock);

      private:
        std::shared_ptr<IR::IRGraph> ir;
        std::unordered_map<std::string, Operations::OperationPtr> operationMap;
    };
};

}// namespace NES::Nautilus::IR
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_PHASES_REDUNDANTOPERATIONREMOVALPHASE_HPP_
