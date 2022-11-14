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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_REMOVEBRONLYBLOCKSPHASE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_REMOVEBRONLYBLOCKSPHASE_HPP_

#include "Nautilus/IR/BasicBlocks/BasicBlock.hpp"
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <memory>
#include <unordered_set>

namespace NES::Nautilus::Tracing {

class ExecutionTrace;
// class Frame;
class Block;
// class ValueRef;
// class BlockRef;

/**
 * @brief This phase converts a execution trace to SSA form.
 * This implies that, each value is only assigned and that all parameters to a basic block are passed by block arguments.
 */
class RemoveBrOnlyBlocksPhase {
  public:
    /**
     * @brief Applies the phase on a execution trace
     * @param trace
     * @return the modified execution trace.
     */
    std::shared_ptr<IR::IRGraph> apply(std::shared_ptr<IR::IRGraph> trace);

    private:
    /**
     * @brief Internal context object, which maintains statue during IR creation.
     */
    class RemoveBrOnlyBlocksPhaseContext {
      public:
        RemoveBrOnlyBlocksPhaseContext(std::shared_ptr<IR::IRGraph> ir) : ir(ir){};
        std::shared_ptr<IR::IRGraph> process();

      private:
        void addPredecessors(IR::BasicBlockPtr parentBlock);
        void processBrOnlyBlocks(IR::BasicBlockPtr parentBlock);
        void findLoopHeadBlocks(IR::BasicBlockPtr parentBlock);
        void createIfOperations(IR::BasicBlockPtr parentBlock);
        // void addScopeLevels(IR::BasicBlockPtr parentBlock);
        // void processBranch(IR::BasicBlockPtr parentBlock, IR::BasicBlockPtr currentBlock, bool conditionalBranch, bool isElse);
        // void processBlock(IR::BasicBlockPtr parentBlock, IR::BasicBlockPtr currentChildBlock);

      private:
        std::shared_ptr<IR::IRGraph> ir;
        std::unordered_set<std::string> visitedBlocks;
    };
};

}// namespace NES::Nautilus::Tracing
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_REMOVEBRONLYBLOCKSPHASE_HPP_
