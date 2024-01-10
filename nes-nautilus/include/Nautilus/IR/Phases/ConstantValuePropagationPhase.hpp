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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_PHASES_CONSTANTVALUEPROPAGATIONPHASE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_PHASES_CONSTANTVALUEPROPAGATIONPHASE_HPP_

#include "Nautilus/IR/Operations/ConstIntOperation.hpp"
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
class ConstantValuePropagationPhase {
  public:
    /**
     * @brief Applies the ConstantValuePropagationPhase to the supplied IR graph.
     * @param IR graph that the ConstantValuePropagationPhase is applied to.
     */
    void apply(std::shared_ptr<IR::IRGraph> ir);

  private:
    /**
     * @brief Internal context object contains phase logic and state.
     */
    class ConstantValuePropagationPhaseContext {
      public:
        struct ConstOpInfo {
            Operations::OperationPtr operation;
            NES::Nautilus::IR::BasicBlockPtr definedInBlock;
            std::shared_ptr<Operations::ConstIntOperation> rootConstOp;
            int64_t constValue;
            bool isUsed;
            // std::vector<NES::Nautilus::IR::BasicBlockPtr> usedBy;
            bool isComplex;
        };
        /**
         * @brief Constructor for the context of the remove br-only-blocks phase.
         * 
         * @param ir: IRGraph to which the remove br-only-blocks phase will be applied.
         */
        ConstantValuePropagationPhaseContext(std::shared_ptr<IR::IRGraph> ir) : ir(ir){};

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
        void handleBlockInvocation(Operations::BasicBlockInvocation basicBlockInvocation);

        void detectComplexVariable(Operations::BasicBlockInvocation basicBlockInvocation);

        void removeConstOpFromBlock(const Operations::OperationPtr& constOp, const NES::Nautilus::IR::BasicBlockPtr& currentBlock);

        void replaceInputArgumentUsages(Operations::OperationPtr toReplace, Operations::OperationPtr replaceWith, BasicBlockPtr currentBlock);
        /**
         * @brief Todo
         * 
         * @param currentBlock: Current block that might be a br-only-block.
         * @param candidates: Blocks that still need to be processed.
         * @param visitedBlocks: Blocks that have already been processed and can be disregarded.
         */
        void inline processBlock(IR::BasicBlockPtr currentBlock,
                                 std::stack<IR::BasicBlockPtr>& newBlocks,
                                 std::unordered_set<std::string> visitedBlocks);
        /**
         * @brief Todo
         * 
         * @param currentBlock: Current block that might be a br-only-block.
         * @param candidates: Blocks that still need to be processed.
         * @param visitedBlocks: Blocks that have already been processed and can be disregarded.
         */
        void inline buildConstOpMap(IR::BasicBlockPtr currentBlock);
        
        void inline propagateConstantValues();

        void inline foldConstantValues(IR::BasicBlockPtr currentBlock);

      private:
        std::shared_ptr<IR::IRGraph> ir;
        std::unordered_map<std::string, ConstOpInfo> constOpMap;
        std::unordered_set<std::string> nonConstOpArgs;
        // std::unordered_map<std::string, std::vector<std::shared_ptr<Operations::ConstIntOperation>>> blockToConstOpMap;
    };
};

}// namespace NES::Nautilus::IR
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_PHASES_CONSTANTVALUEPROPAGATIONPHASE_HPP_
