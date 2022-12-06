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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_LOOPDETECTIONPHASE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_LOOPDETECTIONPHASE_HPP_

// Todo forward refs?
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <memory>
#include <stack>
#include <unordered_map>
#include <unordered_set>

namespace NES::Nautilus::IR {

/**
 * @brief This phase takes an IR graph with blocks that either have branch- or if-operations as terminator-operations.
 *        Subsequently, this phase detects which if-operations are loop operations, converts them and enriches them with
 *        relevant information.
 */
class LoopDetectionPhase {
  public:
    /**
     * @brief Applies the LoopDetectionPhase to the supplied IR graph.
     * @param IR graph that the LoopDetectionPhase is applied to.
     */
    void apply(std::shared_ptr<IR::IRGraph> ir);

    private:
    /**
     * @brief Internal context object contains phase logic and state.
     */
    class LoopDetectionPhaseContext {
      public:

        /**
         * @brief Constructor for the context of the LoopDetectionPhaseContext.
         * 
         * @param ir: IRGraph to which LoopDetectionPhaseContext will be applied.
         */
        LoopDetectionPhaseContext(std::shared_ptr<IR::IRGraph> ir) : ir(ir){};
        /**
         * @brief Actually applies the LoopDetectionPhaseContext to the IR.
         */
        void process();

      private:
        /**
         * @brief Iterates over IR graph, finds all loop-header-blocks, and marks them.
         * 
         * @param currentBlock: Initially will be the body-block of the root operation.
         */
        void findLoopHeadBlocks(IR::BasicBlockPtr currentBlock);

        /**
         * @brief Traverses a branch of the IR graph until either an already visited block or the return block 
         *        is encountered. If the already visited block contains an if-operation and if we did not fully 
         *        exhaust the true-branch of that if-operation, we found a loop-header and can construct a loop operation.
         * 
         * @param currentBlock: The block that we are currently processing.
         * @param ifBlocks: Blocks that contain an if-operation as their terminator operation.
         * @param visitedBlocks: Blocks that have already been processed.
         * @param loopHeaderCandidates: ifBlocks with true-branches that have not been fully explored yet.
         * @param priorBlock: We keep track of the previous block to assign the loop-end-block, when the currentBlock
         *                    is a loop-header block and we are creating its loop-operation. 
         * @param constantValues: To create counted-for-loops we have to keep track of all constants that are created in
         *                        the IR, and of their argument names in case they are passed between blocks.
         */
        void inline checkBranchForLoopHeadBlocks(IR::BasicBlockPtr& currentBlock, 
                        std::stack<IR::BasicBlockPtr>& ifBlocks, std::unordered_set<std::string>& visitedBlocks, 
                        std::unordered_set<std::string>& loopHeaderCandidates, IR::BasicBlockPtr& priorBlock,
                        std::unordered_map<std::string, std::shared_ptr<Operations::ConstIntOperation>>& constantValues);
        /**
         * @brief We check all blocks for constant int operations which might represent the lower- or upperBound or the 
         *        stepSize of loop-operations. We also keep constant value forwarding between blocks.
         * 
         * @param currentBlock: The block that we are currently checking for constant int operations.
         * @param constantValues: An auxiliary hash map that allows us to keep track of all constant int operations
         *                        and their argument names.
         */
        void findAndAddConstantOperations(BasicBlockPtr& currentBlock, 
                        std::unordered_map<std::string, std::shared_ptr<Operations::ConstIntOperation>>& constantValues);
      private:
        std::shared_ptr<IR::IRGraph> ir;
        std::unordered_set<std::string> visitedBlocks;
    };
};

}// namespace NES::Nautilus::IR::LoopDetectionPhase
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_LOOPDETECTIONPHASE_HPP_