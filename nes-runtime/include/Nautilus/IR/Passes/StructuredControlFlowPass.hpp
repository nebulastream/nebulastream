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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_STRUCTUREDCONTROLFLOWPASS_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_STRUCTUREDCONTROLFLOWPASS_HPP_

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
 * @brief This phase converts a execution trace to SSA form.
 * This implies that, each value is only assigned and that all parameters to a basic block are passed by block arguments.
 */
class StructuredControlFlowPass {
  public:
    /**
     * @brief Applies the phase on a execution trace.
     * @param Trace.
     * @return The modified execution trace.
     */
    std::shared_ptr<IR::IRGraph> apply(std::shared_ptr<IR::IRGraph> ir, bool findSimpleCountedLoops = false);

    private:
    /**
     * @brief Internal context object, which maintains statue during IR creation.
     */
    class StructuredControlFlowPassContext {
      public:
        /**
         * @brief Helper struct used to keep track of the currently active branch of an if-operation.
         */
        struct IfOpCandidate {
            std::shared_ptr<IR::Operations::IfOperation> ifOp;
            bool isTrueBranch;
        };

        /**
         * @brief Constructor for the context of the remove br-only-blocks phase.
         * 
         * @param ir: IRGraph to which the remove br-only-blocks phase will be applied.
         */
        StructuredControlFlowPassContext(std::shared_ptr<IR::IRGraph> ir, bool findSimpleCountedLoops) 
            : ir(ir), findSimpleCountedLoops(findSimpleCountedLoops){};
        /**
         * @brief Actually applies the remove br-only-blocks phase to the IR.
         * 
         * @return std::shared_ptr<IR::IRGraph> The IR without br-only-blocks.
         */
        std::shared_ptr<IR::IRGraph> process();

      private:

        /**
         * @brief Iterates over IR graph, finds all loop-header-blocks, and marks them.
         * 
         * @param currentBlock: Initially will be the body-block of the root operation.
         */
        void findLoopHeadBlocks(IR::BasicBlockPtr currentBlock);

        void inline checkBranchForLoopHeadBlocks(IR::BasicBlockPtr& currentBlock, std::stack<IR::BasicBlockPtr>& candidates, 
                        std::unordered_set<std::string>& visitedBlocks, std::unordered_set<std::string>& loopHeadCandidates);

        /**
         * @brief Iterates over IR graph, and connects all if-operations with their corresponding merge-blocks.
         * 
         * @param currentBlock: Initially will be the body-block of the root operation.
         */
        void createIfOperations(IR::BasicBlockPtr currentBlock);

        /**
         * @brief Checks if the given currentBlocks is a merge-block with open edges.
         * 
         * @param currentBlock: Initially will be the body-block of the root operation.
         * @param ifOperations: A stack that contains all visited if-operations 
                                that have not been matched with merge-blocks yet.
         * @param mergeBlockNumVisits: A HashMap that keeps track on how many times we visited a merge-block already.
         * @param newVisit: Signals whether we reached the currentBlock via a new edge.
         * @return true, if the currentBlock is a merge-block with open edges, and false if not.
         */
        bool inline mergeBlockCheck(IR::BasicBlockPtr& currentBlock, 
                    std::stack<std::unique_ptr<IfOpCandidate>>& ifOperations,
                    std::unordered_map<std::string, uint32_t>& candidateEdgeCounter, 
                    const bool newVisit,
                    const std::unordered_set<IR::BasicBlockPtr>& loopBlockWithVisitedBody);

      private:
        std::shared_ptr<IR::IRGraph> ir;
        std::unordered_set<std::string> visitedBlocks;
        bool findSimpleCountedLoops;
    };
};

}// namespace NES::Nautilus::Tracing
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_STRUCTUREDCONTROLFLOWPASS_HPP_
