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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_PHASES_STRUCTUREDCONTROLFLOWPHASE_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_PHASES_STRUCTUREDCONTROLFLOWPHASE_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <memory>
#include <stack>
#include <unordered_map>
#include <unordered_set>

namespace NES::Nautilus::IR {

/**
 * @brief This phase takes an IR graph with blocks that either have branch- or if-operations as terminator-operations.
 *        Subsequently, this phase matches if-operations with their correct corresponding merge-blocks.
 *        Also, if-operations are enriched with information on which blocks branch to the merge-blocks. 
 *        This is required to correctly create MLIR YieldOps.
 */
class StructuredControlFlowPhase {
  public:
    /**
     * @brief Applies the StructuredControlFlowPhase to the supplied IR graph.
     * @param IR graph that the StructuredControlFlowPhase is applied to.
     */
    void apply(std::shared_ptr<IR::IRGraph> ir);

  private:
    /**
     * @brief Internal context object contains phase logic and state.
     */
    class StructuredControlFlowPhaseContext {
      public:
        /**
         * @brief Helper struct used to keep track of the currently active branch of an if-operation.
         */
        struct IfOpCandidate {
            IR::Operations::IfOperation& ifOp;
            bool isTrueBranch;
        };

        /**
         * @brief Constructor for the context of the StructuredControlFlowPhase.
         * 
         * @param ir: IRGraph to which StructuredControlFlowPhase will be applied.
         */
        StructuredControlFlowPhaseContext(std::shared_ptr<IR::IRGraph> ir) : ir(ir){};
        /**
         * @brief Actually applies the StructuredControlFlowPhase to the IR.
         */
        void process();

      private:
        /**
         * @brief Iterates over IR graph, and connects all if-operations with their corresponding merge-blocks.
         * 
         * @param currentBlock: Initially will be the body-block of the root operation.
         */
        void createIfOperations(IR::BasicBlock& currentBlock);

        /**
         * @brief Checks if the given currentBlocks is a merge-block with open edges (open: not yet traversed).
         * 
         * @param currentBlock: Initially will be the body-block of the root operation.
         * @param ifOperations: A stack that contains all visited if-operations 
                                that have not been matched with merge-blocks yet.
         * @param mergeBlockNumVisits: A HashMap that keeps track on how many times we visited a merge-block already.
         * @param newVisit: Signals whether we reached the currentBlock via a new edge.
         * @return true, if the currentBlock is a merge-block with open edges, and false if not.
         */
        bool inline mergeBlockCheck(IR::BasicBlockPtr currentBlock,
                                    std::stack<std::unique_ptr<IfOpCandidate>>& ifOperations,
                                    std::unordered_map<std::string, uint32_t>& numMergeBlocksVisits,
                                    bool newVisit,
                                    const std::unordered_set<IR::BasicBlockPtr> loopBlockWithVisitedBody);

      private:
        std::shared_ptr<IR::IRGraph> ir;
        std::unordered_set<std::string> visitedBlocks;
    };
};

}// namespace NES::Nautilus::IR
#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_IR_PHASES_STRUCTUREDCONTROLFLOWPHASE_HPP_
