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

#include <memory>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace NES::Nautilus::IR {
  namespace Operations {
    class Operation;
    using OperationPtr = std::shared_ptr<Operation>;
    class LoopOperation;
  }
  class IRGraph;
  class BasicBlock;
  using BasicBlockPtr = std::shared_ptr<BasicBlock>;
}
namespace std {
  template<typename>
  class optional;
}

namespace NES::Nautilus::IR {
/**
 * @brief The first phase(LoopDetection) takes an IR graph with blocks that either have branch- or if-operations as 
 *        terminator-operations. Subsequently, this phase detects which if-operations are loop operations. 
 *        The second phase(CountedLoopDetection) requires the ValueScoping phase to be applied before. It analyzes all
 *        loops found in the LoopDetection phase and tries to map them to counted loops.
 */
class LoopDetectionPhase {
  public:
    /**
     * @brief Applies the LoopDetectionPhase to the supplied IR graph.
     * @requirements RemoveBrOnlyPhase
     * @param IR graph that the LoopDetectionPhase is applied to.
     */
    void applyLoopDetection(std::shared_ptr<IR::IRGraph> ir);

    /** 
     * @brief Applies the CountedLoopDetectionPhase to the supplied IR graph.
     * @requirements RemoveBrOnlyPhase, LoopDetectionPhase::applyLoopDetection, StructuredControlFlowPhase, 
                     ValueScopingPhase
     */
    void applyCountedLoopDetection();

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
        void processLoopDetection();

        /**
         * @brief Iterates over loop operations found in 'applyLoopDetection()', and tries to convert each loop 
         *        operation into a counted loop operation. REQUIRES to apply ValueScopingPhase beforehand.
         */
        void processCountedLoopDetection();

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
         */
        void inline checkBranchForLoopHeadBlocks(IR::BasicBlockPtr& currentBlock, 
                        std::stack<IR::BasicBlockPtr>& ifBlocks, std::unordered_set<std::string>& visitedBlocks, 
                        std::unordered_set<std::string>& loopHeaderCandidates, IR::BasicBlockPtr& priorBlock);

        /**
         * @brief NES IR compare operations can be represented by up to three operations (e.g. '<=' is ('<', 'or', '==')
         *        getCompareOp tries to detect the underlying compare operation and returns its first operation.
         * 
         * @param conditionOp: The condition operation (e.g. of if- or loop-operation)
         * @return std::optional<Operations::OperationPtr> : nullopt if we cannot detect the higher level compare 
         *                                                operation, else, the first operation of the compare operation. 
         */
        std::optional<Operations::OperationPtr> getCompareOp(Operations::OperationPtr conditionOp);
        /**
         * @brief TODO
         * 
         */
        void findCountedLoops();
      private:
        std::shared_ptr<IR::IRGraph> ir;
        std::vector<std::shared_ptr<Operations::LoopOperation>> loopOps;
    };
    private:
      std::unique_ptr<LoopDetectionPhaseContext> loopDetectionPhaseContext;
};

}// namespace NES::Nautilus::IR::LoopDetectionPhase
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_LOOPDETECTIONPHASE_HPP_