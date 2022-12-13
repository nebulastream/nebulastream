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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_VALUESCOPINGPHASE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_VALUESCOPINGPHASE_HPP_

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
class ValueScopingPhase {
  public:
    /**
     * @brief Applies the ValueScopingPhase to the supplied IR graph.
     * @param IR graph that the ValueScopingPhase is applied to.
     */
    void apply(std::shared_ptr<IR::IRGraph> ir);

    private:
    /**
     * @brief Internal context object contains phase logic and state.
     */
    class ValueScopingPhaseContext {
      public:

        /**
         * @brief Constructor for the context of the ValueScopingPhaseContext.
         * 
         * @param ir: IRGraph to which ValueScopingPhaseContext will be applied.
         */
        ValueScopingPhaseContext(std::shared_ptr<IR::IRGraph> ir) : ir(ir){};
        /**
         * @brief Actually applies the ValueScopingPhaseContext to the IR.
         */
        void process();

      private:
        /**
         * @brief Iterates over IR graph, finds all loop-header-blocks, and marks them.
         * 
         * @param currentBlock: Initially will be the body-block of the root operation.
         */
        void applyValueScoping();
      private:
        std::shared_ptr<IR::IRGraph> ir;
        std::unordered_set<std::string> visitedBlocks;
    };
};

}// namespace NES::Nautilus::IR::ValueScopingPhase
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_VALUESCOPINGPHASE_HPP_