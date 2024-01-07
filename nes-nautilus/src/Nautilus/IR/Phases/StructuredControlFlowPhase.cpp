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

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopInfo.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Phases/StructuredControlFlowPhase.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Nautilus/Util/IRDumpHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <unordered_map>

using namespace NES::Nautilus::IR::Operations;
namespace NES::Nautilus::IR {

void StructuredControlFlowPhase::apply(std::shared_ptr<IR::IRGraph> ir) {
    auto phaseContext = StructuredControlFlowPhaseContext(std::move(ir));
    phaseContext.process();
};

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::process() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    createIfOperations(rootOperation->getFunctionBasicBlock());
}

bool StructuredControlFlowPhase::StructuredControlFlowPhaseContext::mergeBlockCheck(
    IR::BasicBlockPtr& currentBlock,
    std::stack<std::unique_ptr<IfOpCandidate>>& ifOperations,
    std::unordered_map<std::string, uint32_t>& numMergeBlocksVisits,
    bool newVisit,
    const std::unordered_set<IR::BasicBlockPtr>& loopBlockWithVisitedBody) {
    uint32_t openEdges = 0;
    uint32_t numPriorVisits = 0;
    bool mergeBlockFound = false;

    bool isAlreadyVisitedMergeBlock = numMergeBlocksVisits.contains(currentBlock->getIdentifier());
    if (isAlreadyVisitedMergeBlock) {
        // We deduct '1' from the number of prior visits so that openEdges is > 1 even if we already visited all the
        // merge-blocks's open edges. This is important to not accidentally recognize branch-blocks(openEdges: 1) as
        // merge-blocks.
        numPriorVisits = numMergeBlocksVisits.at(currentBlock->getIdentifier()) - 1;
    }
    // Calculating openEdges:
    //  If we did not loop back to a loop-block coming from the loop-block's body:
    //  -> deduct the number of prior visits from the number of predecessors of the block.
    //  -> if it is a loop-header-block, deduct the number of loopBackEdges.
    //  Else:
    //  -> simply deduct the number of prior visits from the number of loopBackEdges.
    //  -> if the loop-header-block has no more openEdges, we exhausted its true-branch and switch to its false-branch.
    if (!loopBlockWithVisitedBody.contains(currentBlock)) {
        openEdges = currentBlock->getPredecessors().size() - numPriorVisits
            - (currentBlock->isLoopHeaderBlock()) * currentBlock->getNumLoopBackEdges();
    } else {
        openEdges = currentBlock->getNumLoopBackEdges() - numPriorVisits;
        if (openEdges < 2) {
            // We exhausted the loop-operations true-branch (body block) and now switch to its false-branch.
            currentBlock = std::static_pointer_cast<Operations::LoopOperation>(currentBlock->getTerminatorOp())
                               ->getLoopFalseBlock()
                               .getBlock();
            // Since we switched to a new currentBlock, we need to check whether it is a merge-block with openEdges.
            // If the new currentBlock is a loop-header-block again, we have multiple recursive calls.
            return mergeBlockCheck(currentBlock, ifOperations, numMergeBlocksVisits, true, loopBlockWithVisitedBody);
        }
    }
    // If the number of openEdges is 2 or greater, we found a merge-block.
    mergeBlockFound = openEdges > 1;
    // If we found a merge-block, and we came from a new edge increase the visit counter by 1 or set it to 1.
    if (mergeBlockFound && newVisit && isAlreadyVisitedMergeBlock) {
        numMergeBlocksVisits[currentBlock->getIdentifier()] = numMergeBlocksVisits[currentBlock->getIdentifier()] + 1;
    } else if (mergeBlockFound && newVisit && !isAlreadyVisitedMergeBlock) {
        numMergeBlocksVisits.emplace(std::pair{currentBlock->getIdentifier(), 1});
    }
    return mergeBlockFound;
}

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::createIfOperations(IR::BasicBlockPtr currentBlock) {
    std::stack<std::unique_ptr<IfOpCandidate>> ifOperations;
    std::stack<IR::BasicBlockPtr> mergeBlocks;
    std::unordered_map<std::string, uint32_t> numMergeBlockVisits;
    std::unordered_set<IR::BasicBlockPtr> loopBlockWithVisitedBody;
    bool mergeBlockFound = true;
    // The newVisit flag is passed to the mergeBlockCheck() function to indicate whether we traversed a new edge to the
    // currentBlock before calling mergeBlockCheck().
    bool newVisit = true;

    // Iterate over graph until all if-operations have been processed and matched with their corresponding merge-blocks.
    while (mergeBlockFound) {
        // Check blocks (DFS) until an open merge-block was found. Push encountered if-operations to stack.
        // In a nutshell, we identify open merge-blocks by checking the number of incoming edges vs mergeBlockNumVisits
        // and numLoopBackEdges. For example, a block that has 2 incoming edges, 2 numMergeBlockVisits, and 0
        // numLoopBackEdges is an closed merge-block that merges two control-flow-branches. In contrast, a block that has
        // 5 incoming edges, 2 numMergeBlockVisits, and 1 numLoopBackEdges is an open merge-block with still 2 open
        // control-flow-merge-edges. Also, it is a loop-header-block with 1 numLoopBackEdge. (5-2-1 => 2 still open)
        while (!(mergeBlockFound =
                     mergeBlockCheck(currentBlock, ifOperations, numMergeBlockVisits, newVisit, loopBlockWithVisitedBody))
               && (currentBlock->getTerminatorOp()->getOperationType() != Operation::OperationType::ReturnOp)) {
            auto terminatorOp = currentBlock->getTerminatorOp();
            if (terminatorOp->getOperationType() == Operation::OperationType::BranchOp) {
                // If the currentBlock is a simple branch-block, we move to the nextBlock.
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                currentBlock = branchOp->getNextBlockInvocation().getBlock();
                newVisit = true;
            } else if (terminatorOp->getOperationType() == Operation::OperationType::IfOp) {
                // If the currentBlock is an if-block, we push its if-operation on top of our IfOperation stack.
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                ifOperations.emplace(std::make_unique<IfOpCandidate>(IfOpCandidate{ifOp, true}));
                // We now follow the if-operation's true-branch until we find a new if-operation or a merge-block.
                currentBlock = ifOp->getTrueBlockInvocation().getBlock();
                newVisit = true;
            } else if (terminatorOp->getOperationType() == Operation::OperationType::LoopOp) {
                loopBlockWithVisitedBody.emplace(currentBlock);
                if (numMergeBlockVisits.contains(currentBlock->getIdentifier())) {
                    numMergeBlockVisits.erase(currentBlock->getIdentifier());
                }
                auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(terminatorOp);
                currentBlock = loopOp->getLoopBodyBlock().getBlock();
                newVisit = true;
            }
        }
        // If no merge-block was found, we traversed the entire graph and are done (return block is current block).
        if (mergeBlockFound) {
            // If a merge-block was found, depending on whether the we are in the current if-operations' true
            // or false branch, we either set it as the current if-operation's true-branch-block,
            // or set it as current if-operation's false-branch-block.
            if (ifOperations.top()->isTrueBranch) {
                // We explored the current if-operation's true-branch and now switch to its false-branch.
                ifOperations.top()->isTrueBranch = false;
                mergeBlocks.emplace(currentBlock);
                currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getBlock();
                newVisit = true;
            } else {
                // Make sure that we found the current merge-block for the current if-operation.
                assert(mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier());
                // Set currentBlock as merge-block for the current if-operation and all if-operations on the stack that:
                //  1. Are in their false-branches (their merge-block was pushed to the stack).
                //  2. Have a corresponding merge-block on the stack that matches the currentBlock.
                do {
                    ifOperations.top()->ifOp->setMergeBlock(std::move(mergeBlocks.top()));
                    mergeBlocks.pop();
                    ifOperations.pop();
                } while (!ifOperations.empty() && !ifOperations.top()->isTrueBranch && !mergeBlocks.empty()
                         && mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier());
                // In this case, we do not visit a block via a new edge, so newVisit is false.
                // This is important in case the the current top-most if-operation on the stack is in its true-branch
                // and the currentBlock is its merge-block.
                newVisit = false;
            }
        }
    }
}

}//namespace NES::Nautilus::IR