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
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Phases/StructuredControlFlowPhase.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
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
    findLoopHeadBlocks(rootOperation->getFunctionBasicBlock());
    createIfOperations(rootOperation->getFunctionBasicBlock());
}

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::checkBranchForLoopHeadBlocks(
    IR::BasicBlockPtr& currentBlock,
    std::stack<IR::BasicBlockPtr>& candidates,
    std::unordered_set<std::string>& visitedBlocks,
    std::unordered_set<std::string>& loopHeaderCandidates) {
    // Follow the true-branch of the current, and all nested if-operations until either
    // currentBlock is an already visited block, or currentBlock is the return-block.
    // Newly encountered if-operations are added as loopHeadCandidates.
    while (!visitedBlocks.contains(currentBlock->getIdentifier())
           && currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp) {
        auto terminatorOp = currentBlock->getTerminatorOp();
        if (terminatorOp->getOperationType() == Operation::BranchOp) {
            auto nextBlock =
                std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp)->getNextBlockInvocation().getBlock();
            visitedBlocks.emplace(currentBlock->getIdentifier());
            currentBlock = nextBlock;
        } else if (terminatorOp->getOperationType() == Operation::IfOp) {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
            loopHeaderCandidates.emplace(currentBlock->getIdentifier());
            candidates.emplace(currentBlock);
            visitedBlocks.emplace(currentBlock->getIdentifier());
            currentBlock = ifOp->getTrueBlockInvocation().getBlock();
        }
    }
    // If currentBlock is an already visited block that also is a loopHeaderCandidate, we found a loop-header-block.
    if (loopHeaderCandidates.contains(currentBlock->getIdentifier())) {
        //todo replace ifOperation with loopOperation and add LoopInfo #3169
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
        // Currently, we increase the number of loopBackEdges (default: 0) of the loop-header-block.
        // Thus, after this phase, if a block has numLoopBackEdges > 0, it is a loop-header-block.
        // More information will be added in the scope of issue #3169.
        currentBlock->incrementNumLoopBackEdge();
    }
}

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::findLoopHeadBlocks(IR::BasicBlockPtr currentBlock) {
    std::stack<IR::BasicBlockPtr> ifBlocks;
    std::unordered_set<std::string> loopHeaderCandidates;
    std::unordered_set<std::string> visitedBlocks;

    bool returnBlockVisited = false;
    bool noMoreIfBlocks = true;
    // We iterate over the IR graph starting with currentBlock being the body of the root-operation.
    // We stop iterating when we have visited the return block at least once, and there are no more
    // unvisited if-blocks on the stack. If the IR graph is valid, no more unvisited if-operations exist.
    do {
        // Follow a branch through the query branch until currentBlock is either the return- or an already visited block.
        checkBranchForLoopHeadBlocks(currentBlock, ifBlocks, visitedBlocks, loopHeaderCandidates);
        // Set the current values for the loop halting values.
        noMoreIfBlocks = ifBlocks.empty();
        returnBlockVisited = returnBlockVisited || (currentBlock->getTerminatorOp()->getOperationType() == Operation::ReturnOp);
        if (!noMoreIfBlocks) {
            // When we take the false-branch of an ifOperation, we completely exhausted its true-branch.
            // Since loops can only loop back on their true-branch, we can safely stop tracking it as a loop candidate.
            loopHeaderCandidates.erase(ifBlocks.top()->getIdentifier());
            // Set currentBlock to first block in false-branch of ifOperation.
            // The false branch might contain nested loop-operations.
            currentBlock = std::static_pointer_cast<IR::Operations::IfOperation>(ifBlocks.top()->getTerminatorOp())
                               ->getFalseBlockInvocation()
                               .getBlock();
            ifBlocks.pop();
        }
    } while (!(noMoreIfBlocks && returnBlockVisited));
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
            currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getBlock();
            ifOperations.pop();
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
               && (currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp)) {
            auto terminatorOp = currentBlock->getTerminatorOp();
            if (terminatorOp->getOperationType() == Operation::BranchOp) {
                // If the currentBlock is a simple branch-block, we move to the nextBlock.
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                currentBlock = branchOp->getNextBlockInvocation().getBlock();
                newVisit = true;
            } else if (terminatorOp->getOperationType() == Operation::IfOp) {
                // If the currentBlock is an if-block, we push its if-operation on top of our IfOperation stack.
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                ifOperations.emplace(std::make_unique<IfOpCandidate>(IfOpCandidate{ifOp, true}));
                // If the if-operation is a loop-header-if-operation, we processed everything 'above' the loop-header.
                // Thus, we now explore the loop's body, which we remember using the loopBlockWithVisitedBody set.
                // If numMergeBlockVisits exists for the loop-header-block, it will be reset.
                // From now on, we will check whether it is an open-merge-block using its numLoopBackEdges count.
                if (currentBlock->isLoopHeaderBlock()) {
                    loopBlockWithVisitedBody.emplace(currentBlock);
                    if (numMergeBlockVisits.contains(currentBlock->getIdentifier())) {
                        numMergeBlockVisits.erase(currentBlock->getIdentifier());
                    }
                }
                // We now follow the if-operation's true-branch until we find a new if-operation or a merge-block.
                currentBlock = ifOp->getTrueBlockInvocation().getBlock();
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
                    auto mergeBlock = std::move(mergeBlocks.top());
                    ifOperations.top()->ifOp->setMergeBlock(std::move(mergeBlock));
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

}// namespace NES::Nautilus::IR