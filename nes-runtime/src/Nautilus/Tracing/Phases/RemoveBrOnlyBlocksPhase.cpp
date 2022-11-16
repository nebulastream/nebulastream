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
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Util/Logger/Logger.hpp>
#include <Nautilus/Tracing/Phases/RemoveBrOnlyBlocksPhase.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <unordered_map>

using namespace NES::Nautilus::IR::Operations;
namespace NES::Nautilus::Tracing {

std::shared_ptr<IR::IRGraph> RemoveBrOnlyBlocksPhase::apply(std::shared_ptr<IR::IRGraph> ir) {
    auto phaseContext = RemoveBrOnlyBlocksPhaseContext(std::move(ir));
    return phaseContext.process();
};

//Todo write general IR processing mechanism that takes functionality to apply -> can be shared by many passes
std::shared_ptr<IR::IRGraph> RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::process() {
    NES_DEBUG(ir->toString());
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    addPredecessors(rootOperation->getFunctionBasicBlock());
    processBrOnlyBlocks(rootOperation->getFunctionBasicBlock());
    NES_DEBUG(ir->toString());
    findLoopHeadBlocks(rootOperation->getFunctionBasicBlock()); // todo -> found error case
    createIfOperations(rootOperation->getFunctionBasicBlock());
    // addScopeLevels(rootOperation->getFunctionBasicBlock());
    NES_DEBUG(ir->toString());
    return std::move(ir);
}

void inline addPredecessorToBlock(IR::BasicBlockPtr previousBlock, std::vector<IR::BasicBlockPtr>& candidates, 
                                  std::unordered_set<std::string> visitedBlocks) {
    auto terminatorOp = previousBlock->getTerminatorOp();
    if(terminatorOp->getOperationType() == Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
        branchOp->getNextBlockInvocation().getBlock()->addPredecessor(previousBlock);
        if(!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace_back(branchOp->getNextBlockInvocation().getBlock());
        }
    } else if (terminatorOp->getOperationType() == Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
        ifOp->getFalseBlockInvocation().getBlock()->addPredecessor(previousBlock);
        ifOp->getTrueBlockInvocation().getBlock()->addPredecessor(previousBlock);
        if(!visitedBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace_back(ifOp->getFalseBlockInvocation().getBlock());
        }
        if(!visitedBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace_back(ifOp->getTrueBlockInvocation().getBlock());
        }
    }
}

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::addPredecessors(IR::BasicBlockPtr currentBlock) {
    std::vector<IR::BasicBlockPtr> candidates;
    std::unordered_set<std::string> visitedBlocks; //todo use ptr instead?
    addPredecessorToBlock(currentBlock, candidates, visitedBlocks);

    while(!candidates.empty()) {
        visitedBlocks.emplace(currentBlock->getIdentifier());
        currentBlock->setScopeLevel(UINT32_MAX); //todo assuming that every block passes this instruction
        currentBlock = candidates.back();
        candidates.pop_back();
        addPredecessorToBlock(currentBlock, candidates, visitedBlocks);
    }
}

void updatePredecessorBlocks(std::vector<IR::BasicBlockPtr> &brOnlyBlocks, IR::BasicBlockPtr newBlock) {
    // Iterate over all passed br-only-blocks and set non-br-only-block as new target block of predecessors.
    // todo update predecessors of newBlock
    // -> remove all predecessors that are in brOnlyBlocks
    // -> add all predecessors of brOnlyBlocks
    std::unordered_map<std::string, IR::BasicBlockPtr> oldBlocks; //rename
    std::vector<std::weak_ptr<IR::BasicBlock>> newBlocks; //rename
    for(auto brOnlyBlock : brOnlyBlocks) {
        oldBlocks.emplace(std::pair{brOnlyBlock->getIdentifier(), brOnlyBlock});
        for(auto predecessor : brOnlyBlock->getPredecessors()) {
            if(!oldBlocks.contains(predecessor.lock()->getIdentifier()))  {
                newBlocks.emplace_back(predecessor);
            }
            auto terminatorOp = predecessor.lock()->getTerminatorOp();
            if(terminatorOp->getOperationType() == Operation::IfOp) {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                //todo implement proper block equals
                // TODO check if true-branch = false-branch
                // -> change from ifOp to brOp if so
                if(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier() == brOnlyBlock->getIdentifier()) {
                    ifOp->getTrueBlockInvocation().setBlock(newBlock);
                } else {
                    ifOp->getFalseBlockInvocation().setBlock(newBlock);
                }
            } else if(terminatorOp->getOperationType() == Operation::BranchOp) {
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                branchOp->getNextBlockInvocation().setBlock(newBlock);
            } else {
                NES_ERROR("RemoveBrOnlyBlocksPhase::updateTerminatorOperation: Case not implemented: " << terminatorOp->getOperationType());
                NES_NOT_IMPLEMENTED();
            }
        }
    }
    //todo improve (also: could we add duplicated predecessors?)
    for(auto predecessor : newBlock->getPredecessors()) {
        if(!oldBlocks.contains(predecessor.lock()->getIdentifier())) {
            newBlocks.emplace_back(predecessor);
        }
    }
    newBlock->getPredecessors().clear();
    for(auto newPredecessor : newBlocks) {
        newBlock->addPredecessor(newPredecessor.lock());
    }
}

void inline removeBrOnlyBlocks(IR::BasicBlockPtr currentBlock, 
                               std::vector<IR::BasicBlockPtr>& candidates, 
                               std::unordered_set<std::string> visitedBlocks) {
    auto terminatorOp = currentBlock->getTerminatorOp();
    if(terminatorOp->getOperationType() == Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
        // BRANCH ONLY CASE
        if(currentBlock->getOperations().size() == 1) {
            std::vector<IR::BasicBlockPtr> brOnlyBlocks;
            while(currentBlock->getOperations().size() == 1) {
                brOnlyBlocks.emplace_back(currentBlock);
                visitedBlocks.emplace(currentBlock->getIdentifier()); // put every visited br only block in visitedBlocks
                branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(currentBlock->getTerminatorOp());
                currentBlock = branchOp->getNextBlockInvocation().getBlock();
            }
            // currentBlock is now NOT a br-only-block
            updatePredecessorBlocks(brOnlyBlocks, currentBlock);
            if(!visitedBlocks.contains(currentBlock->getIdentifier())) {
                candidates.emplace_back(currentBlock); //todo is this too early?
            }
        } else {
            if(!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
                candidates.emplace_back(branchOp->getNextBlockInvocation().getBlock());
            }
        }
    } else if (terminatorOp->getOperationType() == Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
        if(!visitedBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace_back(ifOp->getFalseBlockInvocation().getBlock());
        }
        if(!visitedBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace_back(ifOp->getTrueBlockInvocation().getBlock());
        }
    }
}

// Todo: function is exactly the same as predecessor main loop function
// -> use an abstraction for it?
void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::processBrOnlyBlocks(IR::BasicBlockPtr currentBlock) {
    std::vector<IR::BasicBlockPtr> candidates;
    std::unordered_set<std::string> visitedBlocks;
    removeBrOnlyBlocks(currentBlock, candidates, visitedBlocks);

    while(!candidates.empty()) {
        visitedBlocks.emplace(currentBlock->getIdentifier());
        currentBlock = candidates.back();
        candidates.pop_back();
        removeBrOnlyBlocks(currentBlock, candidates, visitedBlocks);
    }
}

void inline checkBranchForLoopHeadBlocks(IR::BasicBlockPtr currentBlock, std::stack<IR::BasicBlockPtr>& candidates, 
                        std::unordered_set<std::string>& visitedBlocks, std::unordered_set<std::string>& loopHeadCandidates) {
    while(!visitedBlocks.contains(currentBlock->getIdentifier()) && 
            currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp) {
        auto terminatorOp = currentBlock->getTerminatorOp();
        if(terminatorOp->getOperationType() == Operation::BranchOp) {
            auto nextBlock = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp)->getNextBlockInvocation().getBlock();
            if(!nextBlock->isIfBlock() && loopHeadCandidates.contains(nextBlock->getIdentifier())) {
                nextBlock->setBlockType(IR::BasicBlock::LoopBlock);
            }
            visitedBlocks.emplace(currentBlock->getIdentifier());
            currentBlock = nextBlock;
        } else if (terminatorOp->getOperationType() == Operation::IfOp) {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
            loopHeadCandidates.emplace(currentBlock->getIdentifier());
            candidates.emplace(currentBlock);
            visitedBlocks.emplace(currentBlock->getIdentifier());
            currentBlock = ifOp->getTrueBlockInvocation().getBlock();
        }
    }
}

//todo, as soon as we walk down the false path of a loopCandidate, the candidate itself, and all candidates that we 
//      found in the if path are not valid loopCandidates anymore
void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::findLoopHeadBlocks(IR::BasicBlockPtr currentBlock) {
    std::stack<IR::BasicBlockPtr> ifBlocks; //todo use stack
    std::unordered_set<std::string> loopHeadCandidates;
    std::unordered_set<std::string> visitedBlocks;

    // remember candidate blocks with branchID, if on another branchID, block is not a candidate anymore
    // ALTERNATIVE: if we jump back to a loopCandidate it CANNOT be a loop block anymore
    //  -> we already followed its true-branch, otherwise we would not be jumping in its false-branch
    //  -> if it was a loop-header, we would have encountered it again following its true-branch
    //  -> since we did not encounter it again following its true-branch it cannot be a loop-header
    do {
        checkBranchForLoopHeadBlocks(currentBlock, ifBlocks, visitedBlocks, loopHeadCandidates);
        // an ifBlock that we pop from the ifBlock list CANNOT be a loop-header block anymore
        // since we traverse loopCandidates in reverse order, and since we can only encounter it coming from 'above, 
        // we can only re-encounter a loopCandidate that is not a loop-header if we already followed its else-branch
        if(ifBlocks.top()->getBlockType() == IR::BasicBlock::BlockType::None) { 
            ifBlocks.top()->setBlockType(IR::BasicBlock::IfBlock); //todo could avoid below by changing iteration in 'checkBranchForLoopHeadBlocks'
        }
        currentBlock = std::static_pointer_cast<IR::Operations::IfOperation>(ifBlocks.top()->getTerminatorOp())->getFalseBlockInvocation().getBlock();
        ifBlocks.pop();
    } while(!ifBlocks.empty());
}

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::findControlFlowMerge(IR::BasicBlockPtr& currentBlock, 
                                std::stack<std::unique_ptr<IfOpCandidate>>& ifOperations, 
                                const std::unordered_map<std::string, uint32_t>& candidateEdgeCounter) {
    uint32_t openEdges = currentBlock->getPredecessors().size() - 
            ((candidateEdgeCounter.contains(currentBlock->getIdentifier())) ? 
              candidateEdgeCounter.at(currentBlock->getIdentifier()) - currentBlock->isLoopHeadBlock() : 0);
    bool openMergeBlockFound = (currentBlock->getPredecessors().size() > 1) && (openEdges > 0);
    
    while(!openMergeBlockFound) {
        auto terminatorOp = currentBlock->getTerminatorOp();
        if(terminatorOp->getOperationType() == Operation::BranchOp) {
            auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
            currentBlock = branchOp->getNextBlockInvocation().getBlock();
            // check for loops:
            // if current if-candidate is the same as the one that we are currently searching a merge block for, we found a loop
            // -> because all control flow within a loop must be closed (merge-block or loop-return) before the loop returns
            //    the if-operation that we search for when we loop back to the loop-header, must be the loop-header itself!
            // could maybe use operation pointer comparison?
            // Compare shared pointers (compares underlying pointers). If not possible compare 'toString()'
            if(!ifOperations.empty() && (ifOperations.top()->ifOp->getValue() == currentBlock->getTerminatorOp())) {
                // branch leads to loop -> remove loop-header from ifOperations && follow its false path.
                currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getBlock();
                ifOperations.pop();
            }
        } else if (terminatorOp->getOperationType() == Operation::IfOp) {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
            ifOperations.emplace(std::make_unique<IfOpCandidate>(IfOpCandidate{ifOp, true}));
            currentBlock = ifOp->getTrueBlockInvocation().getBlock();
        }
        uint32_t openEdges = currentBlock->getPredecessors().size() - 
            ((candidateEdgeCounter.contains(currentBlock->getIdentifier())) ? 
              currentBlock->getPredecessors().size() - candidateEdgeCounter.at(currentBlock->getIdentifier()) - currentBlock->isLoopHeadBlock() : 
              currentBlock->isLoopHeadBlock());
        openMergeBlockFound = (currentBlock->getPredecessors().size() > 1) && (openEdges > 1); //todo change: currently we increment edgeCounter too late
    }
}

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::createIfOperations(IR::BasicBlockPtr currentBlock) {
    std::stack<std::unique_ptr<IfOpCandidate>> ifOperations;
    std::stack<IR::BasicBlockPtr> mergeBlocks;
    std::unordered_map<std::string, uint32_t> candidateEdgeCounter;
    
    while(currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp) {
        findControlFlowMerge(currentBlock, ifOperations, candidateEdgeCounter);
        auto currentIfCandidate = std::move(ifOperations.top());
        ifOperations.pop();

        if(currentIfCandidate->isTrueBranch) {//loop blocks can only occur in their true-branches
            //todo handle loop returns or already do so above
            if(candidateEdgeCounter.contains(currentBlock->getIdentifier())) {
                candidateEdgeCounter[currentBlock->getIdentifier()] = candidateEdgeCounter[currentBlock->getIdentifier()] + 1;
            } else {
                candidateEdgeCounter.emplace(std::pair{currentBlock->getIdentifier(), 1});
            }
            currentIfCandidate->isTrueBranch = false; 
            mergeBlocks.emplace(currentBlock);
            currentBlock = currentIfCandidate->ifOp->getFalseBlockInvocation().getBlock();
            ifOperations.emplace(std::move(currentIfCandidate));
        } else {
            assert(mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier());
            candidateEdgeCounter[currentBlock->getIdentifier()] = candidateEdgeCounter[currentBlock->getIdentifier()] + 1;
            mergeBlocks.pop();
            while(!mergeBlocks.empty() && mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier()) {
                mergeBlocks.pop();
                ifOperations.pop();
            }
            uint32_t openEdges = currentBlock->getPredecessors().size() - 
                    ((candidateEdgeCounter.contains(currentBlock->getIdentifier())) ? 
                    candidateEdgeCounter.at(currentBlock->getIdentifier()) - currentBlock->isLoopHeadBlock() : 0);
            bool openMergeBlockFound = (currentBlock->getPredecessors().size() > 1) && (openEdges > 0);
            if(openMergeBlockFound) {
                mergeBlocks.emplace(currentBlock);
                ifOperations.top()->isTrueBranch = false;
                currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getBlock();
            }
        }
    }
}


// IF if-block in loop-head
// -> do not add to if-stack, but keep for exploring after-loop-block

// Possible algorithm:
// High level idea:
// - iterate branch aware:
//  - we always need to know whether we are in the true- or false-branch for all active/opened if-blocks
//  - when we hit a merge block, we 'remember' it for all active true- or false-branches
//  - when we hit a merge-block with an active false-branch of an IfOperation, 
//    and we have hit that merge-block with the active true-branch of that same IfOperation, we have found its merge-block
//  -> could happen for multiple IfOperations for the same merge-block
// Implementation:
// - iterate over graph until we hit if-block
// -> add if-block-true-case to stack/list
// -> follow true-branch
// -> store if-block-false-case
//  -> follow false-branch when true-branch has been explored fully
// - when we hit a merge block:
//  -> mark all active true-cases of open IfOperations with the identity of that merge-block
//  -> for all false-cases of open IfOperations, check if merge-block was marked before by ifOperation
//      -> if so, add merge-block to IfOperation


// void inline applyScopeLevels(IR::BasicBlockPtr currentBlock, std::vector<IR::BasicBlockPtr>& candidates) {
//     // while(!visitedBlocks.contains(currentBlock->getIdentifier()) && 
//     //         currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp) {

//     auto terminatorOp = currentBlock->getTerminatorOp();

//     if(terminatorOp->getOperationType() == Operation::BranchOp) {
//         // visitedBlocks.emplace(currentBlock->getIdentifier());
//         auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
//         auto nextBlock = branchOp->getNextBlockInvocation().getBlock();
//         // todo merge block check as function
//         // uint32_t isMergeBlock = 1 - ((ifOp->getFalseBlockInvocation().getBlock()->getPredecessors().size() > 1) && 
//         //                                  !ifOp->getFalseBlockInvocation().getBlock()->getIsLoopHeadBlock());
//         // Problem: Loop Header Case: How to detect whether branch comes from within loop or from initiating block
//         // -> check if loopHeader has been visited already (UINT32_T_MAX)
//         // -> can a loop header be accessed by two different blocks that are not within the loop?
//         // -> if-else case merge block that is a loop header -> but then the loop header is an actual merge block

//         // When we br to a loop-header-block, when do we lower, and when do we keep the scope level?
//         // -> Idea: when we visited it before, we lower the scope level, because we branch from within the loop
//         // -> PROBLEM: it is possible to branch to a loop twice from outside (if- and else cases to a while(1) loop)
//         // -> SOLUTION: We decrease scope level by one if we have visited the loop before
//         //              -> either, we come from within the loop (easy)
//         //              -> or, we branch to a merge-block, which is a loop header at the same time (also requires decrease)
//         // -> PROBLEM_2: we branch from an if-branch to a loop-head-block that is a merge block
//         //      -> How to differentiate:
//         //          -> a) it is a loop head block encapsuled within the if-block (keep scope level)
//         //              -> SOLUTION: It has 2 predecessors (the branching block, and the loop-end-block)
//         //                          -> CF CANNOT come from another branch, before the merge-block of the current if-operation is closed
//         //          -> b) it is the merge block of the if-block (decrease scope level)
//         //              -> SOLUTION: It has > 2 predecessors
//         // Algorithm:
//         //  (visited?)
//         //      true(loop-end-block or if-merge transition): scope level -= 1
//         //      false: (predecessors > 2):
//         //          true(merge-block): scope level -= 1
//         //          false(nested-loop-header): keep scope level
        
//         if(nextBlock->getIsLoopHeadBlock()) {
//             if(nextBlock->getScopeLevel() != UINT32_MAX) { //visited
//                 uint32_t isMergeBlock = nextBlock->getPredecessors().size() > 2; //not sufficient need to check if back-link or simple branch
//                 // If we backlink to a loop header, we never set a lower scope level, and never find a new candidate
//                 // The only relevant cases are: 
//                 //  a) br on the same level (!isMergeBlock -> same level transfer)
//                 //  b) br to merge-block (isMergeBlock -> level - 1 transfer)
//                 if((nextBlock->getScopeLevel() > (currentBlock->getScopeLevel() - isMergeBlock))) {
//                     nextBlock->setScopeLevel(currentBlock->getScopeLevel() - isMergeBlock);
//                     candidates.emplace_back(nextBlock);
//                 }
//             } else {
//                 if(nextBlock->getPredecessors().size() > 2) { // not visited && merge-block && CANNOT be a nested-loop
//                     if((nextBlock->getScopeLevel() > (currentBlock->getScopeLevel() - 1))) {
//                         nextBlock->setScopeLevel(currentBlock->getScopeLevel() - 1);
//                         candidates.emplace_back(nextBlock);
//                     }
//                     //todo PROBLEM: what if predecessors.size() == 2 -> merge-block -> decrease scope level!
//                 } else { // not visited && nested-loop-header
//                     if((nextBlock->getScopeLevel() > (currentBlock->getScopeLevel()))) {
//                         nextBlock->setScopeLevel(currentBlock->getScopeLevel());
//                         candidates.emplace_back(nextBlock);
//                     }
//                 }
//             }
//         } else {
//             uint32_t isMergeBlock = nextBlock->getPredecessors().size() > 1;
//             if((nextBlock->getScopeLevel() > (currentBlock->getScopeLevel()) - isMergeBlock)) { //not a loop head block!
//                 nextBlock->setScopeLevel(currentBlock->getScopeLevel() - isMergeBlock);
//                 candidates.emplace_back(nextBlock);
//             }
//         }

//         // if((nextBlock->getScopeLevel() > (currentBlock->getScopeLevel() - isMergeBlock))) {
//         //     branchOp->getNextBlockInvocation().getBlock()->setScopeLevel(currentBlock->getScopeLevel());
//         //     candidates.emplace_back(branchOp->getNextBlockInvocation().getBlock());
//         // } //else, block has already been visited and scopeLevel as already smaller
//     } else if (terminatorOp->getOperationType() == Operation::IfOp) {
//         auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
//         // todo check if next block is already merge block, if so do not lower scope level
//         // -> idea: if next block of if-block-branch is merge block already
//         // -> BUT: we do that check already, but not for loopHead blocks
//         // -> SOLUTION: loop body blocks MUST ALWAYS be one scope level lower!
//         if(currentBlock->getIsLoopHeadBlock()) {
//             ifOp->getFalseBlockInvocation().getBlock()->setScopeLevel(currentBlock->getScopeLevel());
//             ifOp->getTrueBlockInvocation().getBlock()->setScopeLevel(currentBlock->getScopeLevel() + 1);
//             candidates.emplace_back(ifOp->getFalseBlockInvocation().getBlock());
//             candidates.emplace_back(ifOp->getTrueBlockInvocation().getBlock());
//         } else {
//             // Set False Block Scope
//             if((ifOp->getFalseBlockInvocation().getBlock()->getPredecessors().size() > 1) && 
//                     !(ifOp->getFalseBlockInvocation().getBlock()->getPredecessors().size() == 2 && 
//                     ifOp->getFalseBlockInvocation().getBlock()->getIsLoopHeadBlock())) {
//                 if(ifOp->getFalseBlockInvocation().getBlock()->getScopeLevel() > (currentBlock->getScopeLevel())) {
//                     ifOp->getFalseBlockInvocation().getBlock()->setScopeLevel(currentBlock->getScopeLevel());
//                     candidates.emplace_back(ifOp->getFalseBlockInvocation().getBlock());
//                 }
//             } else { // simple block or nested loop-header
//                 if(ifOp->getFalseBlockInvocation().getBlock()->getScopeLevel() > (currentBlock->getScopeLevel() + 1)) {
//                     ifOp->getFalseBlockInvocation().getBlock()->setScopeLevel(currentBlock->getScopeLevel() + 1);
//                     candidates.emplace_back(ifOp->getFalseBlockInvocation().getBlock());
//                 }
//             }
//             if((ifOp->getTrueBlockInvocation().getBlock()->getPredecessors().size() > 1) && 
//                     !(ifOp->getTrueBlockInvocation().getBlock()->getPredecessors().size() == 2 && 
//                     ifOp->getTrueBlockInvocation().getBlock()->getIsLoopHeadBlock())) {
//                 if(ifOp->getTrueBlockInvocation().getBlock()->getScopeLevel() > (currentBlock->getScopeLevel())) {
//                     ifOp->getTrueBlockInvocation().getBlock()->setScopeLevel(currentBlock->getScopeLevel());
//                     candidates.emplace_back(ifOp->getTrueBlockInvocation().getBlock());
//                 }
//             } else { // simple block or nested loop-header
//                 if(ifOp->getTrueBlockInvocation().getBlock()->getScopeLevel() > (currentBlock->getScopeLevel() + 1)) {
//                     ifOp->getTrueBlockInvocation().getBlock()->setScopeLevel(currentBlock->getScopeLevel() + 1);
//                     candidates.emplace_back(ifOp->getTrueBlockInvocation().getBlock());
//                 }
//             }
//         }
//     }
// }

// void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::addScopeLevels(IR::BasicBlockPtr currentBlock) {
//     std::vector<IR::BasicBlockPtr> ifBlocks;
//     currentBlock->setScopeLevel(0);
     
//     applyScopeLevels(currentBlock, ifBlocks);
//     while(!ifBlocks.empty()) {
//         visitedBlocks.emplace(currentBlock->getIdentifier());
//         currentBlock = ifBlocks.back();
//         ifBlocks.pop_back();
//         applyScopeLevels(currentBlock, ifBlocks);
//     };
// }


}//namespace NES::Nautilus::Tracing



// Assumption A1: a nested loop is always returned to before an enclosing loop
// Assumption A2: it must be possible to reach a loop-head, starting from the loop-head and only taking true-branches
// -> thus, if we hit return, all if-blocks that we already hit are ordinary if-blocks
// Iterate through IRGraph
// - go down one branch
// - when hitting an if-block -> add it to hash table
// - also go down the true-branch of that if-block
// - also^2 add that if-block to candidate list
// - whenever a branch operation is parsed, check if target is in hash table already
// - if a block was already visited, mark that block/IfOperation as a loopHead
//  -> all if-blocks that were encountered between this loop-end-block and the loop-head-block are regular if-operations
//  -> also mark them (TODO can probably skip that! -> hitting return guarantees ifOps)
//  -> go down until return is encountered
//  -> go through else-branches of candidates to check for potential loops
// ------------------------------------------------------------------------------------

//  Ideas:
//  - assign scope levels starting with 0
//  - hitting an IfOperation CAN lead to a scope level increase
//      - an IfOperation with a defined true AND else case leads to an increase in both branches
//      - an IfOperation without a true case only leads to an increase in the false case
//      - an IfOperation without a false case only leads to an increase in the true case
//      1. if one of the branches of an IfOperation is not defined
//      -> the undefined branch keeps the current scope level
//      2. if the IfOperation marks a loop (WHICH OPERATION MARKS A LOOP?)
//        -> the scope level of the 'false' branch is not lowered
//  - CHALLENGE:
//      - how to find out when to increase scope level after IfOperation and when not to?
//      1.) if true/false branch directly lead to 'merge-block'(has several predecessors) then no increase
//      2.) BUT WHAT IF ITS A LOOP HEAD BLOCK?
//      -> true-case-block must be loop body block that at some point passes CF to block with br back to LOOP HEAD
//      -> QUESTION: Can the false-case-block contain a merge block before an If Operation appears?
//          -> yes: if(x) {loop{x+1}; x+5;} -> merge block would be after loop
//      -> problem: if-only is solved, BUT:
//          - WE CANNOT simply increase the scope level when taking the true/false branch, because it might be a loop
//      -> Naive solution: 
//          - always go down true-cases first, and check whether IfOperation is closed by merge block OR by loop-end-block
//          -> PROBLEM: if we cannot assign scope level yet -> how to find out when IfOperation is closed by merge block?
//              -> SOLUTION:
//                  - go down branch by branch (depth-first && true-case first)
//                  - when hitting an IfOperation -> add to ifOpStack && add false-case-branch to candidates
//                  - when hitting a merge-block -> pop last ifOperation && mark that if-operation with scope level
//                  - keep following until return is met
//                  - then follow false-case-branch of most recent if operation