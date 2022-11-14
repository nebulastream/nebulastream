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
    // findLoopHeadBlocks(rootOperation->getFunctionBasicBlock()); // todo -> found error case
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

//==------------------------------------------------------------------==//
//==---------------------- Scope Level Pass --------------------------==//
//==------------------------------------------------------------------==//


void inline checkBranchForLoopHeadBlocks(IR::BasicBlockPtr currentBlock, std::vector<IR::BasicBlockPtr>& candidates, 
                        std::unordered_set<std::string>& visitedBlocks, std::unordered_set<std::string>& loopHeadCandidates) {
    while(!visitedBlocks.contains(currentBlock->getIdentifier()) && 
            currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp) {
        auto terminatorOp = currentBlock->getTerminatorOp();
        if(terminatorOp->getOperationType() == Operation::BranchOp) {
            auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
            if(loopHeadCandidates.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
                branchOp->getNextBlockInvocation().getBlock()->setIsLoopHeadBlock(true);
            }
            visitedBlocks.emplace(currentBlock->getIdentifier());
            currentBlock = branchOp->getNextBlockInvocation().getBlock();
        } else if (terminatorOp->getOperationType() == Operation::IfOp) {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
            loopHeadCandidates.emplace(currentBlock->getIdentifier());
            candidates.emplace_back(ifOp->getFalseBlockInvocation().getBlock());
            visitedBlocks.emplace(currentBlock->getIdentifier());
            currentBlock = ifOp->getTrueBlockInvocation().getBlock();
        }
    }
}

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::findLoopHeadBlocks(IR::BasicBlockPtr currentBlock) {
    std::vector<IR::BasicBlockPtr> ifBlocks;
    std::unordered_set<std::string> loopHeadCandidates;
    std::unordered_set<std::string> visitedBlocks;

    do {
        checkBranchForLoopHeadBlocks(currentBlock, ifBlocks, visitedBlocks, loopHeadCandidates);
        currentBlock = ifBlocks.back();
        ifBlocks.pop_back();
    } while(!ifBlocks.empty());
}

std::vector<std::shared_ptr<IfOperation>> 
inline findMergeBlock(IR::BasicBlockPtr currentBlock, std::stack<IR::BasicBlockPtr>& trueBranchCandidates,
                        std::stack<IR::BasicBlockPtr>& falseBranchCandidates, bool isTrueBranch,
                        std::unordered_map<std::string, uint32_t>& candidateEdgeCounter) {
    // todo
    // find if operations -> add to tasks -> (Need tasks in class :()) -> could also return stack with new tasks and manage adding them in parent function
    // -> additionally, take 'isTrueBranch' as parameter -> used to add mergeBlocks to candidates
    // DFS until merge-block found
    // - then behavior depends on task type (isTrueBranch)
    //      - true:
    //          1. add merge-block to trueBranchCandidates
    //          2. -> return? -> then manage getting new task in parent function?
    //      - false:
    //          1. add merge-block to falseBranchCandidates
    //          2. -> return
    
    std::vector<std::shared_ptr<IfOperation>> candidates;
    uint32_t openEdges = currentBlock->getPredecessors().size() - ((candidateEdgeCounter.contains(currentBlock->getIdentifier())) ? 
                                                                    candidateEdgeCounter[currentBlock->getIdentifier()] : 0);
    bool openMergeBlockFound = ((currentBlock->getPredecessors().size() > 1 && !currentBlock->getIsLoopHeadBlock()) 
                            || currentBlock->getPredecessors().size() > 3) && (openEdges > 0);
    // change to do while
    while(!openMergeBlockFound) {
        auto terminatorOp = currentBlock->getTerminatorOp();
        if(terminatorOp->getOperationType() == Operation::BranchOp) {
            auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
            currentBlock = branchOp->getNextBlockInvocation().getBlock();
            
        } else if (terminatorOp->getOperationType() == Operation::IfOp) {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
            candidates.emplace_back(ifOp);
            currentBlock = ifOp->getTrueBlockInvocation().getBlock();
        }
        openEdges = currentBlock->getPredecessors().size() - ((candidateEdgeCounter.contains(currentBlock->getIdentifier())) ? 
                                                                candidateEdgeCounter[currentBlock->getIdentifier()] : 0);
        openMergeBlockFound = ((currentBlock->getPredecessors().size() > 1 && !currentBlock->getIsLoopHeadBlock()) 
                            || currentBlock->getPredecessors().size() > 3) && (openEdges > 0);
    }
    if(isTrueBranch || !candidates.empty()) {
        trueBranchCandidates.emplace(currentBlock);
    } else {
        falseBranchCandidates.emplace(currentBlock);
    }
    return candidates;
}

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::createIfOperations(IR::BasicBlockPtr currentBlock) {
    // std::vector<IR::BasicBlockPtr> ifBlocks;
    struct Task {
        std::shared_ptr<IfOperation> ifOp;
        bool trueBranch; // rename to isTrueBranch or isTrueBranchTask?
    };
    std::stack<std::unique_ptr<Task>> tasks;
    std::stack<IR::BasicBlockPtr> trueBranchCandidates; //candidates -> mergeBlock?
    std::stack<IR::BasicBlockPtr> falseBranchCandidates;
    std::unordered_map<std::string, uint32_t> candidateEdgeCounter;
    // IR::BasicBlockPtr currentBlock;
    
     

    // Work on blocks in 'processTasks'?
    // -> manage popping tasks here
    bool followBranch = false;
    //could use returnBlock as stop condition (currentBlock.getTerminatorOperation.getOperationType != Operation::ReturnOp)
    while(currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp) {
        if(tasks.empty()) {
            auto newCandidates = findMergeBlock(currentBlock, trueBranchCandidates, falseBranchCandidates, true, candidateEdgeCounter);
            for(auto& candidate : newCandidates) {
                tasks.emplace(std::make_unique<Task>(Task{candidate, true})); // is correct order!
            }
        }
        auto currentTask = std::move(tasks.top());
        tasks.pop();

        // Loop Return Check:
        //todo support loops!

        // todo  update currentBlock when taking new branch
        if(currentTask->trueBranch) {
            currentTask->trueBranch = false;
            // tasks.emplace(std::move(currentTask));
            // mark merge block as visited by one edge
            auto candidate = trueBranchCandidates.top();
            if(candidateEdgeCounter.contains(candidate->getIdentifier())) {
                candidateEdgeCounter[candidate->getIdentifier()] = candidateEdgeCounter[candidate->getIdentifier()] + 1;
            } else {
                candidateEdgeCounter.emplace(std::pair{candidate->getIdentifier(), 1});
            }
        } else {
            // we went down else case of a block, we did NOT find a new candidate, but we found a merge block
            // -> MUST be match for if-candidate
            assert(trueBranchCandidates.top()->getIdentifier() == falseBranchCandidates.top()->getIdentifier());
            // tasks.pop(); -> already popped before -> reinsert if necessary
            auto candidate = falseBranchCandidates.top();
            // must have been visited before
            candidateEdgeCounter[candidate->getIdentifier()] = candidateEdgeCounter[candidate->getIdentifier()] + 1; 

            // Prepare parent task:
            if(tasks.empty()) {
                currentBlock = trueBranchCandidates.top();
                trueBranchCandidates.pop();
                falseBranchCandidates.pop();
                //update current block
                continue;
            }
            currentTask = std::move(tasks.top());
            tasks.pop();
            if(currentTask->trueBranch) {
                falseBranchCandidates.pop(); //candidate of previous task that match was found for
                uint32_t numOpenPredecessors = trueBranchCandidates.top()->getPredecessors().size() - candidateEdgeCounter[trueBranchCandidates.top()->getIdentifier()];
                if(numOpenPredecessors > 0) {
                    currentTask->trueBranch = false;
                    // continue;
                } else {
                    // current trueBranch candidate has no open predecessors
                    // -> we want to continue digging down
                    // Pop old candidate that we found a match for and simply continue with parent task
                    // trueBranchCandidates.pop(); 
                    currentBlock = trueBranchCandidates.top();
                    trueBranchCandidates.pop(); // merge block fully met -> drop it
                    followBranch = true;
                    // todo is trueBranchCandidate correct?
                }
            } else { // false branch
                trueBranchCandidates.pop();
                uint32_t numOpenPredecessors = falseBranchCandidates.top()->getPredecessors().size() - candidateEdgeCounter[falseBranchCandidates.top()->getIdentifier()];
                if(numOpenPredecessors > 0) {
                    //todo can this happen -> what to do then?
                    assert(trueBranchCandidates.top()->getIdentifier() == falseBranchCandidates.top()->getIdentifier());
                    // continue;
                } else {
                    if(trueBranchCandidates.top()->getIdentifier() == falseBranchCandidates.top()->getIdentifier()) {
                        NES_DEBUG("Found if - merge");
                        currentBlock = falseBranchCandidates.top();
                        trueBranchCandidates.pop();
                        falseBranchCandidates.pop();
                        continue;
                        // auto mergeBlockTerminator = trueBranchCandidates.top()->getTerminatorOp();
                        // if(mergeBlockTerminator->getOperationType() == Operation::ReturnOp) {
                        //     break;
                        // } else {
                        //     if(mergeBlockTerminator->getOperationType() == )
                        // }
                    } else {
                        currentBlock = falseBranchCandidates.top();
                        followBranch = true;
                    }
                }
            }
    //         if(!followBranch) {
    //             currentBlock = (currentTask->trueBranch) ?  currentTask->ifOp->getTrueBlockInvocation().getBlock() : 
    //                                                         currentTask->ifOp->getFalseBlockInvocation().getBlock();
    //         }
    //         auto newCandidates = findMergeBlock(currentBlock, trueBranchCandidates, falseBranchCandidates, currentTask->trueBranch, candidateEdgeCounter);
    //         //todo check if currentBlock is LoopHeader
    //         // merge block found -> how to proceed?
    //         tasks.emplace(currentTask);
    //         for(auto& candidate : newCandidates) {
    //             tasks.emplace(std::make_unique<Task>(Task{candidate, true})); // is correct order!
    //         }
        }
        if(!followBranch) {
            currentBlock = (currentTask->trueBranch) ?  currentTask->ifOp->getTrueBlockInvocation().getBlock() : 
                                                        currentTask->ifOp->getFalseBlockInvocation().getBlock();
        }
        followBranch = false;
        auto newCandidates = findMergeBlock(currentBlock, trueBranchCandidates, falseBranchCandidates, currentTask->trueBranch, candidateEdgeCounter);
        //todo check if currentBlock is LoopHeader
        // merge block found -> how to proceed?
        // tasks.emplace(currentTask);
        tasks.emplace(std::move(currentTask));
        for(auto& candidate : newCandidates) {
            tasks.emplace(std::make_unique<Task>(Task{candidate, true})); // is correct order!
        }
        // add new tasks!
        // check if current task is true or false type
        // true:
        //  1. flip task type
        //  2. get merge-block(candidate) from trueBranchCandidates
        //  3. mark merge-block as visited by one edge
        // -> continue with flipped task (false-branch) of current ifOp
        // false(matching case):
        //  1. check for match (must be true -> throw error if not)
        //  2. increase edgeCounter of falseBranchCandidates.top()
        //  3. pop task from tasks
        //  4. get parentTask
        //      3.1 check if parentTask is trueBranch
        //      trueBranch:
        //          - pop candidate from falseBranchCandidates
        //          - check if current trueBranchCandidate has open predecessors
        //          hasOpenPredecessors:
        //              - keep as candidate
        //              - flip task type
        //              - new iteration
        //          !hasOpenPredecessor:
        //              - pop candidate from trueBranchCandidates
        //              -  call findMergeBlock with unchanged task and next block as entry
        //      falseBranch:
        //          - pop candidate from trueBranchCandidates
        //          - check if current falseBranchCandidate has open predecessors
        //              hasOpenPredecessors:
        //                  - check if it matches if-candidate -> if not error!
        //                      MATCH FOUND:
        //                          - return to 1.
        //              !hasOpenPredecessors
        //                  - pop candidate from falseBranchCandidates
        //                  - call findMergeBlock with unchanged task, and next block as entry
    // }
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