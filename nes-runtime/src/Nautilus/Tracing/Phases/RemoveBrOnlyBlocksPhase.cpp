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

#include "Nautilus/IR/Operations/Loop/LoopOperation.hpp"
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
        currentBlock = candidates.back();
        candidates.pop_back();
        addPredecessorToBlock(currentBlock, candidates, visitedBlocks);
    }
}

void updatePredecessorBlocks(std::vector<IR::BasicBlockPtr> &brOnlyBlocks, IR::BasicBlockPtr newBlock) {
    // Iterate over all passed br-only-blocks and set non-br-only-block as new target block of predecessors.
    std::unordered_map<std::string, IR::BasicBlockPtr> removedBlocks; //rename
    std::vector<std::weak_ptr<IR::BasicBlock>> newBlocks; //rename
    for(auto brOnlyBlock : brOnlyBlocks) {
        removedBlocks.emplace(std::pair{brOnlyBlock->getIdentifier(), brOnlyBlock});
        for(auto predecessor : brOnlyBlock->getPredecessors()) {
            if(!removedBlocks.contains(predecessor.lock()->getIdentifier()))  {
                newBlocks.emplace_back(predecessor);
            }
            auto terminatorOp = predecessor.lock()->getTerminatorOp();
            if(terminatorOp->getOperationType() == Operation::IfOp) {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                if(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier() == brOnlyBlock->getIdentifier()) {
                    ifOp->getTrueBlockInvocation().setBlock(newBlock);
                } else {
                    ifOp->getFalseBlockInvocation().setBlock(newBlock);
                    // Check if control flow along the true- and false-branch now lead to the same block.
                    if(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier() == newBlock->getIdentifier()) {
                        newBlocks.pop_back();
                        predecessor.lock()->removeOperation(predecessor.lock()->getTerminatorOp());
                        auto newBranchOperation = std::make_shared<BranchOperation>();
                        newBranchOperation->getNextBlockInvocation().setBlock(newBlock);
                        for(auto arg : ifOp->getFalseBlockInvocation().getArguments()) {
                            newBranchOperation->getNextBlockInvocation().addArgument(arg);
                        }
                        NES_DEBUG(newBranchOperation->toString());
                        predecessor.lock()->addOperation(std::move(newBranchOperation));
                    }
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
    for(auto predecessor : newBlock->getPredecessors()) {
        if(!removedBlocks.contains(predecessor.lock()->getIdentifier())) {
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
            while(currentBlock->getOperations().size() == 1 && currentBlock->getTerminatorOp()->getOperationType() == Operation::BranchOp) {
                brOnlyBlocks.emplace_back(currentBlock);
                visitedBlocks.emplace(currentBlock->getIdentifier()); // put every visited br only block in visitedBlocks
                branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(currentBlock->getTerminatorOp());
                currentBlock = branchOp->getNextBlockInvocation().getBlock();
            }
            // currentBlock is now NOT a br-only-block
            updatePredecessorBlocks(brOnlyBlocks, currentBlock);
            if(!visitedBlocks.contains(currentBlock->getIdentifier())) {
                candidates.emplace_back(currentBlock);
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

//todo improve structure of function.
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

void inline checkBranchForLoopHeadBlocks(IR::BasicBlockPtr& currentBlock, std::stack<IR::BasicBlockPtr>& candidates, 
                        std::unordered_set<std::string>& visitedBlocks, std::unordered_set<std::string>& loopHeadCandidates) {
    while(!visitedBlocks.contains(currentBlock->getIdentifier()) && 
            currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp) {
        auto terminatorOp = currentBlock->getTerminatorOp();
        if(terminatorOp->getOperationType() == Operation::BranchOp) {
            auto nextBlock = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp)->getNextBlockInvocation().getBlock();
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
    if(loopHeadCandidates.contains(currentBlock->getIdentifier())) {
        //todo replace ifOperation with loopOperation
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
        if(currentBlock->getTerminatorOp()->getOperationType() == Operation::IfOp) {
            // If loop header has two args -> while, 
            // auto loopOp = std::make_shared<LoopOperation>();
        }
        currentBlock->incrementBackLinks();
    }
}

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::findLoopHeadBlocks(IR::BasicBlockPtr currentBlock) {
    std::stack<IR::BasicBlockPtr> ifBlocks;
    std::unordered_set<std::string> loopHeadCandidates;
    std::unordered_set<std::string> visitedBlocks;

    bool returnBlockVisited = false;
    bool noMoreIfBlocks = true;
    do {
        checkBranchForLoopHeadBlocks(currentBlock, ifBlocks, visitedBlocks, loopHeadCandidates);
        noMoreIfBlocks = ifBlocks.empty();
        // todo: shouldn't currentBlock here always be the return block?
        // -> NO! -> we also return when we find a loop!
        // -> BUT, at some point WE MUST return with the return block
        // -> and the return block CANNOT be a loop-header-block!
        // -> we could simply flip the condition here then.
        returnBlockVisited = returnBlockVisited || (currentBlock->getTerminatorOp()->getOperationType() == Operation::ReturnOp);
        if(!noMoreIfBlocks) {
            // When we take the false-branch of a ifOperation, we completely exhausted its true-branch CF.
            // Since loops can only loop back on their true-branch, we can safely stop tracking it as a loop candidate.
            loopHeadCandidates.erase(ifBlocks.top()->getIdentifier());
            currentBlock = std::static_pointer_cast<IR::Operations::IfOperation>(ifBlocks.top()->getTerminatorOp())->getFalseBlockInvocation().getBlock();
            ifBlocks.pop();
        }
    } while(!(noMoreIfBlocks && returnBlockVisited));
}

bool RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::mergeBlockCheck(IR::BasicBlockPtr& currentBlock, 
                                std::stack<std::unique_ptr<IfOpCandidate>>& ifOperations,
                                std::unordered_map<std::string, uint32_t>& mergeBlockNumVisits,
                                const bool newVisit) {
    uint32_t openEdges = 0;
    uint32_t numPriorVisits = 0;
    bool mergeBlockFound = false;
    
    bool isAlreadyVisitedMergeBlock = mergeBlockNumVisits.contains(currentBlock->getIdentifier());
    if(isAlreadyVisitedMergeBlock) {
            numPriorVisits = mergeBlockNumVisits.at(currentBlock->getIdentifier()) - 1;
    }
    if(!currentBlock->isLoopWithVisitedBody()) {
        openEdges = currentBlock->getPredecessors().size() 
                    - (currentBlock->isLoopHeadBlock()) * currentBlock->getBackLinks()
                    - numPriorVisits;
    } else {
        openEdges = currentBlock->getBackLinks() - numPriorVisits;
        if(openEdges < 2) {
                currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getBlock();
                ifOperations.pop();
                // Recursion depth is 1, because false block cannot be an already visited loop header block.
                // return mergeBlockCheck(currentBlock, ifOperations, mergeBlockNumVisits, true);
                isAlreadyVisitedMergeBlock = mergeBlockNumVisits.contains(currentBlock->getIdentifier());
                numPriorVisits = 0;
                if(isAlreadyVisitedMergeBlock) {
                        numPriorVisits = mergeBlockNumVisits.at(currentBlock->getIdentifier()) - 1;
                }
                openEdges = currentBlock->getPredecessors().size() 
                    - (currentBlock->isLoopHeadBlock()) * currentBlock->getBackLinks()
                    - numPriorVisits; //todo is not update!
        }
    }
    // todo Problem: merge-block was visited n/n times (has 4 predecessors, and was visited via 4 unique edges)
    //              -> but there is still an open if-operation that needs to be connected to merge-block
    //      Solution: ONLY case where newVisit is false, is after merge-block was found and we check the 'old' currentBlock
    //                for merge-block qualification, before further traversing the query graph.
    //                So, if newVisit == false -> allow to still take block as merge-block
    //                Potential Problem: Can we have a merge-block, that could be falsely taken as a new merge-block because of the newVisit exception.
    // SOLUTION: if !newVisit && currentBlock == mergeBlocks.top() -> is still merge-block!
    // -> PROBLEM: if we are in true-case branch of ifOp, the mergeBlocks.top() might belong to another if-op
    //  -> SOLUTION_PART_2: check if ifOperations.top().isTrueBranch == false!!
    // ----> !newVisit && !ifOperations.top().isTrueBranch && mergeBlocks.top().getIdentifier() == currentBlock.getIdentifier()
    mergeBlockFound = openEdges > 1;
    if(mergeBlockFound && newVisit && isAlreadyVisitedMergeBlock) {
        mergeBlockNumVisits[currentBlock->getIdentifier()] = mergeBlockNumVisits[currentBlock->getIdentifier()] + 1;
    } else if (mergeBlockFound && newVisit && !isAlreadyVisitedMergeBlock) {
        mergeBlockNumVisits.emplace(std::pair{currentBlock->getIdentifier(), 1});
    }
    return mergeBlockFound;
}


void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::createIfOperations(IR::BasicBlockPtr currentBlock) {
    std::stack<std::unique_ptr<IfOpCandidate>> ifOperations;
    std::stack<IR::BasicBlockPtr> mergeBlocks;
    std::unordered_map<std::string, uint32_t> mergeBlockNumVisits; //rename
    bool mergeBlockFound = true;
    bool newVisit = true;

    // Iterate over graph until all if-operations have been processed and matched with their corresponding merge-blocks.
    while(mergeBlockFound) {
        // Check blocks (DFS) until an open merge-block was found. Push encountered if-operations to stack.
        while(!(mergeBlockFound = mergeBlockCheck(currentBlock, ifOperations, mergeBlockNumVisits, newVisit)) 
                && (currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp)) {
            auto terminatorOp = currentBlock->getTerminatorOp();
            if(terminatorOp->getOperationType() == Operation::BranchOp) {
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                currentBlock = branchOp->getNextBlockInvocation().getBlock();
                newVisit = true;
            } else if (terminatorOp->getOperationType() == Operation::IfOp) {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                ifOperations.emplace(std::make_unique<IfOpCandidate>(IfOpCandidate{ifOp, true}));
                if(currentBlock->isLoopHeadBlock()) {
                    // We processed everything 'above' the loop-header and now dig down its body.
                    currentBlock->setPassed();
                    if(mergeBlockNumVisits.contains(currentBlock->getIdentifier())) {
                        // Erase value from candidate edge counter. If the Loop-Header acts as a merge block for if-ops below,
                        // we start counting down again using its backLinks.
                        mergeBlockNumVisits.erase(currentBlock->getIdentifier()); //requires map to be non-const!
                    }
                }
                currentBlock = ifOp->getTrueBlockInvocation().getBlock();
                newVisit = true;
            }
        }
        // If no merge block was found, we traversed the entire graph and are done (return block is current block).
        if(mergeBlockFound) {
            // Add merge block for current IfOperation's true case, or set merge-block for current if operation.
            if(ifOperations.top()->isTrueBranch) {
                ifOperations.top()->isTrueBranch = false; 
                mergeBlocks.emplace(currentBlock);
                currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getBlock();
                newVisit = true;
            } else {
                // Set currentBlock as merge-block for all if-operations that:
                //  1. Are in their false-branches (their merge-block was pushed to the stack)
                //  2. Have a corresponding merge-block on the stack that matches the currentBlock.
                assert(mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier());
                do {
                    ifOperations.top()->ifOp->setMergeBlock(std::move(mergeBlocks.top()));
                    mergeBlocks.pop();
                    ifOperations.pop();
                } while(!ifOperations.empty() && !ifOperations.top()->isTrueBranch && !mergeBlocks.empty() && mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier());
                newVisit = false;
            }
        }

    }
}
}//namespace NES::Nautilus::Tracing