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
    //todo: when if-branch directly points to loop-header, we exit the loop and do not register the backlink
    // -> does this trigger double increase? Yes!
    // isIfBlock check should prevent double increment
    if(!currentBlock->isIfBlock() && loopHeadCandidates.contains(currentBlock->getIdentifier())) {
        // currentBlock->setBlockType(IR::BasicBlock::LoopBlock);
        currentBlock->incrementBackLinks();
    }
}

//todo, as soon as we walk down the false path of a loopCandidate, the candidate itself, and all candidates that we 
//      found in the if path are not valid loopCandidates anymore
void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::findLoopHeadBlocks(IR::BasicBlockPtr currentBlock) {
    std::stack<IR::BasicBlockPtr> ifBlocks;
    std::unordered_set<std::string> loopHeadCandidates;
    std::unordered_set<std::string> visitedBlocks;

    // remember candidate blocks with branchID, if on another branchID, block is not a candidate anymore
    // ALTERNATIVE: if we jump back to a loopCandidate it CANNOT be a loop block anymore
    //  -> we already followed its true-branch, otherwise we would not be jumping in its false-branch
    //  -> if it was a loop-header, we would have encountered it again following its true-branch
    //  -> since we did not encounter it again following its true-branch it cannot be a loop-header
    // Todo stop condition: return visited AND empty if blocks
    // Todo prohibit double loop backLink increments: mark loop block is visited when taking else-branch
    bool returnBlockVisited = false;
    bool noMoreIfBlocks = true;
    do {
        // todo handle IR without IfOperations!
        checkBranchForLoopHeadBlocks(currentBlock, ifBlocks, visitedBlocks, loopHeadCandidates);
        // an ifBlock that we pop from the ifBlock list CANNOT be a loop-header block anymore
        // -> it means that we already followed the ifBlock's true-branch until the end and did not re-encounter the ifBlock
        noMoreIfBlocks = ifBlocks.empty();
        if(!noMoreIfBlocks) {
            if(ifBlocks.top()->getBlockType() == IR::BasicBlock::BlockType::None) {
                ifBlocks.top()->setBlockType(IR::BasicBlock::IfBlock);
            }
            currentBlock = std::static_pointer_cast<IR::Operations::IfOperation>(ifBlocks.top()->getTerminatorOp())->getFalseBlockInvocation().getBlock();
            ifBlocks.pop();
        }
        returnBlockVisited = returnBlockVisited || (currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp);
    } while(!(noMoreIfBlocks && returnBlockVisited));
}

bool RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::mergeBlockCheck(IR::BasicBlockPtr& currentBlock, 
                                std::stack<std::unique_ptr<IfOpCandidate>>& ifOperations,
                                const std::unordered_map<std::string, uint32_t>& candidateEdgeCounter) {
    // Check if currentBlock is a loop header. If it is, also check whether it has been passed already.
    if(!currentBlock->isLoopHeadBlock()) {
        uint32_t openEdges = currentBlock->getPredecessors().size();
        if(candidateEdgeCounter.contains(currentBlock->getIdentifier())) {
            openEdges -= candidateEdgeCounter.at(currentBlock->getIdentifier()) - 1;
        }
        return openEdges > 1;
    } else {
        // Todo how to handle re-encountering a loop-header (hasBeenPassed==true) that has no more open edges?
        // -> go down its false-path, and pop its if-operation from ifOperations
        if(!currentBlock->hasBeenPassed()) {
            // Loop-Header has not been passed yet. Disregard all backLinks for merge-block considerations.
            // When the Loop-Header is a merge-block for CF above, it will be added to the candidateEdgeCounter and 
            // in-edges will be counted down.
            uint32_t openEdges = currentBlock->getPredecessors().size() - currentBlock->getBackLinks();
            if(candidateEdgeCounter.contains(currentBlock->getIdentifier())) {
                openEdges -= candidateEdgeCounter.at(currentBlock->getIdentifier()) - 1;
            }
            return openEdges > 1;
        } else {
            // We are in the body of the Loop-Header (true-branch). All potential in-edges from 'above' have been counted
            // already. We now count down potential in-edges from 'below' (backLinks).
            // WHAT IF: loop is no merge-block for above edges but for below? -> candidateEdgeCounter not set.
            // -> Loop will be returned as 'new' merge-block and added to list of candidateEdgeCounter blocks.
            //todo Alternative: only look at backLinks and countdown those? -> would require to reset candidateEdgeCounter (not 0 but RESET!)
            uint32_t openEdges = currentBlock->getBackLinks(); //if a loop-block has more than one backLink, there MUST be control flow in the loop-body and the loop-body is a merge-block
            if(candidateEdgeCounter.contains(currentBlock->getIdentifier())) {
                openEdges -= candidateEdgeCounter.at(currentBlock->getIdentifier()) - 1;
            }
            if(openEdges > 1) {
                return true;
            } else {    
                // branch leads to loop -> remove loop-header from ifOperations && follow its false path.
                // todo we skip the check of the child block!
                currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getBlock();
                ifOperations.pop();
                return mergeBlockCheck(currentBlock, ifOperations, candidateEdgeCounter); //todo cannot recurse n-times (only 2) because it would require multiple loops to be stacked in an impossible way?
            }

        }
    }
}

bool RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::findControlFlowMerge(IR::BasicBlockPtr& currentBlock, 
                                std::stack<std::unique_ptr<IfOpCandidate>>& ifOperations, 
                                std::unordered_map<std::string, uint32_t>& candidateEdgeCounter) {
    bool openMergeBlockFound = mergeBlockCheck(currentBlock, ifOperations, candidateEdgeCounter);
    
    while(!openMergeBlockFound && (currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp)) {
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
            // auto firstPotentialLoopIf = ifOperations.top()->ifOp->getValue();
            // auto secondPotentialLoopIf = currentBlock->getTerminatorOp();
            // if(!ifOperations.empty() && (firstPotentialLoopIf == secondPotentialLoopIf)) {
        } else if (terminatorOp->getOperationType() == Operation::IfOp) {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
            ifOperations.emplace(std::make_unique<IfOpCandidate>(IfOpCandidate{ifOp, true}));
            if(currentBlock->isLoopHeadBlock()) {
                // We processed everything 'above' the loop-header and now dig down its body.
                currentBlock->setPassed();
                if(candidateEdgeCounter.contains(currentBlock->getIdentifier())) {
                    // Erase value from candidate edge counter. If the Loop-Header acts as a merge block for if-ops below,
                    // we start counting down again using its backLinks.
                    candidateEdgeCounter.erase(currentBlock->getIdentifier()); //requires map to be non-const!
                }
            }
            currentBlock = ifOp->getTrueBlockInvocation().getBlock();
        }
        // check if block qualifies as merge block
        openMergeBlockFound = mergeBlockCheck(currentBlock, ifOperations, candidateEdgeCounter);
    }
    return !ifOperations.empty(); //todo could it happen that there are no ifOperations, but we still potentially want to continue?
}

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::createIfOperations(IR::BasicBlockPtr currentBlock) {
    std::stack<std::unique_ptr<IfOpCandidate>> ifOperations;
    std::stack<IR::BasicBlockPtr> mergeBlocks;
    std::unordered_map<std::string, uint32_t> candidateEdgeCounter; //rename
    
    // while(currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp) { 
    while(findControlFlowMerge(currentBlock, ifOperations, candidateEdgeCounter)) {
        auto currentIfCandidate = std::move(ifOperations.top());
        ifOperations.pop(); //could skip popping? -> do we actually not re-add it in some case?
        if(currentIfCandidate->isTrueBranch) {
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
            //assumption: merge block candidate must be the one of the next-if-operation (in stack) -> in principle we need to know whether the merge-block actually belongs to the current ifOperation!!!!
            while(!ifOperations.empty() && !ifOperations.top()->isTrueBranch && !mergeBlocks.empty() && 
                    mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier()) {
                mergeBlocks.pop();
                ifOperations.pop();
            }
            // uint32_t openEdges = currentBlock->getPredecessors().size() - currentBlock->isLoopHeadBlock();
            // if(candidateEdgeCounter.contains(currentBlock->getIdentifier())) {
            //     openEdges -= candidateEdgeCounter.at(currentBlock->getIdentifier()) - 1;
            // }
            // bool openMergeBlockFound = mergeBlockCheck(currentBlock, ifOperations, candidateEdgeCounter); //often leads to double check. Can we combine with while condition?
            // if(openMergeBlockFound) {
            //     if()
            //     mergeBlocks.emplace(currentBlock);
            //     ifOperations.top()->isTrueBranch = false;
            //     currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getBlock();
            // }
        }
    }
}
}//namespace NES::Nautilus::Tracing