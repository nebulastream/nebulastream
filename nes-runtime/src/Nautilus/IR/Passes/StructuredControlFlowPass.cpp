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

#include "Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp"
#include "Nautilus/IR/Operations/Loop/LoopInfo.hpp"
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Passes/StructuredControlFlowPass.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <unordered_map>

using namespace NES::Nautilus::IR::Operations;
namespace NES::Nautilus::IR {

std::shared_ptr<IR::IRGraph> StructuredControlFlowPass::apply(std::shared_ptr<IR::IRGraph> ir, bool findSimpleCountedLoops) {
    auto phaseContext = StructuredControlFlowPassContext(std::move(ir), findSimpleCountedLoops);
    return phaseContext.process();
};

std::shared_ptr<IR::IRGraph> StructuredControlFlowPass::StructuredControlFlowPassContext::process() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    findLoopHeadBlocks(rootOperation->getFunctionBasicBlock());
    createIfOperations(rootOperation->getFunctionBasicBlock());
    return std::move(ir);
}

void StructuredControlFlowPass::StructuredControlFlowPassContext::checkBranchForLoopHeadBlocks(
        IR::BasicBlockPtr& currentBlock, std::stack<IR::BasicBlockPtr>& candidates, 
        std::unordered_set<std::string>& visitedBlocks, std::unordered_set<std::string>& loopHeadCandidates) {
    auto previousBlock = currentBlock;
    while(!visitedBlocks.contains(currentBlock->getIdentifier()) && 
            currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp) {
        auto terminatorOp = currentBlock->getTerminatorOp();
        if(terminatorOp->getOperationType() == Operation::BranchOp) {
            auto nextBlock = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp)->getNextBlockInvocation().getBlock();
            visitedBlocks.emplace(currentBlock->getIdentifier());
            previousBlock = currentBlock;
            currentBlock = nextBlock;
        } else if (terminatorOp->getOperationType() == Operation::IfOp) {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
            loopHeadCandidates.emplace(currentBlock->getIdentifier());
            candidates.emplace(currentBlock);
            visitedBlocks.emplace(currentBlock->getIdentifier());
            previousBlock = currentBlock;
            currentBlock = ifOp->getTrueBlockInvocation().getBlock();
        }
    }
    if(loopHeadCandidates.contains(currentBlock->getIdentifier())) {
        //todo replace ifOperation with loopOperation #3169
        // Add counted loop info for simple for-loops as discussed with Dwi
        // Required:
        //  lowerBound: 0 or if-condition-left (might be problematic)
        //  upperBound: if-condition-right
        //  stepSize:   1
        //  inductionVariable: if-condition-left
        //  loopBodyBlockRef:   if-true-branch
        //  loopEndBlock: second predecessor
        //  (afterLoopBlockRef: if-else-branch)
        if(findSimpleCountedLoops) {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
            auto countedLoopInfo = std::make_unique<CountedLoopInfo>();
            if(ifOp->getBooleanValue()->getOperationType() == Operation::CompareOp) {
                auto compareOp = std::static_pointer_cast<Operations::CompareOperation>(ifOp->getBooleanValue());
                countedLoopInfo->lowerBound = compareOp->getLeftInput();
                countedLoopInfo->upperBound = compareOp->getRightInput();
                countedLoopInfo->loopBodyInductionVariable = compareOp->getLeftInput();
            }
            countedLoopInfo->loopEndBlock = previousBlock;
            // Todo the MLIR value for step size is only created when the respective block is called.
            // -> this happens AFTER the loop-operation is created
            // -> thus, the loop-operation would point to an undefined value? Can we then insert a value there?
            // Solution for now: 
            // -> Option 1: Simply create an MLIR constant integer with value 0 (Ignore ValueFrame).
            // -> Option 2: Create an MLIR constant integer using the value defined in the given stepSize. Insert it
            //              into the ValueFrame using the correct identifier, and then when the MLIR code generation
            //              for the addOp, make sure that we check whether the value already exists so we do not 
            //              overwrite it.
            countedLoopInfo->stepSize = std::static_pointer_cast<Operations::AddOperation>(previousBlock->getOperations().at(countedLoopInfo->loopEndBlock->getOperations().size()-2))->getRightInput();
            countedLoopInfo->loopBodyBlock = ifOp->getTrueBlockInvocation().getBlock();
            ifOp->setCountedLoopInfo(std::move(countedLoopInfo));
        }
        currentBlock->incrementNumLoopBackEdge();
    }
}

void StructuredControlFlowPass::StructuredControlFlowPassContext::findLoopHeadBlocks(IR::BasicBlockPtr currentBlock) {
    std::stack<IR::BasicBlockPtr> ifBlocks;
    std::unordered_set<std::string> loopHeadCandidates;
    std::unordered_set<std::string> visitedBlocks;

    bool returnBlockVisited = false;
    bool noMoreIfBlocks = true;
    do {
        checkBranchForLoopHeadBlocks(currentBlock, ifBlocks, visitedBlocks, loopHeadCandidates);
        noMoreIfBlocks = ifBlocks.empty();
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

bool StructuredControlFlowPass::StructuredControlFlowPassContext::mergeBlockCheck(IR::BasicBlockPtr& currentBlock, 
                                std::stack<std::unique_ptr<IfOpCandidate>>& ifOperations,
                                std::unordered_map<std::string, uint32_t>& mergeBlocksNumVisits,
                                const bool newVisit, const std::unordered_set<IR::BasicBlockPtr>& loopBlockWithVisitedBody) {
    uint32_t openEdges = 0;
    uint32_t numPriorVisits = 0;
    bool mergeBlockFound = false;
    
    bool isAlreadyVisitedMergeBlock = mergeBlocksNumVisits.contains(currentBlock->getIdentifier());
    if(isAlreadyVisitedMergeBlock) {
            numPriorVisits = mergeBlocksNumVisits.at(currentBlock->getIdentifier()) - 1;
    }
    // if(!currentBlock->isLoopWithVisitedBody()) {
    if(!loopBlockWithVisitedBody.contains(currentBlock)) {
        openEdges = currentBlock->getPredecessors().size() 
                    - (currentBlock->isLoopHeaderBlock()) * currentBlock->getNumLoopBackEdges()
                    - numPriorVisits;
    } else {
        openEdges = currentBlock->getNumLoopBackEdges() - numPriorVisits;
        if(openEdges < 2) {
                currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getBlock();
                ifOperations.pop();
                // Recursion depth is 1, because false block cannot be an already visited loop header block.
                // return mergeBlockCheck(currentBlock, ifOperations, mergeBlockNumVisits, true);
                isAlreadyVisitedMergeBlock = mergeBlocksNumVisits.contains(currentBlock->getIdentifier());
                numPriorVisits = 0;
                if(isAlreadyVisitedMergeBlock) {
                        numPriorVisits = mergeBlocksNumVisits.at(currentBlock->getIdentifier()) - 1;
                }
                openEdges = currentBlock->getPredecessors().size() 
                    - (currentBlock->isLoopHeaderBlock()) * currentBlock->getNumLoopBackEdges()
                    - numPriorVisits;
        }
    }
    mergeBlockFound = openEdges > 1;
    if(mergeBlockFound && newVisit && isAlreadyVisitedMergeBlock) {
        mergeBlocksNumVisits[currentBlock->getIdentifier()] = mergeBlocksNumVisits[currentBlock->getIdentifier()] + 1;
    } else if (mergeBlockFound && newVisit && !isAlreadyVisitedMergeBlock) {
        mergeBlocksNumVisits.emplace(std::pair{currentBlock->getIdentifier(), 1});
    }
    return mergeBlockFound;
}

void StructuredControlFlowPass::StructuredControlFlowPassContext::createIfOperations(IR::BasicBlockPtr currentBlock) {
    std::stack<std::unique_ptr<IfOpCandidate>> ifOperations;
    std::stack<IR::BasicBlockPtr> mergeBlocks;
    std::unordered_map<std::string, uint32_t> mergeBlockNumVisits;
    std::unordered_set<IR::BasicBlockPtr> loopBlockWithVisitedBody;
    bool mergeBlockFound = true;
    bool newVisit = true;

    // Iterate over graph until all if-operations have been processed and matched with their corresponding merge-blocks.
    while(mergeBlockFound) {
        // Check blocks (DFS) until an open merge-block was found. Push encountered if-operations to stack.
        while(!(mergeBlockFound = mergeBlockCheck(currentBlock, ifOperations, mergeBlockNumVisits, newVisit, loopBlockWithVisitedBody)) 
                && (currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp)) {
            auto terminatorOp = currentBlock->getTerminatorOp();
            if(terminatorOp->getOperationType() == Operation::BranchOp) {
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                currentBlock = branchOp->getNextBlockInvocation().getBlock();
                newVisit = true;
            } else if (terminatorOp->getOperationType() == Operation::IfOp) {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                ifOperations.emplace(std::make_unique<IfOpCandidate>(IfOpCandidate{ifOp, true}));
                if(currentBlock->isLoopHeaderBlock()) {
                    // We processed everything 'above' the loop-header and now dig down its body.
                    loopBlockWithVisitedBody.emplace(currentBlock);
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