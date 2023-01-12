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

#include "Nautilus/IR/BasicBlocks/BasicBlockArgument.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp"
#include "Nautilus/IR/Operations/ConstIntOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp"
#include "Nautilus/IR/Operations/Loop/LoopInfo.hpp"
#include "Nautilus/IR/Operations/Loop/LoopOperation.hpp"
#include "Nautilus/Util/IRDumpHandler.hpp"
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
#include <unordered_set>

using namespace NES::Nautilus::IR::Operations;
namespace NES::Nautilus::IR {

void StructuredControlFlowPhase::apply(std::shared_ptr<IR::IRGraph> ir) {
    if(ir->getAppliedPhases() == IRGraph::AppliedPhases::LoopDetectionPhase) {
        auto phaseContext = StructuredControlFlowPhaseContext(std::move(ir));
        phaseContext.process();
    } else {
        NES_ERROR("LoopDetectionPhase::applyLoopDetection: Could not apply LoopDetectionPhase. Requires IRGraph " <<
                    "to have BrOnlyBlocksPhase applied, but applied phase was: " << ir->printAppliedPhases());
    }
};

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::process() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    ir->setAppliedPhases(IRGraph::AppliedPhases::StructuredControlFlowPhase);
    createIfOperations(rootOperation->getFunctionBasicBlock());
}

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::forwardArgs(const BasicBlockPtr& currentBlock, 
                                                                                const bool toLoopBody) {
    size_t nextBlockIndex = (!toLoopBody && currentBlock->isLoopHeaderBlock());
    for(; nextBlockIndex < currentBlock->getNextBlockInvocations().size() - 
            (toLoopBody && currentBlock->isLoopHeaderBlock()); ++nextBlockIndex) {
        auto nextBlockInvocation = currentBlock->getNextBlockInvocations().at(nextBlockIndex);
        // We check whether the nextBlockInvocation has already received baseOps.
        if(!(nextBlockInvocation->getNextBlock()->getArguments().back()->getBaseOperationWrapper()) ||
            nextBlockInvocation->getNextBlock()->getArguments().empty()) {
            // We have not forwarded args to the nextBlock yet.
            for(size_t i = 0; i < nextBlockInvocation->getNextBlock()->getArguments().size(); ++i) {
                if(nextBlockInvocation->getBranchOps().at(i)->getOperationType() != Operation::BasicBlockArgument) {
                    nextBlockInvocation->getNextBlock()->getArguments().at(i)
                    ->setBaseOperationWrapper(
                        std::make_shared<Operations::BasicBlockArgument::BaseOperationWrapper>(
                            Operations::BasicBlockArgument::BaseOperationWrapper{nextBlockInvocation->getBranchOps().at(i)}));
                } else {
                    // Forward the BaseOperationWrapper to the next block.
                    auto basicBlockArg = std::static_pointer_cast<Operations::BasicBlockArgument>(
                        nextBlockInvocation->getBranchOps().at(i));
                    // If next block is loop-header-block, create new BaseOperationWrapper. 
                    // Simple merge-blocks only replace forwarded BaseOperationWrappers if a  merge-block arg is found.
                    if(nextBlockInvocation->getNextBlock()->isLoopHeaderBlock()) {
                        nextBlockInvocation->getNextBlock()->getArguments().at(i)
                        ->setBaseOperationWrapper(
                            std::make_shared<Operations::BasicBlockArgument::BaseOperationWrapper>(
                                Operations::BasicBlockArgument::BaseOperationWrapper{basicBlockArg->getBaseOperationPtr()}));
                    } else {
                        // Simply forward BaseOperationWrapper. 
                        nextBlockInvocation->getNextBlock()->getArguments().at(i)
                        ->setBaseOperationWrapper(basicBlockArg->getBaseOperationWrapper());
                    }
                }
            }
        } else {
            // We have already visited the nextBlock, thus, it merges control flow (merge- or/and loop-header-block).
            for(size_t argIndex = 0; argIndex < nextBlockInvocation->getNextBlock()->getArguments().size(); ++argIndex) {
                // If arg already has a baseOperation, and it is different, we found a merge-arg.
                if(!(nextBlockInvocation->getNextBlock()->getArguments().at(argIndex)->getIsMergeArg())) {
                    // Check whether the current operation is a BasicBlockArg
                    OperationPtr forwardedOp = (nextBlockInvocation->getBranchOps().at(argIndex)
                                                ->getOperationType() == Operation::BasicBlockArgument) 
                        ? std::static_pointer_cast<Operations::BasicBlockArgument>(
                            nextBlockInvocation->getBranchOps().at(argIndex))->getBaseOperationPtr()
                        : nextBlockInvocation->getBranchOps().at(argIndex);
                    // Check whether operation or base operation of arg equals nextBlock's base operation.
                    if(forwardedOp != nextBlockInvocation->getNextBlock()->getArguments().at(argIndex)->getBaseOperationPtr()) {
                        if(nextBlockInvocation->getNextBlock()->isLoopHeaderBlock()) {
                            nextBlockInvocation->getNextBlock()->getArguments().at(argIndex)
                            ->setBaseOperationPtr(nextBlockInvocation->getNextBlock()->getArguments().at(argIndex));
                        } else {
                            // nextBlock is a non-loop-header merge-block, we have update the BaseOperationWrapper.
                            nextBlockInvocation->getNextBlock()->getArguments().at(argIndex)
                            ->setBaseOperationWrapper(
                                std::make_shared<Operations::BasicBlockArgument::BaseOperationWrapper>(
                                    Operations::BasicBlockArgument::BaseOperationWrapper{
                                        nextBlockInvocation->getNextBlock()->getArguments().at(argIndex)
                                    }));
                        }
                    } // else, simply continue
                } // else: The currentArg is a merge-arg. Thus, its baseOperation is itself(this), and we can just continue.
            }
        }
    }
}

bool NES::Nautilus::IR::StructuredControlFlowPhase::StructuredControlFlowPhaseContext::mergeBlockCheck(
                                IR::BasicBlockPtr& currentBlock, 
                                std::unordered_map<BasicBlock*, uint32_t>& numMergeBlocksVisits,
                                const bool newVisit, const std::unordered_set<IR::BasicBlock*>& visitedBlocks) {
    uint32_t openEdges = 0;
    uint32_t numPriorVisits = 0;
    bool mergeBlockFound = false;

    bool isAlreadyVisitedMergeBlock = numMergeBlocksVisits.contains(currentBlock.get());
    if(isAlreadyVisitedMergeBlock) {
        // We deduct '1' from the number of prior visits so that openEdges is > 1 even if we already visited all the 
        // merge-blocks's open edges. This is important to not accidentally recognize branch-blocks(openEdges: 1) as
        // merge-blocks.
        numPriorVisits = numMergeBlocksVisits.at(currentBlock.get()) - 1;
    }
    // Calculating openEdges:
    //  If we did not loop back to a loop-block coming from the loop-block's body:
    //  -> deduct the number of prior visits from the number of predecessors of the block.
    //  -> if it is a loop-header-block, deduct the number of loopBackEdges.
    //  Else: 
    //  -> simply deduct the number of prior visits from the number of loopBackEdges.
    //  -> if the loop-header-block has no more openEdges, we exhausted its true-branch and switch to its false-branch.
    if(!(currentBlock->isLoopHeaderBlock() && visitedBlocks.contains(currentBlock.get()))) {
        openEdges = currentBlock->getPredecessors().size() 
                    - numPriorVisits
                    - (currentBlock->isLoopHeaderBlock()) * currentBlock->getNumLoopBackEdges();
    } else {
        openEdges = currentBlock->getNumLoopBackEdges() - numPriorVisits;
        if(openEdges < 2) {
                //Todo does this actually guarantee that loop-header-blocks are visited after all loop-body-blocks?
                forwardArgs(currentBlock, /* toLoopBody */ false);
                // Todo is emplacing the loop-header block required? 
                // -> in theory we should not visit it again -> can make visitedBlocks const again
                // visitedBlocks.emplace(currentBlock.get());
                // We exhausted the loop-operations true-branch (body block) and now switch to its false-branch.
                currentBlock = std::static_pointer_cast<Operations::LoopOperation>(currentBlock->getTerminatorOp())
                                ->getLoopFalseBlock().getNextBlock();
                // Since we switched to a new currentBlock, we need to check whether it is a merge-block with openEdges.
                // If the new currentBlock is a loop-header-block again, we have multiple recursive calls.
                return mergeBlockCheck(currentBlock, numMergeBlocksVisits, true, visitedBlocks);
        }
    }
    // If the number of openEdges is 2 or greater, we found a merge-block.
    mergeBlockFound = openEdges > 1;
    // If we found a merge-block, and we came from a new edge increase the visit counter by 1 or set it to 1.
    if(mergeBlockFound && newVisit && isAlreadyVisitedMergeBlock) {
        numMergeBlocksVisits[currentBlock.get()] = numMergeBlocksVisits[currentBlock.get()] + 1;
    } else if (mergeBlockFound && newVisit && !isAlreadyVisitedMergeBlock) {
        numMergeBlocksVisits.emplace(std::pair{currentBlock.get(), 1});
    }
    return mergeBlockFound;
}

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::createIfOperations(IR::BasicBlockPtr currentBlock) {
    std::stack<std::unique_ptr<IfOpCandidate>> ifOperations;
    std::stack<IR::BasicBlockPtr> mergeBlocks;
    std::unordered_map<BasicBlock*, uint32_t> numMergeBlockVisits;
    std::unordered_set<IR::BasicBlock*> visitedBlocks;
    bool mergeBlockFound = true;
    // The newVisit flag is passed to the mergeBlockCheck() function to indicate whether we traversed a new edge to the 
    // currentBlock before calling mergeBlockCheck().
    bool newVisit = true;

    // Iterate over graph until all if-operations have been processed and matched with their corresponding merge-blocks.
    while(mergeBlockFound) {
        // Check blocks (DFS) until an open merge-block was found. Push encountered if-operations to stack.
        // In a nutshell, we identify open merge-blocks by checking the number of incoming edges vs mergeBlockNumVisits 
        // and numLoopBackEdges. For example, a block that has 2 incoming edges, 2 numMergeBlockVisits, and 0
        // numLoopBackEdges is an closed merge-block that merges two control-flow-branches. In contrast, a block that has
        // 5 incoming edges, 2 numMergeBlockVisits, and 1 numLoopBackEdges is an open merge-block with still 2 open
        // control-flow-merge-edges. Also, it is a loop-header-block with 1 numLoopBackEdge. (5-2-1 => 2 still open)
        while(!(mergeBlockFound = mergeBlockCheck(currentBlock, numMergeBlockVisits, newVisit, visitedBlocks)) 
                && (currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp)) {
            //Todo !
            if(!visitedBlocks.contains(currentBlock.get())) {
                forwardArgs(currentBlock, /* toLoopBody */ true);
                visitedBlocks.emplace(currentBlock.get());
            }
            auto terminatorOp = currentBlock->getTerminatorOp();
            if(terminatorOp->getOperationType() == Operation::BranchOp) {
                // If the currentBlock is a simple branch-block, we move to the nextBlock.
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                currentBlock = branchOp->getNextBlockInvocation().getNextBlock();
                newVisit = true;
            } else if (terminatorOp->getOperationType() == Operation::IfOp) { 
                // If the currentBlock is an if-block, we push its if-operation on top of our IfOperation stack.
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                ifOperations.emplace(std::make_unique<IfOpCandidate>(IfOpCandidate{ifOp, true}));
                // We now follow the if-operation's true-branch until we find a new if-operation or a merge-block.
                currentBlock = ifOp->getTrueBlockInvocation().getNextBlock();
                newVisit = true;
            }
            else if (terminatorOp->getOperationType() == Operation::LoopOp) {
                // if(!visitedBlocks.contains(currentBlock.get())) {
                // forwardArgs(currentBlock, /* toLoopBody */ true);
                // }
                if(numMergeBlockVisits.contains(currentBlock.get())) {
                    numMergeBlockVisits.erase(currentBlock.get());
                }
                auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(terminatorOp);
                currentBlock = loopOp->getLoopBodyBlock().getNextBlock();
                newVisit = true;
            }
        }
        // If no merge-block was found, we traversed the entire graph and are done (return block is current block).
        if(mergeBlockFound) {
            // If a merge-block was found, depending on whether the we are in the current if-operations' true
            // or false branch, we either set it as the current if-operation's true-branch-block,
            // or set it as current if-operation's false-branch-block.
            if(ifOperations.top()->isTrueBranch) {
                // We explored the current if-operation's true-branch and now switch to its false-branch.
                ifOperations.top()->isTrueBranch = false; 
                mergeBlocks.emplace(currentBlock);
                currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getNextBlock();
                newVisit = true;
            } else {
                //Todo 
                if(!visitedBlocks.contains(currentBlock.get()) && !currentBlock->isLoopHeaderBlock()) {
                    forwardArgs(currentBlock, /* toLoopBody */ false);
                    visitedBlocks.emplace(currentBlock.get());
                }
                // Make sure that we found the current merge-block for the current if-operation.
                assert(mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier());
                // Set currentBlock as merge-block for the current if-operation and all if-operations on the stack that:
                //  1. Are in their false-branches (their merge-block was pushed to the stack).
                //  2. Have a corresponding merge-block on the stack that matches the currentBlock.
                do {
                    ifOperations.top()->ifOp->setMergeBlock(std::move(mergeBlocks.top()));
                    mergeBlocks.pop();
                    ifOperations.pop();
                } while(!ifOperations.empty() && !ifOperations.top()->isTrueBranch && !mergeBlocks.empty() 
                        && mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier());
                // In this case, we do not visit a block via a new edge, so newVisit is false.
                // This is important in case the the current top-most if-operation on the stack is in its true-branch
                // and the currentBlock is its merge-block.
                newVisit = false;
            }
        }
    }
}

}//namespace NES::Nautilus::IR::Operations