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

#include "Nautilus/IR/BasicBlocks/BasicBlock.hpp"
#include "Nautilus/IR/BasicBlocks/BasicBlockArgument.hpp"
#include "Nautilus/IR/Operations/BranchOperation.hpp"
#include "Nautilus/IR/Operations/Loop/LoopOperation.hpp"
#include "Nautilus/IR/Operations/Operation.hpp"
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Phases/ValueScopingPhase.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <string>
#include <unordered_map>

using namespace NES::Nautilus::IR::Operations;
namespace NES::Nautilus::IR {

void ValueScopingPhase::apply(std::shared_ptr<IR::IRGraph> ir) {
    auto phaseContext = ValueScopingPhaseContext(std::move(ir));
    phaseContext.process();
};

void ValueScopingPhase::ValueScopingPhaseContext::process() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    applyValueScoping();
}

void ValueScopingPhase::ValueScopingPhaseContext::applyValueScoping() {
        // std::stack<IR::BasicBlockPtr> newBlocks;
        IR::BasicBlockPtr currentBlock;
        std::unordered_set<IR::BasicBlock*> visitedLoopHeaders;
        std::unordered_map<IR::Operations::Operation*, IR::Operations::OperationPtr> valueMap;
        std::stack<std::unique_ptr<IR::Operations::IfOperation>> ifOperations;
        std::stack<std::pair<BasicBlockPtr, uint32_t>> mergeBlocks;
    
        uint32_t currentId = 0;
        // Todo First, simply attach the baseValue that the args are referring to, to the args
        // - problem case 1: Merge Block
        //  -> we want the merge-block arg to become the new base-value
        //  -> we want the merge-block arg to contain references to all values that it represents
        // - problem case 2: Loop Block
        //  -> we want only the loop-induction variable, and all loop-iteration-variables to become new base-values
        //  -> all other values should simply be passed on to the loopFalseBlock
        //  -> we want the loop-induction variable and al loop-iteration-variables to contain the initial base-value
        //     and the base-value returned by the loop-body back to the loop-header
        // - overall, if an arg references more than one value, it should stay

        // Todo Iteration:
        // -> iterate until merge-block or loop-header met
        //  Merge-Block: (wrapper approach: potential values: numOpenEdges)
        //      1. if new merge-block, create entry (or create wrapper around merge-block and mark), else proceed to 2.
        //      2. if numOpenEdges > 0, get top-most if-op from if-op stack, and follow its false branch
        //         if numOpenEdges == 0, set nextBlock's args, if arg is merge-block arg, set merge-block arg as baseValue
        // Loop-Header-Block (assuming only 1 loopBackEdge):
        //      1. if first visit, add to loop stack and follow true-branch(body) (is loop stack required? yes, if loop-header-block can also be merge-block)
        //         else, set false-branch-block args, loop-args that contain more than 1 baseValue become new baseValues
        // Merge-Block && Loop-Header:
        //      1. follow merge-block procedure until numOpenEdges == 0
        //      2. add to loop stack and follow true-branch
        //      3. on re-encountering loop-header, set false-branch-block args, loop-args that contain more than 1 baseValue become new baseValues
        currentBlock = ir->getRootOperation()->getFunctionBasicBlock();
        do {
            // check if currentBlock is if-op -> push to ifOp-stack
            // check if currentBlock is merge-block && !loop-header-block -> follow merge-block procedure
            // check if currentBlock is !merge-block && loop-header-block -> follow loop-header-block procedure
            // check if currentBlock is merge-block && !loop-header-block -> follow merge-block until numOpenEdges == 0, then follow loop procedure procedure
            // Could use if-operations to build up merge-block stack (to not change basic-block to contain block status)
            if(currentBlock->isMergeBlock()) {
                if(mergeBlocks.top().first == currentBlock) {
                    if(mergeBlocks.top().second++ >= (currentBlock->getPredecessors().size() - currentBlock->getNumLoopBackEdges())) {
                        // All merge-block edges have been taken - continue post merge-block!
                        if(currentBlock->isLoopHeaderBlock()) {
                            // Merge block is also loop header block
                            // -> allow to get into loop case (must continue on all other cases)
                        } else {
                            // todo set next block args!
                            // -> also set merge-block args as new base-values
                            // Perform if-operation check
                            // newBlocks
                            auto nextBlocks = mergeBlocks.top().first->getNextBlocks();
                            if(nextBlocks.first) {
                                currentBlock = nextBlocks.first;
                            } else {
                                // Todo return operation hit
                            }
                            mergeBlocks.pop();
                        }
                    } else {
                        // There are still merge-block edges to take
                        currentBlock = ifOperations.top()->getFalseBlockInvocation().getBlock();
                        continue; // could replace with boolean to avoid continues
                    }
                } else {
                    // First encounter of merge-block
                    mergeBlocks.emplace(std::make_pair(currentBlock, 1));
                    currentBlock = ifOperations.top()->getFalseBlockInvocation().getBlock();
                }
            }
            if(currentBlock->isLoopHeaderBlock()) {
                // Check if first visit of loop-header-block
                auto loopHeaderBlock = currentBlock;
                if(!visitedLoopHeaders.contains(currentBlock.get())) {
                    // Is first visit
                    visitedLoopHeaders.emplace(currentBlock.get());
                    currentBlock = std::static_pointer_cast<LoopOperation>(
                        loopHeaderBlock->getTerminatorOp())->getLoopBodyBlock().getBlock();
                    continue;
                } else {
                    // We looped back to the loop-header-block
                    auto loopFalseBlock = std::static_pointer_cast<LoopOperation>(
                        loopHeaderBlock->getTerminatorOp())->getLoopFalseBlock().getBlock();
                    // Todo set false-branch-block args

                    if(!visitedLoopHeaders.contains(loopFalseBlock.get())) {
                        currentBlock = loopFalseBlock;
                    }
                }
            }


            if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::BranchOp) {
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(currentBlock->getTerminatorOp());
                auto inArg = currentBlock->getArguments().at(0);
                for(size_t i = 0; i < branchOp->getNextBlockInvocation().getBlock()->getArguments().size(); ++i) {
                    if(branchOp->getNextBlockInvocation().getArguments().at(i)->getOperationType() == Operation::BasicBlockArgument) {
                        branchOp->getNextBlockInvocation().getBlock()->getArguments().at(i)
                            ->addBaseOperation(std::static_pointer_cast<BasicBlockArgument>(
                                branchOp->getNextBlockInvocation().getArguments().at(i))->getBaseOperations().at(0));
                    } else {
                        branchOp->getNextBlockInvocation().getBlock()->getArguments().at(i)
                            ->addBaseOperation(branchOp->getNextBlockInvocation().getArguments().at(i));
                    }
                }
                currentBlock = branchOp->getNextBlockInvocation().getBlock();
            } else if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::IfOp) {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
                for(size_t i = 0; i < ifOp->getTrueBlockInvocation().getBlock()->getArguments().size(); ++i) {
                    if(ifOp->getTrueBlockInvocation().getArguments().at(i)->getOperationType() == Operation::BasicBlockArgument) {
                        ifOp->getTrueBlockInvocation().getBlock()->getArguments().at(i)
                            ->addBaseOperation(std::static_pointer_cast<BasicBlockArgument>(
                                ifOp->getTrueBlockInvocation().getArguments().at(i))->getBaseOperations().at(1));
                    } else {
                        ifOp->getTrueBlockInvocation().getBlock()->getArguments().at(i)
                            ->addBaseOperation(ifOp->getTrueBlockInvocation().getArguments().at(i));
                    }
                }
                for(size_t i = 0; i < ifOp->getFalseBlockInvocation().getBlock()->getArguments().size(); ++i) {
                    if(ifOp->getFalseBlockInvocation().getArguments().at(i)->getOperationType() == Operation::BasicBlockArgument) {
                        ifOp->getFalseBlockInvocation().getBlock()->getArguments().at(i)
                            ->addBaseOperation(std::static_pointer_cast<BasicBlockArgument>(
                                ifOp->getFalseBlockInvocation().getArguments().at(i))->getBaseOperations().at(0));
                    } else {
                        ifOp->getFalseBlockInvocation().getBlock()->getArguments().at(i)
                            ->addBaseOperation(ifOp->getFalseBlockInvocation().getArguments().at(i));
                    }
                }
                ifOperations.emplace(std::static_pointer_cast<IfOperation>(currentBlock->getTerminatorOp()));
                currentBlock = ifOp->getTrueBlockInvocation().getBlock();
            // } else if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::LoopOp) {
            //     auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(currentBlock->getTerminatorOp());
            //     for(size_t i = 0; i < loopOp->getLoopBodyBlock().getBlock()->getArguments().size(); ++i) {
            //         if(loopOp->getLoopBodyBlock().getArguments().at(i)->getOperationType() == Operation::BasicBlockArgument) {
            //             loopOp->getLoopBodyBlock().getBlock()->getArguments().at(i)
            //                 ->setBaseOperation(std::static_pointer_cast<BasicBlockArgument>(
            //                     loopOp->getLoopBodyBlock().getArguments().at(i))->getBaseOperation());
            //         } else {
            //             loopOp->getLoopBodyBlock().getBlock()->getArguments().at(i)
            //                 ->setBaseOperation(loopOp->getLoopBodyBlock().getArguments().at(i));
            //         }
            //     }
            //     for(size_t i = 0; i < loopOp->getLoopFalseBlock().getBlock()->getArguments().size(); ++i) {
            //         if(loopOp->getLoopFalseBlock().getArguments().at(i)->getOperationType() == Operation::BasicBlockArgument) {
            //             loopOp->getLoopFalseBlock().getBlock()->getArguments().at(i)
            //                 ->setBaseOperation(std::static_pointer_cast<BasicBlockArgument>(
            //                     loopOp->getLoopFalseBlock().getArguments().at(i))->getBaseOperation());
            //         } else {
            //             loopOp->getLoopFalseBlock().getBlock()->getArguments().at(i)
            //                 ->setBaseOperation(loopOp->getLoopFalseBlock().getArguments().at(i));
            //         }
            //     }
            //     newBlocks.emplace(loopOp->getLoopFalseBlock().getBlock());
            //     newBlocks.emplace(loopOp->getLoopBodyBlock().getBlock());
            } else if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::ReturnOp) {
                //todo
            } else {
                NES_ERROR("BasicBlock::getNextBlocks: Tried to get next block for unsupported operation type: " 
                            << currentBlock->getTerminatorOp()->getOperationType());
                NES_NOT_IMPLEMENTED();
            }
            // // Idea: iterate over all args, and assign them the correct base values
            // // -> only applies when passing arguments from one block to another

            // // dpsSortedIRGraph.emplace_back(newBlocks.top());
            // auto nextBlocks = newBlocks.top()->getNextBlocks();
            // newBlocks.pop();
            // // Br case
            // if(nextBlocks.second && !visitedBlocks.contains(nextBlocks.first.get())) {
            //     nextBlocks.second->getArguments()
            //     newBlocks.emplace(nextBlocks.second);

            // }
            // if(nextBlocks.first && !visitedBlocks.contains(nextBlocks.first.get())) {
            //     newBlocks.emplace(nextBlocks.first);
            // }
        } while (currentBlock); //TODO replace condition!!
}


}//namespace NES::Nautilus::IR