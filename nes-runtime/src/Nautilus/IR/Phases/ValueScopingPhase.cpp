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
        std::stack<IR::BasicBlockPtr> newBlocks;
        std::unordered_set<IR::BasicBlock*> visitedBlocks;
        std::unordered_map<IR::Operations::Operation*, IR::Operations::OperationPtr> valueMap;
    
        uint32_t currentId = 0;
        newBlocks.emplace(ir->getRootOperation()->getFunctionBasicBlock());
        do {
            visitedBlocks.emplace(newBlocks.top().get());
            if (newBlocks.top()->getTerminatorOp()->getOperationType() == IR::Operations::Operation::BranchOp) {
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(newBlocks.top()->getTerminatorOp());
                auto inArg = newBlocks.top()->getArguments().at(0);
                for(size_t i = 0; i < branchOp->getNextBlockInvocation().getBlock()->getArguments().size(); ++i) {
                    if(branchOp->getNextBlockInvocation().getArguments().at(i)->getOperationType() == Operation::BasicBlockArgument) {
                        branchOp->getNextBlockInvocation().getBlock()->getArguments().at(i)
                            ->setBaseOperation(std::static_pointer_cast<BasicBlockArgument>(
                                branchOp->getNextBlockInvocation().getArguments().at(i))->getBaseOperation());
                    } else {
                        branchOp->getNextBlockInvocation().getBlock()->getArguments().at(i)
                            ->setBaseOperation(branchOp->getNextBlockInvocation().getArguments().at(i));
                    }
                }
                newBlocks.pop();
                newBlocks.emplace(branchOp->getNextBlockInvocation().getBlock());
            } else if (newBlocks.top()->getTerminatorOp()->getOperationType() == IR::Operations::Operation::IfOp) {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(newBlocks.top()->getTerminatorOp());
                for(size_t i = 0; i < ifOp->getTrueBlockInvocation().getBlock()->getArguments().size(); ++i) {
                    if(ifOp->getTrueBlockInvocation().getArguments().at(i)->getOperationType() == Operation::BasicBlockArgument) {
                        ifOp->getTrueBlockInvocation().getBlock()->getArguments().at(i)
                            ->setBaseOperation(std::static_pointer_cast<BasicBlockArgument>(
                                ifOp->getTrueBlockInvocation().getArguments().at(i))->getBaseOperation());
                    } else {
                        ifOp->getTrueBlockInvocation().getBlock()->getArguments().at(i)
                            ->setBaseOperation(ifOp->getTrueBlockInvocation().getArguments().at(i));
                    }
                }
                for(size_t i = 0; i < ifOp->getFalseBlockInvocation().getBlock()->getArguments().size(); ++i) {
                    if(ifOp->getFalseBlockInvocation().getArguments().at(i)->getOperationType() == Operation::BasicBlockArgument) {
                        ifOp->getFalseBlockInvocation().getBlock()->getArguments().at(i)
                            ->setBaseOperation(std::static_pointer_cast<BasicBlockArgument>(
                                ifOp->getFalseBlockInvocation().getArguments().at(i))->getBaseOperation());
                    } else {
                        ifOp->getFalseBlockInvocation().getBlock()->getArguments().at(i)
                            ->setBaseOperation(ifOp->getFalseBlockInvocation().getArguments().at(i));
                    }
                }
                newBlocks.pop();
                newBlocks.emplace(ifOp->getFalseBlockInvocation().getBlock());
                newBlocks.emplace(ifOp->getTrueBlockInvocation().getBlock());
            } else if (newBlocks.top()->getTerminatorOp()->getOperationType() == IR::Operations::Operation::LoopOp) {
                auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(newBlocks.top()->getTerminatorOp());
                for(size_t i = 0; i < loopOp->getLoopBodyBlock().getBlock()->getArguments().size(); ++i) {
                    if(loopOp->getLoopBodyBlock().getArguments().at(i)->getOperationType() == Operation::BasicBlockArgument) {
                        loopOp->getLoopBodyBlock().getBlock()->getArguments().at(i)
                            ->setBaseOperation(std::static_pointer_cast<BasicBlockArgument>(
                                loopOp->getLoopBodyBlock().getArguments().at(i))->getBaseOperation());
                    } else {
                        loopOp->getLoopBodyBlock().getBlock()->getArguments().at(i)
                            ->setBaseOperation(loopOp->getLoopBodyBlock().getArguments().at(i));
                    }
                }
                for(size_t i = 0; i < loopOp->getLoopFalseBlock().getBlock()->getArguments().size(); ++i) {
                    if(loopOp->getLoopFalseBlock().getArguments().at(i)->getOperationType() == Operation::BasicBlockArgument) {
                        loopOp->getLoopFalseBlock().getBlock()->getArguments().at(i)
                            ->setBaseOperation(std::static_pointer_cast<BasicBlockArgument>(
                                loopOp->getLoopFalseBlock().getArguments().at(i))->getBaseOperation());
                    } else {
                        loopOp->getLoopFalseBlock().getBlock()->getArguments().at(i)
                            ->setBaseOperation(loopOp->getLoopFalseBlock().getArguments().at(i));
                    }
                }
                newBlocks.pop();
                newBlocks.emplace(loopOp->getLoopFalseBlock().getBlock());
                newBlocks.emplace(loopOp->getLoopBodyBlock().getBlock());
            } else if (newBlocks.top()->getTerminatorOp()->getOperationType() == IR::Operations::Operation::ReturnOp) {
                //todo
                newBlocks.pop();
            } else {
                NES_ERROR("BasicBlock::getNextBlocks: Tried to get next block for unsupported operation type: " 
                            << newBlocks.top()->getTerminatorOp()->getOperationType());
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
        } while (!newBlocks.empty());
}


}//namespace NES::Nautilus::IR