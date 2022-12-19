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
#include "Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp"
#include "Nautilus/IR/Operations/BranchOperation.hpp"
#include "Nautilus/IR/Operations/IfOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp"
#include "Nautilus/IR/Operations/Loop/LoopOperation.hpp"
#include "Nautilus/IR/Operations/Operation.hpp"
#include "Nautilus/IR/Operations/ReturnOperation.hpp"
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
    replaceArguments();
    NES_DEBUG(ir->toString());
}

void ValueScopingPhase::ValueScopingPhaseContext::replaceArguments() {
    std::stack<IR::BasicBlockPtr> toVisitBlocks;
    std::unordered_set<IR::BasicBlock*> visitedBlocks;
    std::unordered_map<IR::BasicBlock*, uint32_t> mergeBlocks;

    toVisitBlocks.emplace(ir->getRootOperation()->getFunctionBasicBlock());
    do {
        // First, we get the currentBlock, from the list of blocks that we still need to visit. Then, we place it into
        // the list of blocks that have been visited, and we get the next blocks (between 0(return) and 2(if/loop)).
        auto currentBlock = toVisitBlocks.top();
        toVisitBlocks.pop();
        visitedBlocks.emplace(currentBlock.get());
        auto nextBlocks = toVisitBlocks.top()->getNextBlockInvocations();
        // We iterate over all of the operations of the currentBlock.
        for(auto& operation : currentBlock->getOperations()) {
            // The current operation is a non-terminator- or return operation. We check whether the operation uses
            // the currentBlock's arguments, and if so, we check whether we can replace the arguments, with their 
            // corresponding base operations.
            if(operation != currentBlock->getTerminatorOp() || operation->getOperationType() == Operation::ReturnOp) {
                for(auto inputOp : operation->getInputs(operation)) {
                    size_t inputIndex = 0;
                    if(inputOp->getOperationType() == Operation::BasicBlockArgument) {
                        auto arg = std::static_pointer_cast<Operations::BasicBlockArgument>(inputOp);
                        if(arg->getBaseOps().size() == 1) {
                            operation->setInput(operation, arg->getBaseOps().at(0), inputIndex);
                        }
                        ++inputIndex;
                    }
                }
            // The current operation is a terminator operation (except for return). We iterate over the arguments of
            // the terminator operation, and check whether the corresponding operations of the next block are 
            // merge-arguments (more than one option for input). If not, we remove the arg. If the arg has more than
            // one option for input, we check whether we need to forward the arg of the currentBlock, or whether we 
            // forward the underlying base operation. In case of a non-argument-operation, we simply forward it.
            } else {
                for(auto& nextBlockInvocation : nextBlocks) {
                    std::vector<OperationPtr> branchOps = nextBlockInvocation->getBranchOps();
                    nextBlockInvocation->clearBranchOps();
                    for(size_t i = 0; i < branchOps.size(); ++i) {
                        if(nextBlockInvocation->getNextBlock()->getArguments().at(i)->getBaseOps().size() > 1) {
                            if(branchOps.at(i)->getOperationType() == Operation::BasicBlockArgument) {
                                auto arg = std::static_pointer_cast<Operations::BasicBlockArgument>(branchOps.at(i));
                                if(arg->getBaseOps().size() == 1) {
                                    branchOps.at(i) = arg->getBaseOps().at(0);
                                } else {
                                    branchOps.at(i) = arg;
                                }
                            }
                            nextBlockInvocation->addArgument((branchOps.at(i)));
                        }
                    }
                }
            } 
        }
        // Only keep currentBlock arguments that have more than one base operation.
        // -> (Can be fused with terminator-operation loop above, but becomes far less readable.)
        auto previousArgs = currentBlock->getArguments();
        currentBlock->clearArguments();
        for(auto& arg : previousArgs) {
            if(arg->getBaseOps().size() > 1) {
                if(!currentBlock->isLoopHeaderBlock()) {
                    arg->getBaseOps().clear();
                }
                currentBlock->addArgument(arg);
            }
        }
        for(auto& nextBlockInvocation : nextBlocks) {
            // If next block is not a merge-block, simply add it to the blocks that still need to be visited 
            // (toVisitBlocks). Else, check if the merge-block was already visited. If so, check if all merge-edges
            // have already been visited (merge-block is not open). If all merge-edges have been visited, add 
            // the merge-block's child nodes to the toVisitBlocks.
            // If a merge-block has not been visited yet, add it to the merge block list.
            if(!nextBlockInvocation->getNextBlock()->isMergeBlock()) {
                if(!visitedBlocks.contains(nextBlockInvocation->getNextBlock().get())) {
                    toVisitBlocks.emplace(nextBlockInvocation->getNextBlock());
                }
            } else {
                if(mergeBlocks.contains(nextBlockInvocation->getNextBlock().get())) {
                    // We found a known merge-block. We need to check whether we just traversed the last unused
                    // incoming edge to reach the merge-block. If so, continue by following the control flow 'below'
                    // the merge-block, else we increase the number of visits for the merge-block by 1.
                    bool isOpenMergeBlock = mergeBlocks[nextBlockInvocation->getNextBlock().get()] < 
                        nextBlockInvocation->getNextBlock()->getPredecessors().size() 
                        - nextBlockInvocation->getNextBlock()->getNumLoopBackEdges() - 1;
                    if(isOpenMergeBlock) {
                        mergeBlocks[nextBlockInvocation->getNextBlock().get()]++;
                    } else if(!visitedBlocks.contains(nextBlockInvocation->getNextBlock().get())) {
                        toVisitBlocks.emplace(nextBlockInvocation->getNextBlock());
                    }
                } else {
                    // We found a new merge-block.
                    mergeBlocks.emplace(std::make_pair(nextBlockInvocation->getNextBlock().get(), 1));
                }
            }
        }
    } while (!toVisitBlocks.empty());
}


}//namespace NES::Nautilus::IR