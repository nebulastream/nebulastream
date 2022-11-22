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
#include <Nautilus/IR/Passes/RemoveBrOnlyBlocksPass.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <unordered_map>        

using namespace NES::Nautilus::IR::Operations;
namespace NES::Nautilus::IR {

std::shared_ptr<IR::IRGraph> RemoveBrOnlyBlocksPass::apply(std::shared_ptr<IR::IRGraph> ir) {
    auto phaseContext = RemoveBrOnlyBlocksPassContext(std::move(ir));
    return phaseContext.process();
};

std::shared_ptr<IR::IRGraph> RemoveBrOnlyBlocksPass::RemoveBrOnlyBlocksPassContext::process() {
    NES_DEBUG(ir->toString());
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    addPredecessors(rootOperation->getFunctionBasicBlock());
    removeBrOnlyBlocks(rootOperation->getFunctionBasicBlock());
    NES_DEBUG(ir->toString());
    return std::move(ir);
}

void inline addPredecessorToBlock(IR::BasicBlockPtr currentBlock,
                                  std::vector<IR::BasicBlockPtr>& candidates,
                                  std::unordered_set<std::string> visitedBlocks) {
    auto terminatorOp = currentBlock->getTerminatorOp();
    if (terminatorOp->getOperationType() == Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
        branchOp->getNextBlockInvocation().getBlock()->addPredecessor(currentBlock);
        if (!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace_back(branchOp->getNextBlockInvocation().getBlock());
        }
    } else if (terminatorOp->getOperationType() == Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
        ifOp->getFalseBlockInvocation().getBlock()->addPredecessor(currentBlock);
        ifOp->getTrueBlockInvocation().getBlock()->addPredecessor(currentBlock);
        if (!visitedBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace_back(ifOp->getFalseBlockInvocation().getBlock());
        }
        if (!visitedBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace_back(ifOp->getTrueBlockInvocation().getBlock());
        }
    }
}

void RemoveBrOnlyBlocksPass::RemoveBrOnlyBlocksPassContext::addPredecessors(IR::BasicBlockPtr currentBlock) {
    std::vector<IR::BasicBlockPtr> candidates;
    std::unordered_set<std::string> visitedBlocks;
    addPredecessorToBlock(currentBlock, candidates, visitedBlocks);

    while (!candidates.empty()) {
        visitedBlocks.emplace(currentBlock->getIdentifier());
        currentBlock = candidates.back();
        candidates.pop_back();
        addPredecessorToBlock(currentBlock, candidates, visitedBlocks);
    }
}

void updatePredecessorBlocks(std::vector<IR::BasicBlockPtr>& brOnlyBlocks, IR::BasicBlockPtr newBlock) {
    // Iterate over all passed br-only-blocks and set non-br-only-block as new target block of predecessors.
    std::unordered_map<std::string, IR::BasicBlockPtr> removedBlocks;//rename
    std::vector<std::weak_ptr<IR::BasicBlock>> newBlocks;            //rename
    for (auto brOnlyBlock : brOnlyBlocks) {
        removedBlocks.emplace(std::pair{brOnlyBlock->getIdentifier(), brOnlyBlock});
        for (auto predecessor : brOnlyBlock->getPredecessors()) {
            if (!removedBlocks.contains(predecessor.lock()->getIdentifier())) {
                newBlocks.emplace_back(predecessor);
            }
            auto terminatorOp = predecessor.lock()->getTerminatorOp();
            if (terminatorOp->getOperationType() == Operation::IfOp) {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                if (ifOp->getTrueBlockInvocation().getBlock()->getIdentifier() == brOnlyBlock->getIdentifier()) {
                    ifOp->getTrueBlockInvocation().setBlock(newBlock);
                } else {
                    ifOp->getFalseBlockInvocation().setBlock(newBlock);
                    // Check if control flow along the true- and false-branch now lead to the same block.
                    if (ifOp->getTrueBlockInvocation().getBlock()->getIdentifier() == newBlock->getIdentifier()) {
                        newBlocks.pop_back();
                        predecessor.lock()->removeOperation(predecessor.lock()->getTerminatorOp());
                        auto newBranchOperation = std::make_shared<BranchOperation>();
                        newBranchOperation->getNextBlockInvocation().setBlock(newBlock);
                        for (auto arg : ifOp->getFalseBlockInvocation().getArguments()) {
                            newBranchOperation->getNextBlockInvocation().addArgument(arg);
                        }
                        NES_DEBUG(newBranchOperation->toString());
                        predecessor.lock()->addOperation(std::move(newBranchOperation));
                    }
                }
            } else if (terminatorOp->getOperationType() == Operation::BranchOp) {
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                branchOp->getNextBlockInvocation().setBlock(newBlock);
            } else {
                NES_ERROR("RemoveBrOnlyBlocksPhase::updateTerminatorOperation: Case not implemented: "
                          << terminatorOp->getOperationType());
                NES_NOT_IMPLEMENTED();
            }
        }
    }
    for (auto predecessor : newBlock->getPredecessors()) {
        if (!removedBlocks.contains(predecessor.lock()->getIdentifier())) {
            newBlocks.emplace_back(predecessor);
        }
    }
    newBlock->getPredecessors().clear();
    for (auto newPredecessor : newBlocks) {
        newBlock->addPredecessor(newPredecessor.lock());
    }
}

void RemoveBrOnlyBlocksPass::RemoveBrOnlyBlocksPassContext::processPotentialBrOnlyBlock(
        IR::BasicBlockPtr currentBlock,
        std::vector<IR::BasicBlockPtr>& candidates,
        std::unordered_set<std::string> visitedBlocks) {
    auto terminatorOp = currentBlock->getTerminatorOp();
    if (terminatorOp->getOperationType() == Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
        // BRANCH ONLY CASE
        if (currentBlock->getOperations().size() == 1) {
            std::vector<IR::BasicBlockPtr> brOnlyBlocks;
            while (currentBlock->getOperations().size() == 1
                   && currentBlock->getTerminatorOp()->getOperationType() == Operation::BranchOp) {
                brOnlyBlocks.emplace_back(currentBlock);
                visitedBlocks.emplace(currentBlock->getIdentifier());// put every visited br only block in visitedBlocks
                branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(currentBlock->getTerminatorOp());
                currentBlock = branchOp->getNextBlockInvocation().getBlock();
            }
            // currentBlock is now NOT a br-only-block
            updatePredecessorBlocks(brOnlyBlocks, currentBlock);
            if (!visitedBlocks.contains(currentBlock->getIdentifier())) {
                candidates.emplace_back(currentBlock);
            }
        } else {
            if (!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
                candidates.emplace_back(branchOp->getNextBlockInvocation().getBlock());
            }
        }
    } else if (terminatorOp->getOperationType() == Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
        if (!visitedBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace_back(ifOp->getFalseBlockInvocation().getBlock());
        }
        if (!visitedBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace_back(ifOp->getTrueBlockInvocation().getBlock());
        }
    }
}

void RemoveBrOnlyBlocksPass::RemoveBrOnlyBlocksPassContext::removeBrOnlyBlocks(IR::BasicBlockPtr currentBlock) {
    std::vector<IR::BasicBlockPtr> candidates;
    std::unordered_set<std::string> visitedBlocks;
    processPotentialBrOnlyBlock(currentBlock, candidates, visitedBlocks);

    while (!candidates.empty()) {
        visitedBlocks.emplace(currentBlock->getIdentifier());
        currentBlock = candidates.back();
        candidates.pop_back();
        processPotentialBrOnlyBlock(currentBlock, candidates, visitedBlocks);
    }
}
}//namespace NES::Nautilus::Tracing