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
#include <Nautilus/IR/Phases/RemoveBrOnlyBlocksPhase.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <unordered_map>

namespace NES::Nautilus::IR {

void RemoveBrOnlyBlocksPhase::apply(std::shared_ptr<IR::IRGraph> ir) {
    auto phaseContext = RemoveBrOnlyBlocksPhaseContext(std::move(ir));
    phaseContext.process();
};

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::process() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    addPredecessors(rootOperation->getFunctionBasicBlock());
    removeBrOnlyBlocks(rootOperation->getFunctionBasicBlock());
    NES_TRACE("{}", ir->toString());
}

void inline addPredecessorToBlock(IR::BasicBlockPtr currentBlock,
                                  std::stack<IR::BasicBlockPtr>& candidates,
                                  std::unordered_set<std::string> newBlocks) {
    // Add the current block to the predecessors af the next block or true- and false blocks, in case of an if.
    auto terminatorOp = currentBlock->getTerminatorOp();
    if (terminatorOp->getOperationType() == Operations::Operation::OperationType::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
        branchOp->getNextBlockInvocation().getBlock()->addPredecessor(currentBlock);
        if (!newBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace(branchOp->getNextBlockInvocation().getBlock());
        }
    } else if (terminatorOp->getOperationType() == Operations::Operation::OperationType::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
        ifOp->getFalseBlockInvocation().getBlock()->addPredecessor(currentBlock);
        ifOp->getTrueBlockInvocation().getBlock()->addPredecessor(currentBlock);
        if (!newBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace(ifOp->getFalseBlockInvocation().getBlock());
        }
        if (!newBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) {
            candidates.emplace(ifOp->getTrueBlockInvocation().getBlock());
        }
    }
}

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::addPredecessors(IR::BasicBlockPtr currentBlock) {
    std::stack<IR::BasicBlockPtr> newBlocks;
    std::unordered_set<std::string> visitedBlocks;

    // Iterate over all blocks/nodes of the query graph, until all blocks have been visited.
    newBlocks.emplace(currentBlock);
    do {
        visitedBlocks.emplace(currentBlock->getIdentifier());
        currentBlock = newBlocks.top();
        newBlocks.pop();
        addPredecessorToBlock(currentBlock, newBlocks, visitedBlocks);
    } while (!newBlocks.empty());
}

void updatePredecessorBlocks(std::vector<IR::BasicBlockPtr>& brOnlyBlocks, const IR::BasicBlockPtr& nonBrOnlyBlock) {
    // We refer to the block that comes after the br-only-block-chain and is not a br-only-block as non-br-only-block.
    // newBlocks is used to remember all blocks that must become predecessors of the non-br-only-block.
    // removedBlock is used to remember all br-only-blocks that need to be removed as predecessors from the non-br-only-block.
    std::vector<std::weak_ptr<IR::BasicBlock>> newPredecessorBlocks;
    std::unordered_map<std::string, IR::BasicBlockPtr> removedBlocks;
    // Iterate over all passed br-only-blocks and set the non-br-only-block as new target block of predecessors.
    // Also, update all br- and if-operations to point to the non-br-block.
    for (const auto& brOnlyBlock : brOnlyBlocks) {
        removedBlocks.emplace(std::make_pair(brOnlyBlock->getIdentifier(), brOnlyBlock));
        for (const auto& predecessor : brOnlyBlock->getPredecessors()) {
            if (!removedBlocks.contains(predecessor.lock()->getIdentifier())) {
                newPredecessorBlocks.emplace_back(predecessor);
            }
            auto terminatorOp = predecessor.lock()->getTerminatorOp();
            // If the predecessor has an if-operation as its terminator-operation, first check which branch points to
            // the brOnlyBlock. If it is the true-branch-block, simply add the non-br-only-block as the new true-branch-block.
            // If it is the false-branch-block, add it as the new false-branch-block, and check whether the true-branch-block
            // is the same. If it is, we found an empty if-else case and can replace the if-operation with a branch-operation.
            // If the predecessor is a branch-operation, simply set the non-branch-block as the new next-block.
            if (terminatorOp->getOperationType() == Operations::Operation::OperationType::IfOp) {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                if (ifOp->getTrueBlockInvocation().getBlock()->getIdentifier() == brOnlyBlock->getIdentifier()) {
                    ifOp->getTrueBlockInvocation().setBlock(nonBrOnlyBlock);
                } else {
                    ifOp->getFalseBlockInvocation().setBlock(nonBrOnlyBlock);
                    // Check if control flow along the true- and false-branch now lead to the same block.
                    if (ifOp->getTrueBlockInvocation().getBlock()->getIdentifier() == nonBrOnlyBlock->getIdentifier()) {
                        newPredecessorBlocks.pop_back();
                        predecessor.lock()->removeOperation(predecessor.lock()->getTerminatorOp());
                        auto newBranchOperation = std::make_shared<Operations::BranchOperation>();
                        newBranchOperation->getNextBlockInvocation().setBlock(nonBrOnlyBlock);
                        for (const auto& arg : ifOp->getFalseBlockInvocation().getArguments()) {
                            newBranchOperation->getNextBlockInvocation().addArgument(arg);
                        }
                        predecessor.lock()->addOperation(std::move(newBranchOperation));
                    }
                }
            } else if (terminatorOp->getOperationType() == Operations::Operation::OperationType::BranchOp) {
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                branchOp->getNextBlockInvocation().setBlock(nonBrOnlyBlock);
            } else {
                NES_ERROR("RemoveBrOnlyBlocksPhase::updateTerminatorOperation: Case not implemented: {}",
                          magic_enum::enum_name(terminatorOp->getOperationType()));
                NES_NOT_IMPLEMENTED();
            }
        }
    }
    // Add all predecessors of the non-branch-only-block that are NOT branch-only-blocks to the list of new predecessors.
    for (const auto& predecessor : nonBrOnlyBlock->getPredecessors()) {
        if (!removedBlocks.contains(predecessor.lock()->getIdentifier())) {
            newPredecessorBlocks.emplace_back(predecessor);
        }
    }
    // Drop the current predecessor list of the non-br-only-block and fill it with all new-predecessor blocks.
    nonBrOnlyBlock->getPredecessors().clear();
    for (const auto& newPredecessor : newPredecessorBlocks) {
        nonBrOnlyBlock->addPredecessor(newPredecessor.lock());
    }
}

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::processPotentialBrOnlyBlock(
    IR::BasicBlockPtr currentBlock,
    std::stack<IR::BasicBlockPtr>& newBlocks,
    std::unordered_set<std::string> visitedBlocks) {
    // We are searching for branch-only-blocks. If the currentBlock has a different terminator operation, simply
    // add the next-block(s) to the newBlocks and proceed.
    auto terminatorOp = currentBlock->getTerminatorOp();
    if (terminatorOp->getOperationType() == Operations::Operation::OperationType::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
        // If we have a block with a branch operation, check whether it is the only operation in that block.
        if (currentBlock->getOperations().size() == 1) {
            if (currentBlock->getArguments().size() != branchOp->getNextBlockInvocation().getArguments().size()) {
                // TODO handle modification of arguments correctly. Currently, the algorithm can't handle renamed arguments.
                if (!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
                    newBlocks.emplace(branchOp->getNextBlockInvocation().getBlock());
                }
                return;
            }

            // We found a branch-only-block. Now check whether subsequent blocks are also branch-only-blocks.
            // In case we find a branch-only-block-chain, we remove the entire chain (brOnlyBlocks) in one go.
            std::vector<IR::BasicBlockPtr> brOnlyBlocks;
            while (currentBlock->getOperations().size() == 1
                   && currentBlock->getTerminatorOp()->getOperationType() == Operations::Operation::OperationType::BranchOp) {
                brOnlyBlocks.emplace_back(currentBlock);
                visitedBlocks.emplace(currentBlock->getIdentifier());// put every visited br only block in visitedBlocks
                branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(currentBlock->getTerminatorOp());
                currentBlock = branchOp->getNextBlockInvocation().getBlock();
            }
            // brOnlyBlocks now contains a br-only-block-chain of at least one br-only-block.
            // currentBlock now is the first block after the br-only-block-chain that is not a br-only-block.
            // We call updatePredecessorBlocks to update the next-block(s) and predecessor-blocks of all blocks that
            // had next-blocks or predecessor-blocks that are part of the br-only-block-chain (brOnlyBlocks)
            updatePredecessorBlocks(brOnlyBlocks, currentBlock);
            if (!visitedBlocks.contains(currentBlock->getIdentifier())) {
                newBlocks.emplace(currentBlock);
            }
        } else {
            if (!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
                newBlocks.emplace(branchOp->getNextBlockInvocation().getBlock());
            }
        }
    } else if (terminatorOp->getOperationType() == Operations::Operation::OperationType::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
        if (!visitedBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) {
            newBlocks.emplace(ifOp->getFalseBlockInvocation().getBlock());
        }
        if (!visitedBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) {
            newBlocks.emplace(ifOp->getTrueBlockInvocation().getBlock());
        }
    }
}

void RemoveBrOnlyBlocksPhase::RemoveBrOnlyBlocksPhaseContext::removeBrOnlyBlocks(IR::BasicBlockPtr currentBlock) {
    std::stack<IR::BasicBlockPtr> newBlocks;
    std::unordered_set<std::string> visitedBlocks;

    // Iterate over all blocks/nodes of the query graph, until all blocks have been visited.
    newBlocks.emplace(currentBlock);
    do {
        visitedBlocks.emplace(currentBlock->getIdentifier());
        currentBlock = newBlocks.top();
        newBlocks.pop();
        processPotentialBrOnlyBlock(currentBlock, newBlocks, visitedBlocks);
    } while (!newBlocks.empty());
}
}//namespace NES::Nautilus::IR