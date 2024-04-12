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

#include "gsl/pointers.hpp"
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlockArgument.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace NES::Nautilus::IR {
BasicBlock::BasicBlock(std::string identifier,
                       int32_t scopeLevel,
                       std::vector<Operations::OperationPtr> operations,
                       std::vector<std::unique_ptr<Operations::BasicBlockArgument>> arguments)
    : identifier(std::move(identifier)), scopeLevel(scopeLevel), numLoopBackEdges(0), operations(std::move(operations)),
      arguments(std::move(arguments)) {}

std::string BasicBlock::getIdentifier() const { return identifier; }
void BasicBlock::setIdentifier(const std::string& identifier) { this->identifier = identifier; }
uint32_t BasicBlock::getScopeLevel() const { return scopeLevel; }
void BasicBlock::setScopeLevel(uint32_t scopeLevel) { this->scopeLevel = scopeLevel; }
uint32_t BasicBlock::getNumLoopBackEdges() const { return numLoopBackEdges; }
void BasicBlock::incrementNumLoopBackEdge() { ++this->numLoopBackEdges; }
bool BasicBlock::isLoopHeaderBlock() const { return numLoopBackEdges > 0; }
std::vector<Operations::OperationRef> BasicBlock::getOperations() const {
    std::vector<Operations::OperationRef> refs;
    for (auto& basic_block_argument : arguments) {
        refs.emplace_back(basic_block_argument.get());
    }
    return refs;
}
Operations::Operation& BasicBlock::getOperationAt(size_t index) const { return *operations.at(index); }
Operations::Operation& BasicBlock::getTerminatorOp() const { return *operations.back(); }
std::vector<Operations::OperationRef> BasicBlock::getArguments() const {
    std::vector<Operations::OperationRef> refs;
    for (auto& basic_block_argument : arguments) {
        refs.emplace_back(basic_block_argument.get());
    }
    return refs;
}
ssize_t BasicBlock::getIndexOfArgument(const Operations::BasicBlockArgument& arg) const {
    for (uint64_t i = 0; i < arguments.size(); i++) {
        if (std::addressof(*arguments[i]) == std::addressof(arg)) {
            return i;
        }
    }
    return -1;
}

// void BasicBlock::popOperation() { operations.pop_back(); }
void BasicBlock::replaceTerminatorOperation(Operations::OperationPtr loopOperation) {
    operations.pop_back();
    operations.emplace_back(std::move(loopOperation));
}

// NESIR Assembly
BasicBlock& BasicBlock::addOperation(Operations::OperationPtr&& operation) {
    operations.emplace_back(std::move(operation));
    return *this;
}
BasicBlock& BasicBlock::addLoopHeadBlock(BasicBlock& loopHeadBlock) {
    dynamic_cast<Operations::LoopOperation&>(*this->operations.back()).getLoopHeadBlock().setBlock(loopHeadBlock);
    return *this;
}
BasicBlock& BasicBlock::addNextBlock(BasicBlock& nextBlock) {
    dynamic_cast<Operations::BranchOperation&>(*this->operations.back()).getNextBlockInvocation().setBlock(nextBlock);
    return *this;
}
BasicBlock& BasicBlock::addTrueBlock(BasicBlock& thenBlock) {
    dynamic_cast<Operations::IfOperation&>(*this->operations.back()).getTrueBlockInvocation().setBlock(thenBlock);
    return *this;
}
BasicBlock& BasicBlock::addFalseBlock(BasicBlock& elseBlock) {
    dynamic_cast<Operations::IfOperation&>(*this->operations.back()).getFalseBlockInvocation().setBlock(elseBlock);
    return *this;
}

void BasicBlock::addPredecessor(BasicBlock& predecessor) { this->predecessors.emplace_back(&predecessor); }
const std::vector<gsl::not_null<BasicBlock*>>& BasicBlock::getPredecessors() const { return predecessors; }
std::vector<gsl::not_null<BasicBlock*>>& BasicBlock::getPredecessors() { return predecessors; }
void BasicBlock::addNextBlock(BasicBlock& nextBlock, const std::vector<Operations::OperationRef>& ops) {
    auto branchOp = std::make_unique<Operations::BranchOperation>();
    auto& nextBlockIn = branchOp->getNextBlockInvocation();
    nextBlockIn.setBlock(nextBlock);
    for (auto op : ops) {
        nextBlockIn.addArgument(*op);
    }
    addOperation(std::move(branchOp));
    // add this block as a predecessor to the next block
    //Todo #3167 : can we use this to replace the addPredecessor pass? (also: addTrueBlock, and addFalseBlock)
    // nextBlock->addPredecessor(shared_from_this());
}
Operations::OperationPtr BasicBlock::removeOperation(Operations::Operation& operation) {
    auto it = std::find(operations.begin(), operations.end(), operation);
    if (it == operations.end()) {
        return nullptr;
    }

    auto ownedOperation = std::move(*it);
    operations.erase(it);

    return std::move(ownedOperation);
}
void BasicBlock::addOperationBefore(const Operations::Operation& before, Operations::OperationPtr&& operation) {
    auto position = std::find(operations.begin(), operations.end(), before);
    operations.insert(position, std::move(operation));
}

[[nodiscard]] std::pair<BasicBlock*, BasicBlock*> BasicBlock::getNextBlocks() const {
    // std::pair<std::unique_ptr<BasicBlock>, std::unique_ptr<BasicBlock>> nextBlocks;
    if (operations.back()->getOperationType() == IR::Operations::Operation::OperationType::BranchOp) {
        auto& branchOp = dynamic_cast<IR::Operations::BranchOperation&>(*operations.back());
        return std::make_pair(branchOp.getNextBlockInvocation().getBlock(), nullptr);
    } else if (operations.back()->getOperationType() == IR::Operations::Operation::OperationType::IfOp) {
        auto& ifOp = dynamic_cast<IR::Operations::IfOperation&>(*operations.back());
        return std::make_pair(ifOp.getTrueBlockInvocation().getBlock(), ifOp.getFalseBlockInvocation().getBlock());
    } else if (operations.back()->getOperationType() == IR::Operations::Operation::OperationType::LoopOp) {
        auto& loopOp = dynamic_cast<IR::Operations::LoopOperation&>(*operations.back());
        return std::make_pair(loopOp.getLoopBodyBlock().getBlock(), loopOp.getLoopFalseBlock().getBlock());
    } else if (operations.back()->getOperationType() == IR::Operations::Operation::OperationType::ReturnOp) {
        return {};
    } else {
        NES_ERROR("BasicBlock::getNextBlocks: Tried to get next block for unsupported operation type: {}",
                  magic_enum::enum_name(operations.back()->getOperationType()));
        NES_NOT_IMPLEMENTED();
    }
}

}// namespace NES::Nautilus::IR