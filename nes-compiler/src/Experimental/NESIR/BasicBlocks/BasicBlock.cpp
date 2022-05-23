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

#include "Experimental/NESIR/Operations/BranchOperation.hpp"
#include "Experimental/NESIR/Operations/IfOperation.hpp"
#include "Experimental/NESIR/Operations/LoopOperation.hpp"
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <cstdint>
#include <memory>
#include <utility>

namespace NES {
BasicBlock::BasicBlock(std::string identifier, std::vector<OperationPtr> operations, std::vector<std::string> inputArgs, 
                       int32_t scopeLevel)
    : identifier(std::move(identifier)), operations(std::move(operations)), inputArgs(std::move(inputArgs)), 
      parentBlockLevel(scopeLevel) {}

std::string BasicBlock::getIdentifier() { return identifier; }

std::vector<OperationPtr> BasicBlock::getOperations() { return operations; }

std::vector<std::string> BasicBlock::getInputArgs() { return inputArgs; }
int32_t BasicBlock::getParentBlockLevel() { return parentBlockLevel; }

std::shared_ptr<BasicBlock> BasicBlock::addOperation(OperationPtr operation) { operations.push_back(operation); return shared_from_this(); }
void BasicBlock::popOperation() { operations.pop_back(); }

// NESIR Assembly
std::shared_ptr<BasicBlock> BasicBlock::addLoopTerminatorOpHeadBlock(BasicBlockPtr loopHeadBlock) {
    std::static_pointer_cast<LoopOperation>(this->operations.back())->setLoopHeadBlock(loopHeadBlock);
    return shared_from_this();
}
std::shared_ptr<BasicBlock> BasicBlock::addBranchTerminatorOpNextBlock(std::shared_ptr<BasicBlock> nextBlock) {
    std::static_pointer_cast<BranchOperation>(this->operations.back())->setNextBlock(nextBlock);
    return shared_from_this();
}
std::shared_ptr<BasicBlock> BasicBlock::addIfTerminatorOpThenBlock(std::shared_ptr<BasicBlock> thenBlock) {
    std::static_pointer_cast<IfOperation>(this->operations.back())->setThenBranchBlock(thenBlock);
    return shared_from_this();
}
std::shared_ptr<BasicBlock> BasicBlock::addIfTerminatorOpElseBlock(std::shared_ptr<BasicBlock> elseBlock) {
    std::static_pointer_cast<IfOperation>(this->operations.back())->setElseBranchBlock(elseBlock);
    return shared_from_this();
}

}// namespace NES