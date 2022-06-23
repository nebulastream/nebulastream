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

#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LoopOperation.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <cstdint>
#include <memory>
#include <utility>

namespace NES::ExecutionEngine::Experimental::IR {
BasicBlock::BasicBlock(std::string identifier, int32_t scopeLevel, std::vector<Operations::OperationPtr> operations, std::vector<std::string> inputArgs,
                       std::vector<Operations::PrimitiveStamp> inputArgTypes)
    : identifier(std::move(identifier)), scopeLevel(scopeLevel), operations(std::move(operations)), inputArgs(std::move(inputArgs)), 
      inputArgTypes(std::move(inputArgTypes)) {}

std::string BasicBlock::getIdentifier() { return identifier; }
int32_t BasicBlock::getScopeLevel() { return scopeLevel; }
std::vector<Operations::OperationPtr> BasicBlock::getOperations() { return operations; }
Operations::OperationPtr BasicBlock::getTerminatorOp() { return operations.back(); }
std::vector<std::string> BasicBlock::getInputArgs() { return inputArgs; }
std::vector<Operations::PrimitiveStamp> BasicBlock::getInputArgTypes() { return inputArgTypes; }

void BasicBlock::popOperation() { operations.pop_back(); }

// NESIR Assembly
std::shared_ptr<BasicBlock> BasicBlock::addOperation(Operations::OperationPtr operation) { operations.push_back(operation); return shared_from_this(); }
std::shared_ptr<BasicBlock> BasicBlock::addLoopHeadBlock(BasicBlockPtr loopHeadBlock) {
    std::static_pointer_cast<Operations::LoopOperation>(this->operations.back())->setLoopHeadBlock(loopHeadBlock);
    return shared_from_this();
}
std::shared_ptr<BasicBlock> BasicBlock::addNextBlock(std::shared_ptr<BasicBlock> nextBlock) {
    std::static_pointer_cast<Operations::BranchOperation>(this->operations.back())->setNextBlock(nextBlock);
    return shared_from_this();
}
std::shared_ptr<BasicBlock> BasicBlock::addThenBlock(std::shared_ptr<BasicBlock> thenBlock) {
    std::static_pointer_cast<Operations::IfOperation>(this->operations.back())->setThenBranchBlock(thenBlock);
    return shared_from_this();
}
std::shared_ptr<BasicBlock> BasicBlock::addElseBlock(std::shared_ptr<BasicBlock> elseBlock) {
    std::static_pointer_cast<Operations::IfOperation>(this->operations.back())->setElseBranchBlock(elseBlock);
    return shared_from_this();
}

}// namespace NES