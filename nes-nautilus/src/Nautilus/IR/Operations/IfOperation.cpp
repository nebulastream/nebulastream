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
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <string>

namespace NES::Nautilus::IR::Operations {
IfOperation::IfOperation(const Operation& booleanValue)
    : Operation(Operation::OperationType::IfOp, Types::StampFactory::createVoidStamp()), booleanValue(booleanValue) {}

const Operation& IfOperation::getValue() const { return booleanValue; }

BasicBlockInvocation& IfOperation::getTrueBlockInvocation() { return trueBlockInvocation; }
const BasicBlockInvocation& IfOperation::getTrueBlockInvocation() const { return trueBlockInvocation; }
BasicBlockInvocation& IfOperation::getFalseBlockInvocation() { return falseBlockInvocation; }
const BasicBlockInvocation& IfOperation::getFalseBlockInvocation() const { return falseBlockInvocation; }

void IfOperation::setTrueBlockInvocation(BasicBlock& trueBlockInvocation) {
    this->trueBlockInvocation.setBlock(trueBlockInvocation);
}
void IfOperation::setFalseBlockInvocation(BasicBlock& falseBlockInvocation) {
    this->falseBlockInvocation.setBlock(falseBlockInvocation);
}

const BasicBlock* IfOperation::getMergeBlock() const { return mergeBlock; }
const Operation& IfOperation::getBooleanValue() const { return booleanValue; }
void IfOperation::setMergeBlock(const BasicBlock& mergeBlock) { this->mergeBlock = &mergeBlock; }

std::string IfOperation::toString() const {
    return fmt::format("if {} ? Block_{} : Block_{}", getValue().getIdentifier(), trueBlockInvocation, falseBlockInvocation);
}
bool IfOperation::hasFalseCase() const { return this->falseBlockInvocation.getBlock() != nullptr; }
}// namespace NES::Nautilus::IR::Operations
