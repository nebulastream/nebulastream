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
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>

namespace NES::Nautilus::IR::Operations {
IfOperation::IfOperation(OperationPtr booleanValue)
    : Operation(Operation::IfOp, Types::StampFactory::createVoidStamp()), booleanValue(booleanValue) {}

OperationPtr IfOperation::getValue() { return booleanValue.lock(); }

BasicBlockInvocation& IfOperation::getTrueBlockInvocation() { return trueBlockInvocation; }
BasicBlockInvocation& IfOperation::getFalseBlockInvocation() { return falseBlockInvocation; }

void IfOperation::setTrueBlockInvocation(BasicBlockPtr trueBlockInvocation) {
    this->trueBlockInvocation.setBlock(trueBlockInvocation);
}
void IfOperation::setFalseBlockInvocation(BasicBlockPtr falseBlockInvocation) {
    this->falseBlockInvocation.setBlock(falseBlockInvocation);
}

BasicBlockPtr IfOperation::getMergeBlock() { return mergeBlock.lock(); }
OperationPtr IfOperation::getBooleanValue() { return booleanValue.lock(); }
void IfOperation::setMergeBlock(BasicBlockPtr mergeBlock) { this->mergeBlock = mergeBlock; }
void IfOperation::setCountedLoopInfo(std::unique_ptr<CountedLoopInfo> countedLoopInfo) { this->countedLoopInfo = std::move(countedLoopInfo);}
std::unique_ptr<CountedLoopInfo> IfOperation::getCountedLoopInfo() { return std::move(countedLoopInfo);}

std::string IfOperation::toString() {
    std::string baseString = "if " + getValue()->getIdentifier() + " ? b" + trueBlockInvocation.getNextBlock()->getIdentifier() + '(';
    if (trueBlockInvocation.getBranchOps().size() > 0) {
        baseString += trueBlockInvocation.getBranchOps().at(0)->getIdentifier();
        for (int i = 1; i < (int) trueBlockInvocation.getBranchOps().size(); ++i) {
            baseString += ", " + trueBlockInvocation.getBranchOps().at(i)->getIdentifier();
        }
    }
    if (falseBlockInvocation.getNextBlock()) {
        baseString += ") : b" + falseBlockInvocation.getNextBlock()->getIdentifier() + '(';
        if (falseBlockInvocation.getBranchOps().size() > 0) {
            baseString += falseBlockInvocation.getBranchOps().at(0)->getIdentifier();
            for (int i = 1; i < (int) falseBlockInvocation.getBranchOps().size(); ++i) {
                baseString += ", " + falseBlockInvocation.getBranchOps().at(i)->getIdentifier();
            }
        }
    }
    return baseString += ')';
}
bool IfOperation::hasFalseCase() { return this->falseBlockInvocation.getNextBlock() != nullptr; }
}// namespace NES::Nautilus::IR::Operations
