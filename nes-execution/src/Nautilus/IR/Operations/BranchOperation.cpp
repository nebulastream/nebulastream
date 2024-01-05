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
#include <Nautilus/IR/Types/StampFactory.hpp>
namespace NES::Nautilus::IR::Operations {

BranchOperation::BranchOperation() : Operation(OperationType::BranchOp, Types::StampFactory::createVoidStamp()), basicBlock() {}

BasicBlockInvocation& BranchOperation::getNextBlockInvocation() { return basicBlock; }

std::string BranchOperation::toString() {
    std::string baseString = "br Block_" + basicBlock.getBlock()->getIdentifier() + "(";
    if (basicBlock.getBlock()->getArguments().size() > 0) {
        baseString += basicBlock.getArguments().at(0)->getIdentifier();
        for (int i = 1; i < (int) basicBlock.getArguments().size(); ++i) {
            baseString += ", " + basicBlock.getArguments().at(i)->getIdentifier();
        }
    }
    return baseString + ")";
}
bool BranchOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::BranchOp; }

}// namespace NES::Nautilus::IR::Operations
