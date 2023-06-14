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

#include <Nautilus/IR/Operations/LogicalOperations/BitWiseRightShiftOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
namespace NES::Nautilus::IR::Operations {

BitWiseRightShiftOperation::BitWiseRightShiftOperation(OperationIdentifier identifier,
                                                       OperationPtr leftInput,
                                                       OperationPtr rightInput)
    : Operation(OperationType::BitWiseRightShift, identifier, leftInput->getStamp()), leftInput(std::move(leftInput)),
      rightInput(std::move(rightInput)) {
    leftInput->addUsage(this);
    rightInput->addUsage(this);
}

std::string BitWiseRightShiftOperation::toString() {
    return getIdentifier() + " = " + getLeftInput()->getIdentifier() + " >> " + getRightInput()->getIdentifier();
}
bool BitWiseRightShiftOperation::classof(const Operation* Op) {
    return Op->getOperationType() == OperationType::BitWiseRightShift;
}

OperationPtr BitWiseRightShiftOperation::getLeftInput() { return leftInput.lock(); }
OperationPtr BitWiseRightShiftOperation::getRightInput() { return rightInput.lock(); }
}// namespace NES::Nautilus::IR::Operations