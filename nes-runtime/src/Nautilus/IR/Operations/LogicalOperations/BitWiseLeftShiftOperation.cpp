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

#include <Nautilus/IR/Operations/LogicalOperations/BitWiseLeftShiftOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
namespace NES::Nautilus::IR::Operations {

BitWiseLeftShiftOperation::BitWiseLeftShiftOperation(OperationIdentifier identifier,
                                                     OperationPtr leftInput,
                                                     OperationPtr rightInput)
    : Operation(OperationType::BitWiseLeftShift, identifier, leftInput->getStamp()), leftInput(std::move(leftInput)),
      rightInput(std::move(rightInput)) {
    leftInput->addUsage(this);
    rightInput->addUsage(this);
}

std::string BitWiseLeftShiftOperation::toString() {
    return getIdentifier() + " = " + getLeftInput()->getIdentifier() + " << " + getRightInput()->getIdentifier();
}
bool BitWiseLeftShiftOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::BitWiseLeftShift; }

OperationPtr BitWiseLeftShiftOperation::getLeftInput() { return leftInput.lock(); }
OperationPtr BitWiseLeftShiftOperation::getRightInput() { return rightInput.lock(); }
}// namespace NES::Nautilus::IR::Operations