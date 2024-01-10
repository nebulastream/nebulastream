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

#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
namespace NES::Nautilus::IR::Operations {

AddOperation::AddOperation(OperationIdentifier identifier, OperationPtr leftInput, OperationPtr rightInput)
    : Operation(OperationType::AddOp, identifier, leftInput->getStamp()), leftInput(std::move(leftInput)),
      rightInput(std::move(rightInput)) {
    leftInput->addUsage(this);
    rightInput->addUsage(this);
}

std::string AddOperation::toString() {
    return getIdentifier() + " = " + getLeftInput()->getIdentifier() + " + " + getRightInput()->getIdentifier();
}
bool AddOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::AddOp; }

OperationPtr AddOperation::getLeftInput() { return leftInput.lock(); }
OperationPtr AddOperation::getRightInput() { return rightInput.lock(); }
void AddOperation::setLeftInput(OperationPtr newLeftInput) {
    this->leftInput = newLeftInput;
    // newLeftInput->addUsage(this);
}
void AddOperation::setRightInput(OperationPtr newRightInput) {
    this->rightInput = newRightInput;
    // newRightInput->addUsage(this);
}
}// namespace NES::Nautilus::IR::Operations