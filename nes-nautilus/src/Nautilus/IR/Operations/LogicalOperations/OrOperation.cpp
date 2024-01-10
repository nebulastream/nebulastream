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

#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
namespace NES::Nautilus::IR::Operations {

OrOperation::OrOperation(OperationIdentifier identifier, OperationPtr leftInput, OperationPtr rightInput)
    : Operation(OperationType::OrOp, identifier, Types::StampFactory::createBooleanStamp()), leftInput(std::move(leftInput)),
      rightInput(std::move(rightInput)) {
    leftInput->addUsage(this);
    rightInput->addUsage(this);
}

std::string OrOperation::toString() {
    return getIdentifier() + " = " + getLeftInput()->getIdentifier() + " or " + getRightInput()->getIdentifier();
    ;
}
bool OrOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::AddOp; }
OperationPtr OrOperation::getLeftInput() { return leftInput.lock(); }
OperationPtr OrOperation::getRightInput() { return rightInput.lock(); }
void OrOperation::setLeftInput(OperationWPtr newLeftInput) {
    this->leftInput = newLeftInput;
}
void OrOperation::setRightInput(OperationWPtr newRightInput) {
    this->rightInput = newRightInput;
}
}// namespace NES::Nautilus::IR::Operations