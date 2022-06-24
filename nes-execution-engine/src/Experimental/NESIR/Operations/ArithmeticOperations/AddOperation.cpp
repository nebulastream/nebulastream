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

#include <Experimental/NESIR/Operations/ArithmeticOperations/AddOperation.hpp>
namespace NES::ExecutionEngine::Experimental::IR::Operations {

AddOperation::AddOperation(std::string identifier, OperationPtr leftInput, OperationPtr rightInput, PrimitiveStamp stamp)
    : Operation(OperationType::AddOp, stamp), identifier(std::move(identifier)), leftInput(std::move(leftInput)),
      rightInput(std::move(rightInput)) {}

std::string AddOperation::toString() {
    return "AddOperation_" + identifier + "(" + getLeftInput()->toString() + ", " + getRightInput()->toString() + ")";
}
bool AddOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::AddOp; }

std::string AddOperation::getIdentifier() { return identifier; }
OperationPtr AddOperation::getLeftInput() { return leftInput.lock(); }
OperationPtr AddOperation::getRightInput() { return rightInput.lock(); }
}// namespace NES::ExecutionEngine::Experimental::IR::Operations