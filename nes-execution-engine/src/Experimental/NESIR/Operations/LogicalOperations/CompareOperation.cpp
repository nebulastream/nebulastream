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

#include <Experimental/NESIR/Operations/LogicalOperations/CompareOperation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
CompareOperation::CompareOperation(OperationIdentifier identifier,
                                   OperationPtr leftInput,
                                   OperationPtr rightInput,
                                   Comparator comparator)
    : Operation(Operation::CompareOp,identifier, BOOLEAN), leftInput(std::move(leftInput)),
      rightInput(std::move(rightInput)), comparator(comparator) {}

OperationPtr CompareOperation::getLeftInput() { return leftInput.lock(); }
OperationPtr CompareOperation::getRightInput() { return rightInput.lock(); }
CompareOperation::Comparator CompareOperation::getComparator() { return comparator; }

std::string CompareOperation::toString() {
    return "CompareOperation_" + identifier + "(" + getLeftInput()->getIdentifier() + ", " + getRightInput()->getIdentifier() + ")";
}

}// namespace NES::ExecutionEngine::Experimental::IR::Operations
