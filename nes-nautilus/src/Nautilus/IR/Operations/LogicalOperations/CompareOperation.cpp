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

#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
namespace NES::Nautilus::IR::Operations {
CompareOperation::CompareOperation(OperationIdentifier identifier,
                                   OperationPtr leftInput,
                                   OperationPtr rightInput,
                                   Comparator comparator)
    : Operation(Operation::OperationType::CompareOp, identifier, Types::StampFactory::createBooleanStamp()),
      leftInput(std::move(leftInput)), rightInput(std::move(rightInput)), comparator(comparator) {
    leftInput->addUsage(this);
    rightInput->addUsage(this);
}

OperationPtr CompareOperation::getLeftInput() { return leftInput.lock(); }
OperationPtr CompareOperation::getRightInput() { return rightInput.lock(); }
void CompareOperation::setLeftInput(OperationWPtr newLeftInput) {
    this->leftInput = newLeftInput;
}
void CompareOperation::setRightInput(OperationWPtr newRightInput) {
    this->rightInput = newRightInput;
}
CompareOperation::Comparator CompareOperation::getComparator() { return comparator; }

bool CompareOperation::isLessThan() { return (comparator == LT); }
bool CompareOperation::isLessEqual() { return (comparator == LE); }
bool CompareOperation::isGreaterThan() { return (comparator == GT); }
bool CompareOperation::isGreaterEqual() { return (comparator == GE); }
bool CompareOperation::isEquals() { return (comparator == EQ); }
bool CompareOperation::isLessThanOrGreaterThan() { return isLessThan() || isGreaterThan(); }
bool CompareOperation::isLess() { return isLessThan() || isLessEqual(); }
bool CompareOperation::isGreater() { return isGreaterThan() || isGreaterEqual(); }

std::string CompareOperation::getComparatorAsString() {
    switch (comparator) {
        case EQ: return "==";
        case NE: return "!=";
        case LT: return "<";
        case LE: return "<=";
        case GT: return ">";
        case GE: return ">=";
    }
}

std::string CompareOperation::toString() {
    return identifier + " = " + getLeftInput()->getIdentifier() + " " + getComparatorAsString() + " " + getRightInput()->getIdentifier();
}

}// namespace NES::Nautilus::IR::Operations
