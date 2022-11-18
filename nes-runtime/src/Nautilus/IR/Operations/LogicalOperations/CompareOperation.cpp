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
    : Operation(Operation::CompareOp, identifier, Types::StampFactory::createBooleanStamp()), leftInput(std::move(leftInput)),
      rightInput(std::move(rightInput)), comparator(comparator) {
    leftInput->addUsage(this);
    rightInput->addUsage(this);
}

OperationPtr CompareOperation::getLeftInput() { return leftInput.lock(); }
OperationPtr CompareOperation::getRightInput() { return rightInput.lock(); }
CompareOperation::Comparator CompareOperation::getComparator() { return comparator; }

std::string CompareOperation::toString() {
    std::string comperator;
    switch (comparator) {
        case IEQ: comperator = "=="; break;
        case INE: comperator = "!="; break;
        case ISLT: comperator = "<"; break;
        case ISLE: comperator = "<="; break;
        case ISGT: comperator = ">"; break;
        case ISGE: comperator = ">="; break;
        case IULT: comperator = "<"; break;
        case IULE: comperator = "<="; break;
        case IUGT: comperator = ">"; break;
        case IUGE: comperator = ">="; break;
        case FOLT: comperator = "<"; break;
        case FOLE: comperator = "<="; break;
        case FOGT: comperator = ">"; break;
        case FOGE: comperator = ">="; break;
        case FOEQ: comperator = "=="; break;
        case FONE: comperator = "!="; break;
    }

    return identifier + " = " + getLeftInput()->getIdentifier() + " " + comperator + " " + getRightInput()->getIdentifier();
}

}// namespace NES::Nautilus::IR::Operations
