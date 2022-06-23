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
CompareOperation::CompareOperation(std::string identifier, std::string leftArgName, std::string rightArgName, 
                                    Comparator comparator) 
    : Operation(Operation::CompareOp, BOOLEAN), identifier(std::move(identifier)), leftArgName(std::move(leftArgName)),
      rightArgName(std::move(rightArgName)), comparator(comparator) {}

std::string CompareOperation::getIdentifier() { return identifier; }
std::string CompareOperation::getLeftArgName() { return leftArgName; }
std::string CompareOperation::getRightArgName() { return rightArgName; }
CompareOperation::Comparator CompareOperation::getComparator() { return comparator; }

std::string CompareOperation::toString() { 
    return "CompareOperation_" + identifier + "(" + leftArgName + ", " + rightArgName + ")";
}

}// namespace NES
