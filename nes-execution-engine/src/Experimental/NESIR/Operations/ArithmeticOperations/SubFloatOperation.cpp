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

#include <Experimental/NESIR/Operations/ArithmeticOperations/SubFloatOperation.hpp>
#include <string>
namespace NES::ExecutionEngine::Experimental::IR::Operations {
SubFloatOperation::SubFloatOperation(std::string identifier, std::string leftArgName, std::string rightArgName)
    : Operation(OperationType::SubFloatOp), identifier(std::move(identifier)), leftArgName(std::move(leftArgName)), 
                rightArgName(std::move(rightArgName)) {}


std::string SubFloatOperation::toString() { 
    return "SubFloatOperation_" + identifier + "(" + leftArgName + ", " + rightArgName + ")";
}
bool SubFloatOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::SubFloatOp; }

std::string SubFloatOperation::getIdentifier() { return identifier; }
std::string SubFloatOperation::getLeftArgName() { return leftArgName; }
std::string SubFloatOperation::getRightArgName() { return rightArgName; }
}// namespace NES