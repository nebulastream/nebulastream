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

#include <Experimental/NESIR/Operations/ArithmeticOperations/AddFloatOperation.hpp>
namespace NES::ExecutionEngine::Experimental::IR::Operations {
AddFloatOperation::AddFloatOperation(std::string identifier, std::string leftArgName, std::string rightArgName)
    : Operation(OperationType::AddFloatOp), identifier(std::move(identifier)), leftArgName(std::move(leftArgName)), rightArgName(std::move(rightArgName)) {}


std::string AddFloatOperation::toString() { 
    return "AddFloatOperation_" + identifier + "(" + leftArgName + ", " + rightArgName + ")";
}
bool AddFloatOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::AddIntOp; }

std::string AddFloatOperation::getIdentifier() { return identifier; }
std::string AddFloatOperation::getLeftArgName() { return leftArgName; }
std::string AddFloatOperation::getRightArgName() { return rightArgName; }
}// namespace NES