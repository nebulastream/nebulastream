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

#include <Experimental/NESIR/Operations/MulOperation.hpp>
namespace NES::ExecutionEngine::Experimental::IR::Operations {
MulOperation::MulOperation(std::string identifier, std::string leftArgName, std::string rightArgName)
    : Operation(OperationType::MulOp), identifier(std::move(identifier)), leftArgName(std::move(leftArgName)), rightArgName(std::move(rightArgName)) {}


std::string MulOperation::toString() {
    return identifier + " = Mul(" + leftArgName + ", " + rightArgName + ")";
}
bool MulOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::AddOp; }

std::string MulOperation::getIdentifier() { return identifier; }
std::string MulOperation::getLeftArgName() { return leftArgName; }
std::string MulOperation::getRightArgName() { return rightArgName; }
}// namespace NES