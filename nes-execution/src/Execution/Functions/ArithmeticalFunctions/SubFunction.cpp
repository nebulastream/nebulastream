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
#include <Execution/Functions/ArithmeticalFunctions/SubFunction.hpp>

namespace NES::Runtime::Execution::Functions {

VarVal SubFunction::execute(Record& record) const {
    const auto leftValue = leftSubFunction->execute(record);
    const auto rightValue = rightSubFunction->execute(record);
    return leftValue - rightValue;
}
SubFunction::SubFunction(FunctionPtr leftSubFunction, FunctionPtr rightSubFunction)
    : leftSubFunction(std::move(leftSubFunction)), rightSubFunction(std::move(rightSubFunction)) {}

}// namespace NES::Runtime::Execution::Functions
