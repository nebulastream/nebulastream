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
#include <memory>
#include <utility>
#include <vector>
#include <Execution/Functions/ArithmeticalFunctions/ExecutableFunctionMul.hpp>
#include <ErrorHandling.hpp>

namespace NES::Runtime::Execution::Functions
{

VarVal ExecutableFunctionMul::execute(Record& record) const
{
    const auto leftValue = leftExecutableFunctionSub->execute(record);
    const auto rightValue = rightExecutableFunctionSub->execute(record);
    return leftValue * rightValue;
}

ExecutableFunctionMul::ExecutableFunctionMul(
    std::unique_ptr<Function> leftExecutableFunctionSub, std::unique_ptr<Function> rightExecutableFunctionSub)
    : leftExecutableFunctionSub(std::move(leftExecutableFunctionSub)), rightExecutableFunctionSub(std::move(rightExecutableFunctionSub))
{
}

std::unique_ptr<Function> RegisterExecutableFunctionMul(std::vector<std::unique_ptr<Functions::Function>> childFunctions)
{
    PRECONDITION(subFunctions.size() == 2, "Mul function must have exactly two sub-functions");
    return std::make_unique<ExecutableFunctionMul>(std::move(subFunctions[0]), std::move(subFunctions[1]));
}

}
