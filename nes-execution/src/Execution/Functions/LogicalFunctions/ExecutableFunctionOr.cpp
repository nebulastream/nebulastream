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
#include <Execution/Functions/Function.hpp>
#include <Execution/Functions/LogicalFunctions/ExecutableFunctionOr.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/Execution.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <ErrorHandling.hpp>

namespace NES::Runtime::Execution::Functions
{

VarVal ExecutableFunctionOr::execute(Record& record) const
{
    const auto leftValue = leftExecutableFunction->execute(record);
    const auto rightValue = rightExecutableFunction->execute(record);
    return leftValue || rightValue;
}

ExecutableFunctionOr::ExecutableFunctionOr(
    std::unique_ptr<Function> leftExecutableFunctionSub, std::unique_ptr<Function> rightExecutableFunctionSub)
    : leftExecutableFunction(std::move(leftExecutableFunctionSub)), rightExecutableFunction(std::move(rightExecutableFunctionSub))
{
}

std::unique_ptr<Function> RegisterExecutableFunctionOr(std::vector<std::unique_ptr<Functions::Function>> childFunctions)
{
    PRECONDITION(childFunctions.size() == 2, "Or function must have exactly two sub-functions");
    return std::make_unique<ExecutableFunctionOr>(std::move(childFunctions[0]), std::move(childFunctions[1]));
}

}
