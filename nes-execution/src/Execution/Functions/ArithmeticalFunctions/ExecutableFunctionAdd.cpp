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
#include <Execution/Functions/ArithmeticalFunctions/ExecutableFunctionAdd.hpp>
#include <ErrorHandling.hpp>

namespace NES::Runtime::Execution::Functions
{

VarVal ExecutableFunctionAdd::execute(Record& record) const
{
    const auto leftValue = leftExecutableFunctionSub->execute(record);
    const auto rightValue = rightExecutableFunctionSub->execute(record);
    return leftValue + rightValue;
}

ExecutableFunctionAdd::ExecutableFunctionAdd(FunctionPtr leftExecutableFunctionSub, FunctionPtr rightExecutableFunctionSub)
    : leftExecutableFunctionSub(std::move(leftExecutableFunctionSub)), rightExecutableFunctionSub(std::move(rightExecutableFunctionSub))
{
}

FunctionPtr RegisterExecutableFunctionAdd(std::vector<FunctionPtr> subFunctions)
{
    PRECONDITION(subFunctions.size() == 2, "Add function must have exactly two sub-functions");
    return std::make_unique<ExecutableFunctionAdd>(std::move(subFunctions[0]), std::move(subFunctions[1]));
}

}
