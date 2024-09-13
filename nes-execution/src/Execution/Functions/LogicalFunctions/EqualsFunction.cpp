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

#include <Execution/Functions/LogicalFunctions/EqualsFunction.hpp>
#include <ErrorHandling.hpp>
namespace NES::Runtime::Execution::Functions
{

EqualsFunction::EqualsFunction(FunctionPtr leftSubFunction, FunctionPtr rightSubFunction)
    : leftSubFunction(std::move(leftSubFunction)), rightSubFunction(std::move(rightSubFunction))
{
}

VarVal EqualsFunction::execute(Record& record) const
{
    const auto leftValue = leftSubFunction->execute(record);
    const auto rightValue = rightSubFunction->execute(record);
    return leftValue == rightValue;
}

FunctionPtr RegisterEqualsFunction(std::vector<FunctionPtr> subFunctions)
{
    PRECONDITION(subFunctions.size() == 2, "Equals function must have exactly two sub-functions");
    return std::make_unique<EqualsFunction>(std::move(subFunctions[0]), std::move(subFunctions[1]));
}

}