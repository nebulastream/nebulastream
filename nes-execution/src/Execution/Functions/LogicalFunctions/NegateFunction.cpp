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
#include <ErrorHandling.hpp>
#include <Execution/Functions/LogicalFunctions/ExecutableFunctionNegate.hpp>

namespace NES::Runtime::Execution::Functions {

ExecutableFunctionNegate::ExecutableFunctionNegate(FunctionPtr subFunction)
    : subFunction(std::move(subFunction)) {}

VarVal ExecutableFunctionNegate::execute(Record& record) const
{
    const auto value = subFunction->execute(record);
    return !value;
}

FunctionPtr RegisterExecutableFunctionNegate(std::vector<FunctionPtr> subFunctions)
{
    PRECONDITION(subFunctions.size() == 1, "Negate function must have exactly one sub-function");
    return std::make_unique<ExecutableFunctionNegate>(std::move(subFunctions[0]));
}

}