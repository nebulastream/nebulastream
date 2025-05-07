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

#pragma once
#include <Execution/Functions/Function.hpp>

namespace NES::Runtime::Execution::Functions
{

/// ToBase64s the result of the childFunction
class ExecutableFunctionImageManip final : public Function
{
public:
    explicit ExecutableFunctionImageManip(std::string functionName, std::vector<std::unique_ptr<Function>> childFunctions);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    std::string functionName;
    const std::vector<std::unique_ptr<Function>> childFunctions;
};
}
