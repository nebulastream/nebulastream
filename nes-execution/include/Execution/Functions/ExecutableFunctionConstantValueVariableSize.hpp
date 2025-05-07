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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Functions
{

/// A function that represents a constant value of variable size, e.g., a string.
class ExecutableFunctionConstantValueVariableSize final : public Function
{
public:
    explicit ExecutableFunctionConstantValueVariableSize(std::string_view value);
    explicit ExecutableFunctionConstantValueVariableSize(const int8_t* value, size_t size);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    size_t sizeIncludingLength;
    std::unique_ptr<int8_t[]> data;
};

}
