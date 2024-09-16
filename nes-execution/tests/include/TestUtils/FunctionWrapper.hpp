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

#include <memory>
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/StdInt.hpp>
namespace NES::Runtime::Execution::Functions
{

template <typename FunctionType>
class UnaryFunctionWrapper
{
public:
    UnaryFunctionWrapper()
    {
        auto input = std::make_unique<ExecutableFunctionReadField>("value");
        function = std::make_unique<FunctionType>(std::move(input));
    }
    VarVal eval(VarVal value) const
    {
        auto record = Record({{"value", value}});
        return function->execute(record);
    }

    std::unique_ptr<FunctionType> function;
};

template <typename FunctionType>
class BinaryFunctionWrapper
{
public:
    BinaryFunctionWrapper()
    {
        auto leftFunction = std::make_unique<ExecutableFunctionReadField>("left");
        auto rightFunction = std::make_unique<ExecutableFunctionReadField>("right");
        function = std::make_unique<FunctionType>(std::move(leftFunction), std::move(rightFunction));
    }
    [[nodiscard]] VarVal eval(VarVal left, VarVal right) const
    {
        auto record = Record({{"left", left}, {"right", right}});
        return function->execute(record);
    }

    std::unique_ptr<FunctionType> function;
};
} /// namespace NES::Runtime::Execution::Functions
