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
#include <type_traits>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Functions
{

template <typename T>
requires std::is_integral_v<T> || std::is_floating_point_v<T>
class ExecutableFunctionConstantValue final : public Function
{
public:
    explicit ExecutableFunctionConstantValue(T value) : value(value) { }

    [[nodiscard]] VarVal execute(const Record&, ArenaRef&) const override { return VarVal(value); }

private:
    const T value;
};

using ConstantInt8ValueFunction = ExecutableFunctionConstantValue<int8_t>;
using ConstantInt16ValueFunction = ExecutableFunctionConstantValue<int16_t>;
using ConstantInt32ValueFunction = ExecutableFunctionConstantValue<int32_t>;
using ConstantInt64ValueFunction = ExecutableFunctionConstantValue<int64_t>;
using ConstantUInt8ValueFunction = ExecutableFunctionConstantValue<uint8_t>;
using ConstantUInt16ValueFunction = ExecutableFunctionConstantValue<uint16_t>;
using ConstantUInt32ValueFunction = ExecutableFunctionConstantValue<uint32_t>;
using ConstantUInt64ValueFunction = ExecutableFunctionConstantValue<uint64_t>;
using ConstantFloatValueFunction = ExecutableFunctionConstantValue<float>;
using ConstantDoubleValueFunction = ExecutableFunctionConstantValue<double>;
using ConstantBooleanValueFunction = ExecutableFunctionConstantValue<bool>;

}
