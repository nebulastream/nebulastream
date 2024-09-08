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
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES::Runtime::Execution::Functions
{

/**
 * @brief This function returns a specific constant values.
 */
template <typename T>
requires std::is_integral_v<T> || std::is_floating_point_v<T>
class ConstantValueFunction : public Function
{
public:
    explicit ConstantValueFunction(T value);
    VarVal execute(Record& record) const override;

private:
    const T value;
};

using ConstantInt8ValueFunction = ConstantValueFunction<int8_t>;
using ConstantInt16ValueFunction = ConstantValueFunction<int16_t>;
using ConstantInt32ValueFunction = ConstantValueFunction<int32_t>;
using ConstantInt64ValueFunction = ConstantValueFunction<int64_t>;
using ConstantUInt8ValueFunction = ConstantValueFunction<uint8_t>;
using ConstantUInt16ValueFunction = ConstantValueFunction<uint16_t>;
using ConstantUInt32ValueFunction = ConstantValueFunction<uint32_t>;
using ConstantUInt64ValueFunction = ConstantValueFunction<uint64_t>;
using ConstantFloatValueFunction = ConstantValueFunction<float>;
using ConstantDoubleValueFunction = ConstantValueFunction<double>;
using ConstantBooleanValueFunction = ConstantValueFunction<bool>;

} /// namespace NES::Runtime::Execution::Functions
