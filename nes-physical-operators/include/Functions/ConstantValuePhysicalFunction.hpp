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

#include <cstdint>
#include <type_traits>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

template <typename T>
requires std::is_integral_v<T> || std::is_floating_point_v<T>
class ConstantValuePhysicalFunction final : public PhysicalFunctionConcept
{
public:
    explicit ConstantValuePhysicalFunction(T value) : value(value) { }
    VarVal execute(const Record&, ArenaRef&) const override { return VarVal(value); }

private:
    const T value;
};

using ConstantInt8ValueFunction = ConstantValuePhysicalFunction<int8_t>;
using ConstantInt16ValueFunction = ConstantValuePhysicalFunction<int16_t>;
using ConstantInt32ValueFunction = ConstantValuePhysicalFunction<int32_t>;
using ConstantInt64ValueFunction = ConstantValuePhysicalFunction<int64_t>;
using ConstantUInt8ValueFunction = ConstantValuePhysicalFunction<uint8_t>;
using ConstantUInt16ValueFunction = ConstantValuePhysicalFunction<uint16_t>;
using ConstantUInt32ValueFunction = ConstantValuePhysicalFunction<uint32_t>;
using ConstantUInt64ValueFunction = ConstantValuePhysicalFunction<uint64_t>;
using ConstantFloatValueFunction = ConstantValuePhysicalFunction<float>;
using ConstantDoubleValueFunction = ConstantValuePhysicalFunction<double>;
using ConstantBooleanValueFunction = ConstantValuePhysicalFunction<bool>;

}
