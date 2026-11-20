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

#include <Functions/CastToPhysicalFunction.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <string_view>
#include <type_traits>
#include <utility>
#include <DataTypes/VariableSizedData.hpp>
#include <Arena.hpp>
#include <function.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

CastToPhysicalFunction::CastToPhysicalFunction(PhysicalFunction childFunction, DataType castToType)
    : castToType(std::move(castToType)), childFunction(std::move(childFunction))
{
}

CastToPhysicalFunction::CastToPhysicalFunction(PhysicalFunction childFunction, DataType castToType, std::optional<DataType> inputType)
    : castToType(std::move(castToType)), childFunction(std::move(childFunction)), inputType(std::move(inputType))
{
}

namespace
{
/// The text of a value, formatted into a buffer of the worker thread; read it with copyText before the next call.
/// Integers in decimal, floats in their shortest round-trip form with ".0" if they would read as integers (as SQL
/// prints a DOUBLE), booleans as true/false.
thread_local std::array<char, 64> textBuffer{};

template <typename T>
uint32_t formatText(const T value)
{
    if constexpr (std::is_same_v<T, bool>)
    {
        const std::string_view text = value ? "true" : "false";
        std::ranges::copy(text, textBuffer.begin());
        return static_cast<uint32_t>(text.size());
    }
    else if constexpr (std::is_same_v<T, char>)
    {
        textBuffer[0] = value;
        return 1;
    }
    else
    {
        auto [end, error] = std::to_chars(textBuffer.data(), textBuffer.data() + textBuffer.size(), value);
        auto size = static_cast<uint32_t>(end - textBuffer.data());
        if constexpr (std::is_floating_point_v<T>)
        {
            const std::string_view text{textBuffer.data(), size};
            if (std::isfinite(value) and text.find_first_of(".eE") == std::string_view::npos)
            {
                textBuffer[size++] = '.';
                textBuffer[size++] = '0';
            }
        }
        return size;
    }
}

void copyText(char* destination, const uint32_t size)
{
    std::copy_n(textBuffer.data(), size, destination);
}

template <typename T>
VarVal toText(const VarVal& value, ArenaRef& arena)
{
    const auto size = nautilus::invoke(+[](const T input) { return formatText<T>(input); }, value.getRawValueAs<nautilus::val<T>>());
    auto text = arena.allocateVariableSizedData(size);
    nautilus::invoke(copyText, static_cast<nautilus::val<char*>>(text.getContent()), size);
    return VarVal{text, value.isNullable(), false};
}
}

VarVal CastToPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);
    if (castToType.type == DataType::Type::VARSIZED and inputType and inputType->type != DataType::Type::VARSIZED)
    {
        if (value.isNullable() and value.isNull())
        {
            return VarVal{VariableSizedData{nullptr, 0}, true, true};
        }
        switch (inputType->type)
        {
            case DataType::Type::INT8: return toText<int8_t>(value, arena);
            case DataType::Type::INT16: return toText<int16_t>(value, arena);
            case DataType::Type::INT32: return toText<int32_t>(value, arena);
            case DataType::Type::INT64: return toText<int64_t>(value, arena);
            case DataType::Type::UINT8: return toText<uint8_t>(value, arena);
            case DataType::Type::UINT16: return toText<uint16_t>(value, arena);
            case DataType::Type::UINT32: return toText<uint32_t>(value, arena);
            case DataType::Type::UINT64: return toText<uint64_t>(value, arena);
            case DataType::Type::FLOAT32: return toText<float>(value, arena);
            case DataType::Type::FLOAT64: return toText<double>(value, arena);
            case DataType::Type::BOOLEAN: return toText<bool>(value, arena);
            case DataType::Type::CHAR: return toText<char>(value, arena);
            default: throw UnknownOperation("Cannot cast {} to VARSIZED", *inputType);
        }
    }
    return value.castToType(castToType.type);
}

PhysicalFunction castIntegerResult(PhysicalFunction function, const DataType& outputType)
{
    return outputType.isInteger() ? PhysicalFunction{CastToPhysicalFunction(std::move(function), outputType)} : function;
}

}
