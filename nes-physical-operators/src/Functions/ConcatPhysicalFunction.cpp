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

#include <Functions/ConcatPhysicalFunction.hpp>

#include <charconv>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Strings.hpp>
#include <Arena.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <val_bool.hpp>

namespace NES
{

namespace
{
/// Proxy for the serialize-into-concat fallback below: decimal text for a materialised numeric operand,
/// byte-identical to the sink codecs (fixed-6-with-stripped-zeros floats, canonical decimal integers).
/// Returns the number of chars written into `dest`.
template <typename T>
uint64_t serializeNumericText(const T value, int8_t* const dest)
{
    char* const out = reinterpret_cast<char*>(dest);
    if constexpr (std::is_floating_point_v<T>)
    {
        const std::string text = formatFloat(value);
        std::memcpy(out, text.data(), text.size());
        return text.size();
    }
    else if constexpr (std::is_same_v<T, char>)
    {
        *out = value;
        return 1;
    }
    else if constexpr (std::is_same_v<T, bool>)
    {
        *out = value ? '1' : '0';
        return 1;
    }
    else
    {
        return static_cast<uint64_t>(std::to_chars(out, out + 21, value).ptr - out);
    }
}

/// Worst-case text length per type: the widest fixed-6 double (~317 chars for 1.7e308, bounded with slack
/// like the sink codec), int64 min / uint64 max (20 chars), a single char/bool byte.
template <typename T>
consteval uint64_t maxNumericTextLen()
{
    if constexpr (std::is_floating_point_v<T>)
    {
        return 344;
    }
    else if constexpr (std::is_same_v<T, char> || std::is_same_v<T, bool>)
    {
        return 1;
    }
    else
    {
        return 21;
    }
}

template <typename T>
VarVal serializeMaterializedNumeric(const nautilus::val<T>& underlying, const ArenaRef& arena, const VarVal& original)
{
    const auto dest = arena.allocateMemory(nautilus::val<size_t>{maxNumericTextLen<T>()});
    const auto length = nautilus::invoke(serializeNumericText<T>, underlying, dest);
    const bool nullable = original.isNullable();
    return VarVal{VariableSizedData{dest, length}, nullable, nullable ? original.isNull() : nautilus::val<bool>{false}};
}

/// The serialize-into-concat fallback: compiled plans hand CONCAT lazy operands whose raw input text is
/// forwarded span-wise with no parse, but the interpreted formatter materialises records, so a numeric
/// operand arrives here as a plain value with no text form (castToType(VARSIZED) would throw). Serialize
/// it into the arena; the wrapped bytes then take the same path as a string operand.
VarVal withTextForm(const VarVal& value, const ArenaRef& arena)
{
    return value.customVisit(
        [&]<typename T>(const T& underlying) -> VarVal
        {
            using PlainT = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<PlainT, VariableSizedData> || std::is_same_v<PlainT, std::shared_ptr<LazyValueRepresentation>>)
            {
                return value;
            }
            else
            {
                return serializeMaterializedNumeric(underlying, arena, value);
            }
        });
}
}

ConcatPhysicalFunction::ConcatPhysicalFunction(PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

VarVal ConcatPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = withTextForm(leftPhysicalFunction.execute(record, arena), arena);
    const auto rightValue = withTextForm(rightPhysicalFunction.execute(record, arena), arena);
    /// CONCAT is expressed purely through generic VarVal operations: cast each operand to VARSIZED (which
    /// forwards a numeric field's raw text as a string with no parse), then `+`, which on two VARSIZED values
    /// means string concatenation. It never names a rope or a span -- `+` is "add" for numbers and "concat"
    /// for strings, disambiguated by type, and the rope lives entirely inside the VARSIZED value type.
    return leftValue.castToType(DataType::Type::VARSIZED) + rightValue.castToType(DataType::Type::VARSIZED);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterConcatPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 2, "Concat function must have exactly two child functions");
    return ConcatPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.childFunctions[1]);
}
}
