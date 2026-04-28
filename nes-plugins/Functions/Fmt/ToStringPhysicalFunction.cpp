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

#include "ToStringPhysicalFunction.hpp"

#include <charconv>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <val.hpp>

namespace NES
{

namespace
{
/// Conservative upper bound for the textual length of a value of type T.
/// Integers: ceil(log10(2^bits)) ~ bits * 0.302 → use bits * 3 / 8 + 2 (sign + slack).
/// Floats: std::to_chars shortest round-trip is at most 24 chars for double, 16 for float.
template <typename T>
constexpr uint32_t toCharsBufferSize()
{
    if constexpr (std::is_floating_point_v<T>)
    {
        return std::is_same_v<T, double> ? 32 : 16;
    }
    else
    {
        return sizeof(T) * 3 + 2;
    }
}

/// Formats `value` into `buf` via std::to_chars and returns the number of
/// bytes written. Bound to a runtime symbol via nautilus::invoke.
template <typename T>
uint32_t toCharsValue(T value, int8_t* buf, uint32_t bufSize)
{
    auto* charBuf = reinterpret_cast<char*>(buf);
    auto result = std::to_chars(charBuf, charBuf + bufSize, value);
    return static_cast<uint32_t>(result.ptr - charBuf);
}

void writeBoolTrue(int8_t* dst)
{
    dst[0] = 't';
    dst[1] = 'r';
    dst[2] = 'u';
    dst[3] = 'e';
}

void writeBoolFalse(int8_t* dst)
{
    dst[0] = 'f';
    dst[1] = 'a';
    dst[2] = 'l';
    dst[3] = 's';
    dst[4] = 'e';
}

void writeChar(int8_t* dst, char c)
{
    dst[0] = static_cast<int8_t>(c);
}
}

ToStringPhysicalFunction::ToStringPhysicalFunction(PhysicalFunction child) : child(std::move(child))
{
}

VarVal ToStringPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto value = child.execute(record, arena);
    /// customVisit dispatches on the underlying nautilus type at trace time.
    /// All branches are instantiated; only one runs per row.
    return value.customVisit(
        [&]<typename Underlying>(Underlying&& underlying) -> VarVal
        {
            using U = std::remove_cvref_t<Underlying>;

            if constexpr (std::is_same_v<U, VariableSizedData>)
            {
                return underlying;
            }
            else if constexpr (std::is_same_v<U, nautilus::val<bool>>)
            {
                auto trueBuf = arena.allocateVariableSizedData(nautilus::val<uint64_t>{4});
                auto falseBuf = arena.allocateVariableSizedData(nautilus::val<uint64_t>{5});
                nautilus::invoke(writeBoolTrue, trueBuf.getContent());
                nautilus::invoke(writeBoolFalse, falseBuf.getContent());
                if (underlying)
                {
                    return trueBuf;
                }
                return falseBuf;
            }
            else if constexpr (std::is_same_v<U, nautilus::val<char>>)
            {
                auto buf = arena.allocateVariableSizedData(nautilus::val<uint64_t>{1});
                nautilus::invoke(writeChar, buf.getContent(), underlying);
                return buf;
            }
            else
            {
                /// Numeric: U is nautilus::val<int*_t> / nautilus::val<uint*_t> / nautilus::val<float|double>.
                using ValueType = typename U::basic_type;
                constexpr uint32_t bufSize = toCharsBufferSize<ValueType>();
                auto buf = arena.allocateVariableSizedData(nautilus::val<uint64_t>{bufSize});
                nautilus::val<uint32_t> writtenSize = nautilus::invoke(
                    toCharsValue<ValueType>, underlying, buf.getContent(), nautilus::val<uint32_t>{bufSize});
                return VariableSizedData{buf.getContent(), nautilus::val<uint64_t>{writtenSize}};
            }
        });
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::Registerto_stringPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "to_string requires exactly one child function");
    return ToStringPhysicalFunction(std::move(args.childFunctions[0]));
}

}
