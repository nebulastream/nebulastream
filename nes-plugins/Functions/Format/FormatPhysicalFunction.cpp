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

#include "FormatPhysicalFunction.hpp"

#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <cstring>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <nautilus/function.hpp>
#include <select.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <charconv>
#include <type_traits>

namespace NES
{
    namespace {

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

        VarVal stringifyValue(const VarVal& value, const ArenaRef& arena)
        {
            return value.customVisit(
                [&]<typename Underlying>(Underlying&& underlying) -> VarVal
                {
                    using U = std::remove_cvref_t<Underlying>;

                    if constexpr (std::is_same_v<U, VariableSizedData>)
                    {
                        return VarVal{underlying, value.isNullable(), value.isNull()};
                    }
                    else if constexpr (std::is_same_v<U, nautilus::val<bool>>)
                    {
                        auto trueBuf = arena.allocateVariableSizedData(nautilus::val<uint64_t>{4});
                        auto falseBuf = arena.allocateVariableSizedData(nautilus::val<uint64_t>{5});

                        nautilus::invoke(writeBoolTrue, trueBuf.getContent());
                        nautilus::invoke(writeBoolFalse, falseBuf.getContent());

                        if (underlying)
                        {
                            return VarVal{trueBuf, value.isNullable(), value.isNull()};
                        }

                        return VarVal{falseBuf, value.isNullable(), value.isNull()};
                    }
                    else if constexpr (std::is_same_v<U, nautilus::val<char>>)
                    {
                        auto buf = arena.allocateVariableSizedData(nautilus::val<uint64_t>{1});
                        nautilus::invoke(writeChar, buf.getContent(), underlying);
                        return VarVal{buf, value.isNullable(), value.isNull()};
                    }
                    else
                    {
                        using ValueType = U::basic_type;

                        constexpr uint32_t bufSize = toCharsBufferSize<ValueType>();

                        auto buf = arena.allocateVariableSizedData(nautilus::val<uint64_t>{bufSize});

                        nautilus::val<uint32_t> writtenSize = nautilus::invoke(
                            toCharsValue<ValueType>,
                            underlying,
                            buf.getContent(),
                            nautilus::val<uint32_t>{bufSize});

                        auto result = VariableSizedData{
                            buf.getContent(),
                            nautilus::val<uint64_t>{writtenSize}};

                        return VarVal{result, value.isNullable(), value.isNull()};
                    }
                });
        }

        void storeArgument(
            int8_t* argPtrsRaw,
            int8_t* argSizesRaw,
            uint64_t index,
            int8_t* argPtr,
            uint64_t argSize)
        {
            auto** argPtrs = reinterpret_cast<int8_t**>(argPtrsRaw);
            auto* argSizes = reinterpret_cast<uint64_t*>(argSizesRaw);

            argPtrs[index] = argPtr;
            argSizes[index] = argSize;
        }

        uint64_t writeFormattedOutput(
            const int8_t* fmt,
            uint64_t fmtSize,
            int8_t* argPtrsRaw,
            int8_t* argSizesRaw,
            uint64_t /*argCount*/,
            int8_t* out)
        {
            auto** argPtrs = reinterpret_cast<int8_t**>(argPtrsRaw);
            auto* argSizes = reinterpret_cast<uint64_t*>(argSizesRaw);

            uint64_t outIndex = 0;
            uint64_t argIndex = 0;

            for (uint64_t i = 0; i < fmtSize; ++i)
            {
                if (fmt[i] == '{')
                {
                    if (i + 1 < fmtSize && fmt[i + 1] == '{')
                    {
                        out[outIndex++] = '{';
                        ++i;
                    }
                    else if (i + 1 < fmtSize && fmt[i + 1] == '}')
                    {
                        const auto argSize = argSizes[argIndex];
                        std::memcpy(out + outIndex, argPtrs[argIndex], argSize);
                        outIndex += argSize;

                        ++argIndex;
                        ++i;
                    }
                    else
                    {
                        out[outIndex++] = fmt[i];
                    }
                }
                else if (fmt[i] == '}')
                {
                    if (i + 1 < fmtSize && fmt[i + 1] == '}')
                    {
                        out[outIndex++] = '}';
                        ++i;
                    }
                    else
                    {
                        out[outIndex++] = fmt[i];
                    }
                }
                else
                {
                    out[outIndex++] = fmt[i];
                }
            }
            return outIndex;
        }
    }

FormatPhysicalFunction::FormatPhysicalFunction(std::vector<PhysicalFunction> childPhysicalFunctions)
    : childPhysicalFunctions(std::move(childPhysicalFunctions))
{
}

VarVal FormatPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
        PRECONDITION(
            not childPhysicalFunctions.empty(),
            "Format function must have at least one child function");

        /// Format is validated on the logical side:
        ///   - the format string is a constant VARSIZED literal
        ///   - the number of `{}` placeholders matches the number of arguments
        ///
        /// The physical side evaluates all children, stringifies every argument to
        /// VariableSizedData, stores their pointers/sizes in arena-allocated arrays,
        /// computes the output size, allocates the final VARSIZED result, and writes
        /// literal fragments plus argument bytes into it.

    const auto formatStringValue = childPhysicalFunctions[0].execute(record, arena);
    const auto formatString = formatStringValue.getRawValueAs<VariableSizedData>();

    const auto argCount = childPhysicalFunctions.size() - 1;

    auto argPtrsBuffer = arena.allocateVariableSizedData(
        nautilus::val<uint64_t>{argCount * sizeof(int8_t*)});

    auto argSizesBuffer = arena.allocateVariableSizedData(
        nautilus::val<uint64_t>{argCount * sizeof(uint64_t)});

    auto nullable = formatStringValue.isNullable();
    auto newNull = formatStringValue.isNullable() and formatStringValue.isNull();
    auto maxOutputSize = formatString.getSize();

    std::vector<VarVal> argValues;
    argValues.reserve(argCount);

    for (size_t i = 0; i < argCount; ++i)
    {
        const auto rawArgValue = childPhysicalFunctions[i + 1].execute(record, arena);
        nullable = nullable or rawArgValue.isNullable();
        newNull = newNull or (rawArgValue.isNullable() and rawArgValue.isNull());
        auto argValue = stringifyValue(rawArgValue, arena);
        const auto arg = argValue.getRawValueAs<VariableSizedData>();

        maxOutputSize = maxOutputSize + arg.getSize();

        nautilus::invoke(
            storeArgument,
            argPtrsBuffer.getContent(),
            argSizesBuffer.getContent(),
            nautilus::val<uint64_t>{i},
            arg.getContent(),
            arg.getSize());

        argValues.emplace_back(std::move(argValue));
    }

    auto output = arena.allocateVariableSizedData(
        nautilus::select(newNull, nautilus::val<uint64_t>{0}, maxOutputSize));

    const auto actualOutputSize = nautilus::select(
        newNull,
        nautilus::val<uint64_t>{0},
        nautilus::invoke(
            writeFormattedOutput,
            formatString.getContent(),
            formatString.getSize(),
            argPtrsBuffer.getContent(),
            argSizesBuffer.getContent(),
            nautilus::val<uint64_t>{argCount},
            output.getContent()));

    return VarVal{VariableSizedData{output.getContent(), actualOutputSize},
    nullable, newNull};
    }

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterFormatPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        not physicalFunctionRegistryArguments.childFunctions.empty(),
        "Format function must have at least one child function (the format string)");
    return FormatPhysicalFunction(std::move(physicalFunctionRegistryArguments.childFunctions));
}

}
