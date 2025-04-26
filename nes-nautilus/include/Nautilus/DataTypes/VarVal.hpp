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

#include <concepts>
#include <cstdint>
#include <memory>
#include <variant>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
// #include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/std/sstream.h>
#include <nautilus/std/string.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>

namespace NES::Nautilus
{

struct FundamentalBasicScalarValue
{
    template <typename... Args>
    using NautilusType = std::variant<nautilus::val<Args>...>;
    using AnyType = NautilusType<bool, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double>;

    template <typename T>
    requires(std::is_constructible_v<AnyType, T>)
    FundamentalBasicScalarValue(T t) : value(t)
    {
    }

    AnyType value;
};

struct Nullable
{
    FundamentalBasicScalarValue value;
    nautilus::val<bool> valid;
};

struct FundamentalScalarValue
{
    using AnyType = std::variant<FundamentalBasicScalarValue, Nullable>;
    AnyType value;

    template <typename T>
    requires(std::is_constructible_v<AnyType, T>)
    FundamentalScalarValue(T t) : value(t)
    {
    }
};

struct FixedSize
{
    FundamentalScalarValue get(size_t index);
    FundamentalScalarValue get(nautilus::val<size_t> index);
    std::vector<FundamentalScalarValue> values;
};

struct FixedSizePtr
{
    template <typename T>
    FixedSize load()
    {
        std::vector<FundamentalScalarValue> values;
        for (size_t i = 0; i < size; i++)
        {
            values.emplace_back(*static_cast<T*>(pointer) + i);
        }
    }

    template <typename T>
    FundamentalScalarValue get(size_t index)
    {
        std::vector<FundamentalScalarValue> values;
        return *static_cast<T*>(pointer)[index];
    }

    template <typename T>
    FundamentalScalarValue get(nautilus::val<size_t> index)
    {
        std::vector<FundamentalScalarValue> values;
        return *static_cast<T*>(pointer)[index];
    }

    nautilus::val<int8_t*> pointer;
    size_t size;
};

struct VarSizedPtr
{
    template <typename T>
    FundamentalScalarValue get(size_t index)
    {
        std::vector<FundamentalScalarValue> values;
        return *static_cast<T*>(pointer)[index];
    }

    template <typename T>
    FundamentalScalarValue get(nautilus::val<size_t> index)
    {
        std::vector<FundamentalScalarValue> values;
        return *static_cast<T*>(pointer)[index];
    }

    nautilus::val<int8_t*> pointer;
    nautilus::val<size_t> size;
};

struct VarVal
{
    using AnyVal = std::variant<FundamentalScalarValue, FixedSize, FixedSizePtr, VarSizedPtr>;
    AnyVal value;

    template <typename T>
    requires(std::is_constructible_v<AnyVal, T>)
    VarVal(T t) : value(t)
    {
    }
};

}
