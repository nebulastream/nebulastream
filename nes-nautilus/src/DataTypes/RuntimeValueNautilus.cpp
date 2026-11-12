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

#include <DataTypes/RuntimeValueNautilus.hpp>

#include <concepts>
#include <cstdint>
#include <string>
#include <string_view>
#include <DataTypes/VariableSizedData.hpp>
#include <nautilus/tracing/TracingUtil.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_details.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
void requireRuntime()
{
    PRECONDITION(
        !nautilus::tracing::inTracer(), "Native runtime values cannot be materialized while tracing; use a traced memory load instead");
}
}

VarVal toVarVal(const RuntimeValueView value)
{
    requireRuntime();
    const auto type = value.getDataType();
    return visitRuntimeType(
        type.type,
        [&]<typename T>() -> VarVal
        {
            if constexpr (std::same_as<T, std::string_view>)
            {
                if (value.isNull())
                {
                    return VarVal{VariableSizedData{nautilus::val<int8_t*>{nullptr}, nautilus::val<uint64_t>{0}}, true, true};
                }
                const auto bytes = value.get<std::string_view>();
                /// VariableSizedData currently exposes a mutable pointer; this bridge only borrows it for read access.
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-type-const-cast)
                auto* pointer = reinterpret_cast<int8_t*>(const_cast<char*>(bytes.data()));
                return VarVal{
                    VariableSizedData{nautilus::val<int8_t*>{pointer}, nautilus::val<uint64_t>{bytes.size()}}, type.nullable, false};
            }
            else
            {
                return VarVal{value.isNull() ? T{} : value.get<T>(), type.nullable, value.isNull()};
            }
        });
}

RuntimeValue fromVarVal(const VarVal& value, const DataType type)
{
    requireRuntime();
    PRECONDITION(value.getDataType() == type, "Nautilus value {} does not match native runtime type {}", value.getDataType(), type);
    if (nautilus::details::RawValueResolver<bool>::getRawValue(value.isNull()))
    {
        return RuntimeValue::null(type);
    }
    return visitRuntimeType(
        type.type,
        [&]<typename T>() -> RuntimeValue
        {
            if constexpr (std::same_as<T, std::string_view>)
            {
                const auto bytes = value.getRawValueAs<VariableSizedData>();
                const auto size = nautilus::details::RawValueResolver<uint64_t>::getRawValue(bytes.getSize());
                const auto* pointer = nautilus::details::RawValueResolver<int8_t*>::getRawValue(bytes.getContent());
                if (size == 0)
                {
                    return RuntimeValue{type, std::string{}};
                }
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                return RuntimeValue{type, std::string{reinterpret_cast<const char*>(pointer), size}};
            }
            else
            {
                return RuntimeValue{type, nautilus::details::RawValueResolver<T>::getRawValue(value.getRawValueAs<nautilus::val<T>>())};
            }
        });
}

}
