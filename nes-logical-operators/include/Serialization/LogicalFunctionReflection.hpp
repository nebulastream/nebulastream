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
#include <string>
#include <type_traits>
#include <Functions/LogicalFunction.hpp>
#include <Util/Reflection.hpp>

namespace NES::detail
{
struct ReflectedLogicalFunction
{
    std::string functionType;
    Reflected functionConfig;
};

// Helper to detect TypedLogicalFunction instances
template <typename T>
struct is_typed_logical_function : std::false_type
{
};

template <typename Checked>
struct is_typed_logical_function<TypedLogicalFunction<Checked>> : std::true_type
{
};

template <typename T>
inline constexpr bool is_typed_logical_function_v = is_typed_logical_function<T>::value;
}

namespace NES
{

template <>
struct Reflector<detail::ErasedLogicalFunction>
{
    Reflected operator()(const detail::ErasedLogicalFunction& function) const { return function.reflect(); }
};

template <LogicalFunctionConcept Checked>
struct Reflector<TypedLogicalFunction<Checked>>
{
    Reflected operator()(const TypedLogicalFunction<Checked>& function) const
    {
        return reflect(detail::ReflectedLogicalFunction{std::string{function.getType()}, Reflector<Checked>{}(*function)});
    }
};

template <>
struct Reflector<TypedLogicalFunction<detail::ErasedLogicalFunction>>
{
    Reflected operator()(const TypedLogicalFunction<detail::ErasedLogicalFunction>& function) const;
};

template <>
struct Unreflector<TypedLogicalFunction<>>
{
    TypedLogicalFunction<> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

template <LogicalFunctionConcept Checked>
struct Unreflector<TypedLogicalFunction<Checked>>
{
    TypedLogicalFunction<Checked> operator()(const Reflected& rfl, const ReflectionContext& context) const
    {
        auto erased = context.unreflect<LogicalFunction>(rfl);
        if (auto casted = erased.tryGetAs<Checked>())
        {
            return casted.value();
        }
        throw CannotDeserialize("Expected logical function of type {}, but got {}", NAMEOF_TYPE(Checked), erased.getType());
    }
};

static_assert(requires(LogicalFunction logicalFunction) {
    { reflect(logicalFunction) } -> std::same_as<Reflected>;
});

}
