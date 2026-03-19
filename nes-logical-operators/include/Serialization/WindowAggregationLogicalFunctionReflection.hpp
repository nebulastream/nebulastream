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
#include <string>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <nameof.hpp>

namespace NES::detail
{
struct ReflectedWindowAggregationLogicalFunction
{
    std::string functionType;
    Reflected functionConfig;
};
}

namespace NES
{

template <>
struct Reflector<detail::ErasedWindowAggregationFunction>
{
    Reflected operator()(const detail::ErasedWindowAggregationFunction& function) const { return function.reflect(); }
};

template <WindowAggregationFunctionConcept Checked>
struct Reflector<TypedWindowAggregationLogicalFunction<Checked>>
{
    Reflected operator()(const TypedWindowAggregationLogicalFunction<Checked>& function) const
    {
        return reflect(detail::ReflectedWindowAggregationLogicalFunction{std::string{function.getName()}, Reflector<Checked>{}(*function)});
    }
};

template <>
struct Reflector<TypedWindowAggregationLogicalFunction<detail::ErasedWindowAggregationFunction>>
{
    Reflected operator()(const TypedWindowAggregationLogicalFunction<detail::ErasedWindowAggregationFunction>& function) const;
};

template <>
struct Unreflector<TypedWindowAggregationLogicalFunction<>>
{
    TypedWindowAggregationLogicalFunction<> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

template <WindowAggregationFunctionConcept Checked>
struct Unreflector<TypedWindowAggregationLogicalFunction<Checked>>
{
    TypedWindowAggregationLogicalFunction<Checked> operator()(const Reflected& rfl, const ReflectionContext& context) const
    {
        auto erased = context.unreflect<WindowAggregationLogicalFunction>(rfl);
        if (auto casted = erased.tryGetAs<Checked>())
        {
            return casted.value();
        }
        throw CannotDeserialize(
            "Expected window aggregation logical function of type {}, but got {}", NAMEOF_TYPE(Checked), erased.getName());
    }
};

}
