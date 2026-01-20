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
#include <Functions/LogicalFunction.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

template <>
struct Reflector<detail::ErasedLogicalFunction>
{
    Reflected operator()(const detail::ErasedLogicalFunction& function) const
    {
        return function.reflect();
    }
};

struct ReflectedLogicalFunction
{
    std::string functionType;
    Reflected functionConfig;
};


template <LogicalFunctionConcept Checked>
struct Reflector<TypedLogicalFunction<Checked>>
{
    Reflected operator()(const TypedLogicalFunction<Checked>& function) const
    {
        return reflect(ReflectedLogicalFunction{std::string{function.getType()}, reflect(*function)});
    }
};

template <LogicalFunctionConcept Checked>
struct Unreflector<TypedLogicalFunction<Checked>>
{
    TypedLogicalFunction<Checked> operator()(const Reflected& _) const
    {
        throw NotImplemented("Unreflector");
    }
};

template <>
struct Reflector<TypedLogicalFunction<detail::ErasedLogicalFunction>>
{
    Reflected operator()(const TypedLogicalFunction<detail::ErasedLogicalFunction>& function) const
    {
        return reflect(ReflectedLogicalFunction{std::string{function.getType()}, function->reflect()});
    }
};

template <>
struct Unreflector<TypedLogicalFunction<>>
{
    TypedLogicalFunction<> operator()(const Reflected& rfl) const
    {
        auto [name, data] = unreflect<ReflectedLogicalFunction>(rfl);

        LogicalFunctionRegistryArguments argument;
        argument.data = data;

        auto logicalFunction =  LogicalFunctionRegistry::instance().create(name, argument);
        if (!logicalFunction.has_value())
        {
            throw CannotDeserialize("Failed to deserialize logical function");
        }
        return logicalFunction.value();
    }
};

static_assert(requires(LogicalFunction logicalFunction){ {reflect(logicalFunction)} -> std::same_as<Reflected>;});
}