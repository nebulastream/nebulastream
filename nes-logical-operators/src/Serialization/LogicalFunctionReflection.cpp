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

#include <Serialization/LogicalFunctionReflection.hpp>

#include <memory>
#include <utility>

#include <Functions/LogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <Util/Reflection/SharedPtrReflection.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionUnreflectionRegistry.hpp>

namespace NES
{
Unreflector<std::shared_ptr<const detail::ErasedLogicalFunction>>::Unreflector(ContextType state) : state(std::move(state))
{
}

std::shared_ptr<const detail::ErasedLogicalFunction>
Unreflector<std::shared_ptr<const detail::ErasedLogicalFunction>>::operator()(const Reflected& data, const ReflectionContext& context) const
{
    if (!state)
    {
        throw CannotDeserialize("Shared pointer deserialization state must not be null");
    }
    if (data.isEmpty())
    {
        return nullptr;
    }
    const auto definition = context.unreflect<detail::SharedPointerDefinition>(data);
    const auto text = rfl::json::write(*definition.value);
    auto result = state->getOrCreate<detail::ErasedLogicalFunction>(
        definition.id,
        text,
        [&context, &definition]
        {
            auto [name, functionData] = context.unreflect<detail::ReflectedLogicalFunction>(definition.value);
            auto logicalFunction = LogicalFunctionUnreflectionRegistry::instance().unreflect(name, functionData, context);
            if (!logicalFunction.has_value())
            {
                throw CannotDeserialize("Failed to unreflect logical function of type {}", name);
            }
            return std::const_pointer_cast<detail::ErasedLogicalFunction>(std::move(logicalFunction).value().self);
        });
    return result;
}

Reflected Reflector<TypedLogicalFunction<>>::operator()(const TypedLogicalFunction<>& function, const ReflectionContext& context) const
{
    return context.reflect(function.self);
}

TypedLogicalFunction<> Unreflector<TypedLogicalFunction<>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    return TypedLogicalFunction<>{context.unreflect<std::shared_ptr<const detail::ErasedLogicalFunction>>(rfl)};
}
}
