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

#include <cstdint>
#include <memory>
#include <typeindex>
#include <utility>
#include <variant>

#include <Functions/LogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <Util/Reflection/SharedPointerDeserializationState.hpp>
#include <folly/ScopeGuard.h>
#include <rfl/Generic.hpp>
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
    const auto* object = std::get_if<rfl::Generic::Object>(&data->variant());
    if (object == nullptr)
    {
        throw CannotDeserialize("Expected shared pointer object with id and value");
    }
    const auto idField = object->get("id");
    const auto definition = object->get("value");
    if (!idField.has_value() || !definition.has_value())
    {
        throw CannotDeserialize("Shared pointer definition requires id and value");
    }
    const auto* id = std::get_if<int64_t>(&idField.value().variant());
    if ((id == nullptr) || *id <= 0)
    {
        throw CannotDeserialize("Shared pointer id must be a positive integer");
    }

    const auto type = std::type_index{typeid(detail::ErasedLogicalFunction)};
    const auto text = rfl::json::write(definition.value());
    auto& identities = state->deserialized;
    if (const auto found = identities.find(*id); found != identities.end())
    {
        const auto& entry = found->second;
        if (entry.type != type)
        {
            throw CannotDeserialize("Shared pointer id {} reused with incompatible pointee type", *id);
        }
        if (entry.serializedValue != text)
        {
            throw CannotDeserialize("Conflicting definition for shared pointer id {}", *id);
        }
        if (!entry.owner)
        {
            throw CannotDeserialize("Cycles in shared pointer reflection are not supported");
        }
        return std::static_pointer_cast<const detail::ErasedLogicalFunction>(entry.owner);
    }

    identities.emplace(*id, SharedPointerDeserializationState::Entry{.owner = nullptr, .type = type, .serializedValue = text});
    auto eraseIncomplete = folly::makeGuard([&identities, identity = *id]() noexcept { identities.erase(identity); });
    auto [name, functionData] = context.unreflect<detail::ReflectedLogicalFunction>(Reflected{definition.value()});
    auto logicalFunction = LogicalFunctionUnreflectionRegistry::instance().unreflect(name, functionData, context);
    if (!logicalFunction.has_value())
    {
        throw CannotDeserialize("Failed to unreflect logical function of type {}", name);
    }
    auto result = std::move(logicalFunction).value().self;
    identities.at(*id).owner = std::const_pointer_cast<detail::ErasedLogicalFunction>(result);
    eraseIncomplete.dismiss();
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
