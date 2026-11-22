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

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <variant>

#include <Util/Reflection/ReflectionCore.hpp>
#include <Util/Reflection/SharedPointerDeserializationState.hpp>
#include <Util/Reflection/SharedPointerSerializationState.hpp>
#include <folly/ScopeGuard.h>
#include <rfl/Generic.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
/// Every occurrence carries its full definition, not a back-reference, so readers may
/// consume plans in any order. IDs are meaningful only within the injected state batch.
/// Definitions must remain identical within that scope; cycles and polymorphic type
/// changes at the same address are rejected. Const and mutable views share identity.
template <typename T>
struct Reflector<std::shared_ptr<T>>
{
    using ContextType = std::shared_ptr<SharedPointerSerializationState>;

    explicit Reflector(ContextType state) : state(std::move(state)) { }

    Reflected operator()(const std::shared_ptr<T>& value, const ReflectionContext& ctx) const
    {
        static_assert(!std::is_array_v<T> && !std::is_void_v<T> && !std::is_volatile_v<T>);
        if (!state)
        {
            throw CannotSerialize("Shared pointer serialization state must not be null");
        }
        if (!value)
        {
            return Reflected{};
        }

        using Pointee = std::remove_const_t<T>;
        auto& identities = *state;
        auto found = identities.serialized.find(value.get());
        if (found == identities.serialized.end())
        {
            if (identities.nextId == std::numeric_limits<int64_t>::max())
            {
                throw CannotSerialize("Shared pointer identity space exhausted");
            }
            found = identities.serialized
                        .emplace(
                            value.get(),
                            SharedPointerSerializationState::Entry{
                                value, std::type_index{typeid(Pointee)}, identities.nextId++, std::nullopt, false})
                        .first;
        }
        auto& entry = found->second;
        if (entry.type != std::type_index{typeid(Pointee)})
        {
            throw CannotSerialize("Shared pointer address reused with incompatible pointee type");
        }
        if (entry.active)
        {
            throw CannotSerialize("Cycles in shared pointer reflection are not supported");
        }

        entry.active = true;
        const auto* identity = value.get();
        auto resetActive = folly::makeGuard(
            [&identities, identity]() noexcept
            {
                if (const auto current = identities.serialized.find(identity); current != identities.serialized.end())
                {
                    current->second.active = false;
                }
            });

        const auto definition = ctx.reflect<Pointee>(*value);
        const auto text = rfl::json::write(*definition);
        auto& current = identities.serialized.at(identity);
        if (current.serializedValue && *current.serializedValue != text)
        {
            throw CannotSerialize("Conflicting definition for shared pointer id {}", current.id);
        }
        current.serializedValue = text;
        rfl::Generic::Object object;
        /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) key assignment inserts object properties
        object["id"] = rfl::Generic{current.id};
        object["value"] = *definition;
        /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        current.active = false;
        resetActive.dismiss();
        return Reflected{rfl::Generic{std::move(object)}};
    }

private:
    ContextType state;
};

template <typename T>
struct Unreflector<std::shared_ptr<T>>
{
    using ContextType = std::shared_ptr<SharedPointerDeserializationState>;

    explicit Unreflector(ContextType state) : state(std::move(state)) { }

    std::shared_ptr<T> operator()(const Reflected& data, const ReflectionContext& ctx) const
    {
        static_assert(!std::is_array_v<T> && !std::is_void_v<T> && !std::is_volatile_v<T>);
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
        using Pointee = std::remove_const_t<T>;
        const auto type = std::type_index{typeid(Pointee)};
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
            return std::static_pointer_cast<T>(entry.owner);
        }

        identities.emplace(*id, SharedPointerDeserializationState::Entry{.owner = nullptr, .type = type, .serializedValue = text});
        auto eraseIncomplete = folly::makeGuard([&identities, identity = *id]() noexcept { identities.erase(identity); });
        auto result = std::make_shared<Pointee>(ctx.unreflect<Pointee>(Reflected{definition.value()}));
        identities.at(*id).owner = result;
        eraseIncomplete.dismiss();
        return result;
    }

private:
    ContextType state;
};

}
