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
#include <string>
#include <type_traits>
#include <typeindex>
#include <variant>
#include <Util/Reflection/ReflectionCore.hpp>

namespace NES
{

/// Every occurrence carries its full definition, not a back-reference, so readers may
/// consume plans in any order. IDs are meaningful only within one ReflectionContext scope.
/// Definitions must remain identical within that scope; cycles and polymorphic type
/// changes at the same address are rejected. Const and mutable views share identity.
template <typename T>
struct Reflector<std::shared_ptr<T>>
{
    Reflected operator()(const std::shared_ptr<T>& value, const ReflectionContext& ctx) const
    {
        static_assert(!std::is_array_v<T> && !std::is_void_v<T> && !std::is_volatile_v<T>);
        if (!value)
        {
            return Reflected{};
        }

        using Pointee = std::remove_const_t<T>;
        auto& identities = *ctx.pointerIdentities;
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
                            ReflectionContext::SerializedPointer{
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
        try
        {
            const auto definition = ctx.reflect<Pointee>(*value);
            const auto text = rfl::json::write(*definition);
            if (entry.definition && *entry.definition != text)
            {
                throw CannotSerialize("Conflicting definition for shared pointer id {}", entry.id);
            }
            entry.definition = text;
            rfl::Generic::Object object;
            object["id"] = rfl::Generic{entry.id};
            object["value"] = *definition;
            entry.active = false;
            return Reflected{rfl::Generic{std::move(object)}};
        }
        catch (...) /// NOLINT(no-raw-catch-all) clear cycle detection before propagating the original error
        {
            entry.active = false;
            throw;
        }
    }
};

template <typename T>
struct Unreflector<std::shared_ptr<T>>
{
    std::shared_ptr<T> operator()(const Reflected& data, const ReflectionContext& ctx) const
    {
        static_assert(!std::is_array_v<T> && !std::is_void_v<T> && !std::is_volatile_v<T>);
        if (data.isEmpty())
        {
            return nullptr;
        }
        const auto* object = std::get_if<rfl::Generic::Object>(&data->variant());
        if (!object)
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
        if (!id || *id <= 0)
        {
            throw CannotDeserialize("Shared pointer id must be a positive integer");
        }
        using Pointee = std::remove_const_t<T>;
        const auto type = std::type_index{typeid(Pointee)};
        const auto text = rfl::json::write(definition.value());
        auto& identities = ctx.pointerIdentities->deserialized;
        if (const auto found = identities.find(*id); found != identities.end())
        {
            const auto& entry = found->second;
            if (entry.type != type)
            {
                throw CannotDeserialize("Shared pointer id {} reused with incompatible pointee type", *id);
            }
            if (entry.definition != text)
            {
                throw CannotDeserialize("Conflicting definition for shared pointer id {}", *id);
            }
            if (!entry.owner)
            {
                throw CannotDeserialize("Cycles in shared pointer reflection are not supported");
            }
            return std::static_pointer_cast<Pointee>(entry.owner);
        }

        identities.emplace(*id, ReflectionContext::DeserializedPointer{nullptr, type, text, {}});
        try
        {
            /// Construct from the unreflector result, not a default-constructed placeholder.
            auto result = std::make_shared<Pointee>(ctx.unreflect<Pointee>(Reflected{definition.value()}));
            auto& entry = identities.at(*id);
            if constexpr (requires(Pointee& pointee) { pointee.validate(); })
            {
                entry.validate
                    = [](const std::shared_ptr<void>& owner) { static_cast<void>(std::static_pointer_cast<Pointee>(owner)->validate()); };
            }
            entry.owner = result;
            return result;
        }
        catch (...) /// NOLINT(no-raw-catch-all) remove the incomplete identity before propagating the original error
        {
            identities.erase(*id);
            throw;
        }
    }
};

}
