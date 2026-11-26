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
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ErrorHandling.hpp>

namespace NES
{

/// Batch-scoped, non-concurrent identity bookkeeping for shared-pointer deserialization.
class SharedPointerDeserializationState
{
    struct Entry
    {
        std::shared_ptr<void> owner;
        std::type_index type;
        std::string serializedValue;
    };

    /// An empty owner marks a definition currently being constructed (cycles are rejected).
    std::unordered_map<int64_t, Entry> deserialized;

public:
    /// Reuse or construct one shared owner for a checked, repeated definition.
    template <typename T, typename Factory>
    [[nodiscard]] std::shared_ptr<T> getOrCreate(int64_t id, const std::string& serializedValue, Factory&& factory)
    {
        static_assert(!std::is_const_v<T>);
        if (id <= 0)
        {
            throw CannotDeserialize("Shared pointer id must be a positive integer");
        }
        const auto type = std::type_index{typeid(T)};
        if (const auto found = deserialized.find(id); found != deserialized.end())
        {
            const auto& entry = found->second;
            if (entry.type != type)
            {
                throw CannotDeserialize("Shared pointer id {} reused with incompatible pointee type", id);
            }
            if (entry.serializedValue != serializedValue)
            {
                throw CannotDeserialize("Conflicting definition for shared pointer id {}", id);
            }
            if (!entry.owner)
            {
                throw CannotDeserialize("Cycles in shared pointer reflection are not supported");
            }
            return std::static_pointer_cast<T>(entry.owner);
        }

        deserialized.emplace(id, Entry{.owner = nullptr, .type = type, .serializedValue = serializedValue});
        try
        {
            std::shared_ptr<T> result = std::invoke(std::forward<Factory>(factory));
            if (!result)
            {
                throw CannotDeserialize("Shared pointer construction returned null for id {}", id);
            }
            deserialized.at(id).owner = std::move(result);
            return std::static_pointer_cast<T>(deserialized.at(id).owner);
        }
        catch (...)
        {
            deserialized.erase(id);
            throw;
        }
    }

    /// Return completed, unique objects whose exact unqualified pointee type is T.
    template <typename T>
    [[nodiscard]] std::vector<std::shared_ptr<T>> getObjects() const
    {
        using Pointee = std::remove_const_t<T>;
        std::vector<std::shared_ptr<T>> objects;
        for (const auto& [id, entry] : deserialized)
        {
            static_cast<void>(id);
            if (entry.owner && entry.type == std::type_index{typeid(Pointee)})
            {
                objects.push_back(std::static_pointer_cast<T>(entry.owner));
            }
        }
        return objects;
    }
};

}
