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
#include <memory>
#include <string>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <vector>

namespace NES
{

template <typename T>
struct Unreflector;

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

    template <typename T>
    friend struct Unreflector;

public:
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
