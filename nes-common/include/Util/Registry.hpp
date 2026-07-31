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
#include <algorithm>
#include <concepts>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Util/Strings.hpp>

namespace NES
{

/// The registry singleton allows registration of factory methods to produce a certain type.
/// There exists multiple distinct registries for different types.
template <typename ConcreteRegistry, typename KeyTypeT, typename ReturnTypeT, typename ArgumentsT, bool CaseSensitive = false>
class Registry
{
public:
    using KeyType = KeyTypeT;
    using ReturnType = ReturnTypeT;
    using Arguments = ArgumentsT;
    using CreatorFn = std::function<ReturnType(Arguments)>;

private:
    struct InternalKey
    {
        KeyType key;
        auto operator<=>(const InternalKey&) const = default;

        struct Hash
        {
            std::size_t operator()(const InternalKey& key) const { return std::hash<KeyType>()(key.key); }
        };
    };

    static InternalKey internalKey(const KeyType& externalKey)
    {
        if constexpr (
            !CaseSensitive && std::convertible_to<KeyType, std::string_view> && std::constructible_from<KeyType, std::string_view>)
        {
            return InternalKey{KeyType(toUpperCase(static_cast<std::string_view>(externalKey)))};
        }
        return InternalKey{externalKey};
    }

public:
    /// Cannot copy and move
    Registry(const Registry& other) = delete;
    Registry(Registry&& other) noexcept = delete;
    Registry& operator=(const Registry& other) = delete;
    Registry& operator=(Registry&& other) noexcept = delete;
    ~Registry() = default;

    [[nodiscard]] bool contains(const KeyType& key) const { return registryImpl.contains(internalKey(key)); }

    template <typename CallArguments>
    [[nodiscard]] std::optional<ReturnType> create(const KeyType& key, CallArguments&& args) const
    {
        if (const auto entry = registryImpl.find(internalKey(key)); entry != registryImpl.end())
        {
            /// Call the creator function of the entry.
            return entry->second(std::forward<CallArguments>(args));
        }
        return std::nullopt;
    }

    [[nodiscard]] std::vector<KeyType> getRegisteredNames() const
    {
        std::vector<KeyType> names;
        names.reserve(registryImpl.size());
        std::ranges::transform(registryImpl, std::back_inserter(names), [](const auto& kv) { return kv.first.key; });
        return names;
    }

    static void registerPlugin(KeyType key, CreatorFn creatorFunction)
    {
        instance().registryImpl.emplace(internalKey(std::move(key)), std::move(creatorFunction));
    }

    static ConcreteRegistry& instance()
    {
        static ConcreteRegistry instance;
        return instance;
    }

protected:
    /// A single registry will be constructed in the static instance() method. It is impossible to create a registry otherwise.
    Registry() = default;

private:
    std::unordered_map<InternalKey, CreatorFn, typename InternalKey::Hash> registryImpl;

    friend ConcreteRegistry;
};
}
