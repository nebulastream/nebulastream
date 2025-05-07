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
template <typename Registrar, bool CaseSensitive = false>
class Registry
{
    struct InternalKey
    {
        typename Registrar::KeyType key;
        auto operator<=>(const InternalKey&) const = default;
        struct Hash
        {
            std::size_t operator()(const InternalKey& key) const { return std::hash<typename Registrar::KeyType>()(key.key); }
        };
    };

    InternalKey internalKey(const Registrar::KeyType& externalKey) const
    {
        if constexpr (
            !CaseSensitive && std::convertible_to<typename Registrar::KeyType, std::string_view>
            && std::constructible_from<typename Registrar::KeyType, std::string_view>)
        {
            return InternalKey{typename Registrar::KeyType(NES::Util::toUpperCase(static_cast<std::string_view>(externalKey)))};
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

    [[nodiscard]] bool contains(const typename Registrar::KeyType& key) const { return registryImpl.contains(internalKey(key)); }

    template <typename Arguments>
    [[nodiscard]] std::optional<typename Registrar::ReturnType> create(const typename Registrar::KeyType& key, Arguments&& args) const
    {
        if (const auto entry = registryImpl.find(internalKey(key)); entry != registryImpl.end())
        {
            /// Call the creator function of the entry.
            return entry->second(std::forward<Arguments>(args));
        }
        return std::nullopt;
    }

    [[nodiscard]] std::vector<typename Registrar::KeyType> getRegisteredNames() const
    {
        std::vector<typename Registrar::KeyType> names;
        names.reserve(registryImpl.size());
        std::ranges::transform(registryImpl, std::back_inserter(names), [](const auto& kv) { return kv.first.key; });
        return names;
    }

protected:
    /// A single registry will be constructed in the static instance() method. It is impossible to create a registry otherwise.
    Registry() { Registrar::registerAll(*this); }

private:
    /// Only the Registrar can register new entries.
    void addEntry(typename Registrar::KeyType key, typename Registrar::CreatorFn creatorFunction)
    {
        registryImpl.emplace(internalKey(std::move(key)), std::move(creatorFunction));
    }
    friend Registrar;

    std::unordered_map<InternalKey, typename Registrar::CreatorFn, typename InternalKey::Hash> registryImpl;
};

/// Tagging the Registrar avoids the following issue:
/// If the Registrar is not tagged with a ConcreteRegistry and two registries have the same key, return type and arguments, then we would instantiate
/// the same 'Registrar<key, returnType, arguments>' for both Registries. Since the registries are templated on the Registrar, both registries
/// would be instantiated with the same Registrar, leading to the same instantiation of the Registry, leading to the generation of only one
/// implementation for the Registry. If we then call 'instance()' on the first registry and afterward 'instance()' on the second registry,
/// both 'instance()' calls would access the same implementation. Since the registry is a singleton, both would access the first registry.
template <typename ConcreteRegistry, typename KeyTypeT, typename ReturnTypeT, typename Arguments>
class Registrar
{
    using Tag = ConcreteRegistry;
    using KeyType = KeyTypeT;
    using ReturnType = ReturnTypeT;
    using CreatorFn = std::function<ReturnType(Arguments)>;
    static void registerAll(Registry<Registrar>& registry);
    template <typename Registrar, bool>
    friend class Registry;
};

/// CRTPBase of the Registry. This allows the `instance()` method to return a concrete instance of the registry, which is useful
/// if custom member functions are added to the concrete registry class.
template <typename ConcreteRegistry, typename KeyTypeT, typename ReturnTypeT, typename Arguments, bool CaseSensitive = false>
class BaseRegistry : public Registry<Registrar<ConcreteRegistry, KeyTypeT, ReturnTypeT, Arguments>, CaseSensitive>
{
    BaseRegistry() = default;

public:
    static ConcreteRegistry& instance()
    {
        static ConcreteRegistry instance;
        return instance;
    }
    friend ConcreteRegistry;
};
}
