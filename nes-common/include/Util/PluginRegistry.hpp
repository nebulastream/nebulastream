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
#include <functional>
#include <unordered_map>
#include <utility>
namespace NES
{

/// The registry singleton allows registration of factory methods to produce a certain type.
/// There exists multiple distinct registries for different types.
template <typename Registrar>
class Registry
{
public:
    /// Cannot copy and move
    Registry(const Registry& other) = delete;
    Registry(Registry&& other) noexcept = delete;
    Registry& operator=(const Registry& other) = delete;
    Registry& operator=(Registry&& other) noexcept = delete;
    ~Registry() = default;

    [[nodiscard]] bool contains(const typename Registrar::Key& key) const { return registryImpl.contains(key); }

    template <typename... Args>
    [[nodiscard]] typename Registrar::Type create(const typename Registrar::Key& key, Args... args) const
    {
        return registryImpl.at(key)(args...);
    }

    [[nodiscard]] std::vector<typename Registrar::Key> getRegisteredNames() const
    {
        std::vector<typename Registrar::Key> names;
        names.reserve(registryImpl.size());
        std::ranges::transform(registryImpl, std::back_inserter(names), [](const auto& kv) { return kv.first; });
        return names;
    }

protected:
    /// A single registry will be constructed in the static instance() method. It is impossible to create a registry otherwise.
    Registry() { Registrar::registerAll(*this); }

private:
    /// Only the Registrar can register new plugins.
    void registerPlugin(typename Registrar::Key key, typename Registrar::CreatorFn fn)
    {
        registryImpl.emplace(std::move(key), std::move(fn));
    }
    friend Registrar;

    std::unordered_map<typename Registrar::Key, typename Registrar::CreatorFn> registryImpl;
};

template <typename K, typename BaseType, typename... Args>
class Registrar
{
    using Key = K;
    using Type = std::unique_ptr<BaseType>;
    using CreatorFn = std::function<Type(Args...)>;
    static void registerAll(Registry<Registrar>& registry);
    friend class Registry<Registrar>;
};

static_assert(!std::copy_constructible<Registry<Registrar<std::string, std::string>>>);
static_assert(!std::move_constructible<Registry<Registrar<std::string, std::string>>>);
static_assert(!std::is_copy_assignable_v<Registry<Registrar<std::string, std::string>>>);
static_assert(!std::is_move_assignable_v<Registry<Registrar<std::string, std::string>>>);
static_assert(!std::is_default_constructible_v<Registry<Registrar<std::string, std::string>>>);

/// CRTPBase of the Registry. This allows the `instance()` method to return a concrete instance of the registry, which is useful
/// if custom member functions are added to the concrete registry class.
template <typename ConcreteRegistry, typename KeyType, typename Type, typename... Args>
class BaseRegistry : public Registry<Registrar<KeyType, Type, Args...>>
{
public:
    static ConcreteRegistry& instance()
    {
        static ConcreteRegistry instance;
        return instance;
    }
};
}
