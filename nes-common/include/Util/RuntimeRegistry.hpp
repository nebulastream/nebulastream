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
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Util/Strings.hpp>

namespace NES
{

/// A registry whose entries are registered at runtime (static initialization time) by
/// auto-generated per-plugin glue translation units. The linker keeps
/// the glue TUs alive via --whole-archive applied to a dedicated glue sub-library
/// (see cmake/RuntimeRegistrationUtil.cmake).
///
/// EntryTypeT is arbitrary: a factory std::function, a struct bundling several
/// functions, or a plain value. Concrete registries derive via CRTP so instance()
/// returns the concrete type and custom convenience members can be added:
///
///     class MyRegistry : public RuntimeRegistry<MyRegistry, std::string, MyEntry> {};
///
/// If CaseSensitive is false and the key type is string-like, keys are normalized to
/// upper case on insertion and lookup (mirrors the behavior of Identifier).
template <typename ConcreteRegistry, typename KeyTypeT, typename EntryTypeT, bool CaseSensitive = true>
class RuntimeRegistry
{
    KeyTypeT internalKey(const KeyTypeT& externalKey) const
    {
        if constexpr (
            !CaseSensitive && std::convertible_to<KeyTypeT, std::string_view> && std::constructible_from<KeyTypeT, std::string_view>)
        {
            return KeyTypeT{toUpperCase(static_cast<std::string_view>(externalKey))};
        }
        return externalKey;
    }

protected:
    RuntimeRegistry() = default;

public:
    using KeyType = KeyTypeT;
    using EntryType = EntryTypeT;

    RuntimeRegistry(const RuntimeRegistry&) = delete;
    RuntimeRegistry(RuntimeRegistry&&) noexcept = delete;
    RuntimeRegistry& operator=(const RuntimeRegistry&) = delete;
    RuntimeRegistry& operator=(RuntimeRegistry&&) noexcept = delete;
    ~RuntimeRegistry() = default;

    static ConcreteRegistry& instance()
    {
        static ConcreteRegistry inst;
        return inst;
    }

    /// Register an entry. Returns false if an entry with this key already exists.
    /// Should be fatal if the entry is not optional.
    [[nodiscard]] bool addEntry(const KeyTypeT& name, EntryTypeT entry)
    {
        return entries.try_emplace(internalKey(name), std::move(entry)).second;
    }

    [[nodiscard]] bool contains(const KeyTypeT& name) const { return entries.contains(internalKey(name)); }

    /// Returns a copy of the registered entry, or nullopt if the key is unknown.
    [[nodiscard]] std::optional<EntryTypeT> find(const KeyTypeT& name) const
    {
        if (const auto it = entries.find(internalKey(name)); it != entries.end())
        {
            return it->second;
        }
        return std::nullopt;
    }

    [[nodiscard]] std::vector<KeyTypeT> getRegisteredNames() const
    {
        std::vector<KeyTypeT> names;
        names.reserve(entries.size());
        std::ranges::transform(entries, std::back_inserter(names), [](const auto& kv) { return kv.first; });
        return names;
    }

private:
    std::unordered_map<KeyTypeT, EntryTypeT> entries;

    friend ConcreteRegistry;
};

}
