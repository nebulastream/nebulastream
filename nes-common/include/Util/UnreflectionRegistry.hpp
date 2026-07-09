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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// Outcome of registering a per-plugin unreflector. The generated glue keys its reaction off this:
/// only a ConflictingPlugin is a real error worth aborting on.
enum class UnreflectorRegistrationOutcome : std::uint8_t
{
    /// First registration of this key.
    Registered,
    /// The key is already bound to the *same* plugin identity. This is benign: the whole-archive
    /// glue sub-library can legitimately land on a final link line more than once (CMake does not
    /// reliably de-duplicate $<LINK_LIBRARY:WHOLE_ARCHIVE,...> across transitive link paths), so
    /// the same static initializer runs more than once. The re-registration is ignored.
    DuplicateIdenticalPlugin,
    /// The key is already bound to a *different* plugin identity, i.e. two distinct plugins claim
    /// the same serialized name. This is a genuine conflict.
    ConflictingPlugin,
};

/// The unreflection registry is a standalone counterpart to BaseRegistry that dispatches
/// deserialization by name without compile-time knowledge of the concrete plugin types.
/// Entries are populated by per-plugin glue translation units at static initialization
/// time; the linker keeps those TUs alive via --whole-archive applied to a dedicated glue
/// sub-library (see cmake/UnreflectionRegistrationUtil.cmake).
template <typename ConcreteRegistry, typename KeyTypeT, typename ReturnTypeT>
class UnreflectionRegistry
{
protected:
    UnreflectionRegistry() = default;

public:
    using KeyType = KeyTypeT;
    using ReturnType = ReturnTypeT;
    using UnreflectorFn = std::function<ReturnTypeT(const Reflected&, const ReflectionContext&)>;

    UnreflectionRegistry(const UnreflectionRegistry&) = delete;
    UnreflectionRegistry(UnreflectionRegistry&&) noexcept = delete;
    UnreflectionRegistry& operator=(const UnreflectionRegistry&) = delete;
    UnreflectionRegistry& operator=(UnreflectionRegistry&&) noexcept = delete;
    ~UnreflectionRegistry() = default;

    static ConcreteRegistry& instance()
    {
        static ConcreteRegistry inst;
        return inst;
    }

    /// Register an unreflector under `name`, tagged with `pluginIdentity` (a stable string that is
    /// unique per plugin, e.g. its C++ type). The outcome lets the caller distinguish a benign
    /// re-registration of the *same* plugin (which happens when the whole-archive glue archive is
    /// linked more than once) from a genuine key collision between two *different* plugins. The
    /// generated glue only aborts on ConflictingPlugin; a runtime plugin-loading path could instead
    /// roll back or report. `name` is taken by reference so the caller can still inspect it on the
    /// failure path regardless of how the underlying map handles its arguments.
    [[nodiscard]] UnreflectorRegistrationOutcome
    addUnreflectorEntry(const KeyTypeT& name, const std::string_view pluginIdentity, UnreflectorFn unreflectorFunction)
    {
        const auto [it, inserted] = unreflectorTable.try_emplace(name, Entry{std::move(unreflectorFunction), std::string{pluginIdentity}});
        if (inserted)
        {
            return UnreflectorRegistrationOutcome::Registered;
        }
        return std::string_view{it->second.identity} == pluginIdentity ? UnreflectorRegistrationOutcome::DuplicateIdenticalPlugin
                                                                       : UnreflectorRegistrationOutcome::ConflictingPlugin;
    }

    [[nodiscard]] bool contains(const KeyTypeT& name) const { return unreflectorTable.contains(name); }

    [[nodiscard]] std::optional<ReturnTypeT> unreflect(const KeyTypeT& name, const Reflected& data, const ReflectionContext& context) const
    {
        if (const auto it = unreflectorTable.find(name); it != unreflectorTable.end())
        {
            return it->second.unreflector(data, context);
        }
        return std::nullopt;
    }

private:
    struct Entry
    {
        UnreflectorFn unreflector;
        std::string identity;
    };

    std::unordered_map<KeyTypeT, Entry> unreflectorTable;

    friend ConcreteRegistry;
};

}
