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
#include <optional>
#include <unordered_map>
#include <utility>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

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

    /// Register an unreflector. Returns false if an entry with this key already exists; the
    /// caller decides how to react. The generated glue throws at static initialization time so
    /// duplicates are loud and unrecoverable, while a runtime plugin-loading path can roll back
    /// or report the conflict instead of leaving the registry in a half-initialized state.
    /// `name` is taken by reference so the caller can still inspect it on the failure path
    /// regardless of how the underlying map handles its arguments.
    [[nodiscard]] bool addUnreflectorEntry(const KeyTypeT& name, UnreflectorFn unreflectorFunction)
    {
        return unreflectorTable.try_emplace(name, std::move(unreflectorFunction)).second;
    }

    [[nodiscard]] bool contains(const KeyTypeT& name) const { return unreflectorTable.contains(name); }

    [[nodiscard]] std::optional<ReturnTypeT> unreflect(const KeyTypeT& name, const Reflected& data, const ReflectionContext& context) const
    {
        if (const auto it = unreflectorTable.find(name); it != unreflectorTable.end())
        {
            return it->second(data, context);
        }
        return std::nullopt;
    }

private:
    std::unordered_map<KeyTypeT, UnreflectorFn> unreflectorTable;

    friend ConcreteRegistry;
};

}
