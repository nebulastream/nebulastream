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

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <Engine.hpp>
#include <Module.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Shared slot that holds the eventual nautilus::engine::CompiledModule for a pipeline.
/// Created during pipeline setup, populated once module.compile() finishes.
/// Handles distributed during setup() share this slot via shared_ptr and lazily resolve
/// their ModuleFunction on first invocation.
struct CompiledModuleSlot
{
    std::optional<nautilus::engine::CompiledModule> compiled;
};

/// Stable, copyable, callable handle into a function in a CompiledModuleSlot.
///
/// The slot is populated after all operators have registered their helper functions.
/// On first call, the handle resolves itself to a nautilus::engine::ModuleFunction and
/// caches it. Subsequent calls hit the cached ModuleFunction directly, which itself
/// has lock-free dispatch in the hot path (cf. nautilus Module.hpp).
template <typename R, typename... Args>
class CompiledHandle
{
public:
    CompiledHandle(std::shared_ptr<CompiledModuleSlot> slot, std::string name)
        : slot(std::move(slot)), name(std::move(name))
    {
    }

    R operator()(Args... args) const
    {
        if (not cached.has_value())
        {
            INVARIANT(slot->compiled.has_value(), "Cannot invoke CompiledHandle '{}' before its module has been compiled", name);
            cached.emplace(slot->compiled->template getFunction<R(Args...)>(name));
        }
        return (*cached)(std::forward<Args>(args)...);
    }

private:
    std::shared_ptr<CompiledModuleSlot> slot;
    std::string name;
    mutable std::optional<nautilus::engine::ModuleFunction<R(Args...)>> cached;
};

}
