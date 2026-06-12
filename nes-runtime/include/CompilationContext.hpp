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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <Interface/CompiledHandle.hpp>
#include <Engine.hpp>
#include <Module.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Per-pipeline compilation context handed to PhysicalOperator::setup().
///
/// Operators call registerFunction() during setup to add helper functions
/// (e.g. hash-map cleanup) to the pipeline's nautilus::engine::NautilusModule.
/// All registrations and the main pipeline function compile together in a
/// single module.compile() call, so they share IR optimization passes and
/// backend invocation. The handle returned here resolves lazily on first
/// invocation once the surrounding pipeline stage has compiled its module.
class CompilationContext
{
public:
    CompilationContext(nautilus::engine::NautilusModule& moduleBuilder, std::shared_ptr<CompiledModuleSlot> slot)
        : moduleBuilder(moduleBuilder), slot(std::move(slot))
    {
    }

    template <typename R, typename... FunctionArguments>
    CompiledHandle<R, FunctionArguments...> registerFunction(R (*fnptr)(nautilus::val<FunctionArguments>...))
    {
        auto name = mintName();
        moduleBuilder.registerFunction(name, fnptr);
        return CompiledHandle<R, FunctionArguments...>(slot, std::move(name));
    }

    template <typename R, typename... FunctionArguments>
    CompiledHandle<R, FunctionArguments...> registerFunction(std::function<R(nautilus::val<FunctionArguments>...)> func)
    {
        auto name = mintName();
        moduleBuilder.registerFunction<R(nautilus::val<FunctionArguments>...)>(name, std::move(func));
        return CompiledHandle<R, FunctionArguments...>(slot, std::move(name));
    }

private:
    std::string mintName() { return "nes_helper_" + std::to_string(nextId.fetch_add(1, std::memory_order_relaxed)); }

    nautilus::engine::NautilusModule& moduleBuilder; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::shared_ptr<CompiledModuleSlot> slot;
    std::atomic<uint64_t> nextId{0};
};
}
