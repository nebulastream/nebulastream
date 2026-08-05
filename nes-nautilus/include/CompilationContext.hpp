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

#include <any>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <fmt/format.h>
#include <CompilationStatistics.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <Module.hpp>
#include <val_concepts.hpp>

namespace NES
{

class CompilationContext;

/// Handle to a function registered in a nautilus module.
/// All functions of a pipeline are compiled together into exactly one module, so the handle only becomes
/// invocable once the pipeline stage has compiled that module; the CompilationContext resolves all handles
/// directly after compilation. Invoking a handle before that is a logic error.
/// Copies share the resolution state. The contained ModuleFunction co-owns the compiled executable
/// (nautilus shares the module state via shared_ptr), so the handle remains valid even after the
/// CompiledExecutablePipelineStage and its CompiledModule are destroyed.
template <typename Signature>
class PipelineFunction;

template <typename R, typename... Args>
class PipelineFunction<R(Args...)>
{
    struct SharedState
    {
        std::string name;
        std::optional<nautilus::engine::ModuleFunction<R(Args...)>> function;
    };

    explicit PipelineFunction(std::shared_ptr<SharedState> state) : state(std::move(state)) { }

    std::shared_ptr<SharedState> state;

    friend class CompilationContext;

public:
    R operator()(Args... args) const
    {
        INVARIANT(state->function.has_value(), "Nautilus function '{}' was invoked before its pipeline module was compiled", state->name);
        return (*state->function)(std::forward<Args>(args)...);
    }
};

/// Similar to the execution context, this class provides access to functionality for compiling code in a pipeline.
/// It builds the single nautilus module of a pipeline: operators register named functions during setup(), the
/// pipeline stage adds the main pipeline function and compiles all of them together with one compile() call, and
/// compile() makes the handles returned to the operators invocable. The handles co-own the compiled module state,
/// so this context may be destroyed after compilation.
class CompilationContext
{
    nautilus::engine::NautilusModule module;
    std::optional<nautilus::engine::CompiledModule> compiledModule;
    std::vector<std::function<void(nautilus::engine::CompiledModule&)>> pendingResolvers;
    std::unordered_map<std::string, std::any> cache;
    uint64_t functionNameCounter = 0;
    /// Set once compile() has run; registering further functions afterwards would append a resolver
    /// that never runs, leaving its handle permanently unresolved, so it is a precondition violation.
    bool compiled = false;

    template <typename R, typename... FunctionArguments>
    auto registerUncachedFunction(std::function<R(nautilus::val<FunctionArguments>...)> func, const std::string_view namePrefix)
    {
        using RawR = nautilus::engine::details::raw_return_type_t<R>;
        using Handle = PipelineFunction<RawR(FunctionArguments...)>;

        auto state = std::make_shared<typename Handle::SharedState>();
        state->name = fmt::format("{}_{}", namePrefix, functionNameCounter++);
        module.registerFunction(state->name, std::move(func));
        pendingResolvers.emplace_back([state](nautilus::engine::CompiledModule& compiledModule)
                                      { state->function = compiledModule.getFunction<RawR(FunctionArguments...)>(state->name); });
        return Handle(std::move(state));
    }

public:
    explicit CompilationContext(nautilus::engine::NautilusModule module) : module(std::move(module)) { }

    /// Registers a function once per key in this pipeline module. Reusing a key returns the first handle,
    /// so every caller must derive a unique key from the function's signature and semantics.
    template <typename R, typename... FunctionArguments>
    auto registerFunction(std::function<R(nautilus::val<FunctionArguments>...)> func, const std::string_view key)
    {
        PRECONDITION(!compiled, "registerFunction() must not be called after the module has been compiled");
        PRECONDITION(!key.empty(), "Registered function key must not be empty");

        using RawR = nautilus::engine::details::raw_return_type_t<R>;
        using Handle = PipelineFunction<RawR(FunctionArguments...)>;

        const auto keyString = std::string{key};
        if (const auto existing = cache.find(keyString); existing != cache.end())
        {
            const auto handle = std::any_cast<Handle>(&existing->second);
            PRECONDITION(handle != nullptr, "Compilation cache key '{}' was reused for a different type", key);
            return *handle;
        }

        /// Function names only need to be unique within the module. The semantic key remains in the cache and may
        /// contain schema punctuation that is unsuitable for a backend symbol name.
        auto handle = registerUncachedFunction(std::move(func), "cachedFunction");
        cache.emplace(keyString, handle);
        return handle;
    }

    template <typename R, typename... FunctionArguments>
    auto registerFunction(R (*fnptr)(nautilus::val<FunctionArguments>...), const std::string_view key)
    {
        return registerFunction(std::function<R(nautilus::val<FunctionArguments>...)>(fnptr), key);
    }

    /// Compiles all registered functions and resolves every handle returned by registerFunction().
    void compile()
    {
        PRECONDITION(!compiled, "CompilationContext::compile() must only be called once");
        compiledModule.emplace(module.compile());
        for (const auto& resolver : pendingResolvers)
        {
            resolver(*compiledModule);
        }
        pendingResolvers.clear();
        compiled = true;
    }

    [[nodiscard]] std::shared_ptr<const nautilus::compiler::CompilationStatistics> getStatistics() const
    {
        PRECONDITION(compiledModule.has_value(), "Compilation statistics are unavailable before compile()");
        return compiledModule->getStatistics();
    }
};
}
