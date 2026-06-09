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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <fmt/format.h>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <val_concepts.hpp>

namespace NES
{

class CompilationContext;

/// Handle to a function that an operator registered in its pipeline's nautilus module during setup().
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
/// afterwards resolveAfterCompilation() makes the handles returned to the operators invocable.
class CompilationContext
{
    /// We assume that a compilation context never outlives the module; both live in CompiledExecutablePipelineStage::start()
    nautilus::engine::NautilusModule& module; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::vector<std::function<void(nautilus::engine::CompiledModule&)>> pendingResolvers;
    uint64_t functionNameCounter = 0;

public:
    explicit CompilationContext(nautilus::engine::NautilusModule& module) : module(module) { }

    template <typename R, typename... FunctionArguments>
    auto
    registerFunction(std::function<R(nautilus::val<FunctionArguments>...)> func, const std::string_view namePrefix = "operatorFunction")
    {
        using RawR = nautilus::engine::details::raw_return_type_t<R>;
        using Handle = PipelineFunction<RawR(FunctionArguments...)>;

        /// The counter guarantees unique names within the module. It can never collide with the name of the
        /// main pipeline function, which carries no counter suffix.
        auto state = std::make_shared<typename Handle::SharedState>();
        state->name = fmt::format("{}_{}", namePrefix, functionNameCounter++);
        module.registerFunction(state->name, std::move(func));
        pendingResolvers.emplace_back([state](nautilus::engine::CompiledModule& compiledModule)
                                      { state->function = compiledModule.getFunction<RawR(FunctionArguments...)>(state->name); });
        return Handle(std::move(state));
    }

    template <typename R, typename... FunctionArguments>
    auto registerFunction(R (*fnptr)(nautilus::val<FunctionArguments>...), const std::string_view namePrefix = "operatorFunction")
    {
        return registerFunction(std::function<R(nautilus::val<FunctionArguments>...)>(fnptr), namePrefix);
    }

    /// Called by CompiledExecutablePipelineStage once, directly after compiling the pipeline's module.
    void resolveAfterCompilation(nautilus::engine::CompiledModule& compiledModule)
    {
        for (const auto& resolver : pendingResolvers)
        {
            resolver(compiledModule);
        }
        pendingResolvers.clear();
    }
};
}
