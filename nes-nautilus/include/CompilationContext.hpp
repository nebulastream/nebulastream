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
#include <unordered_map>
#include <utility>
#include <vector>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <nautilus/common/FunctionAttributes.hpp>
#include <nautilus/function.hpp>
#include <nautilus/nautilus_function.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <Module.hpp>
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

    /// A function handed out by registerTracedFunction(), plus an address identifying the body it came from.
    /// A null identity disables the collision check.
    struct StoredFunction
    {
        std::shared_ptr<void> function;
        const void* bodyIdentity = nullptr;
    };

    /// The functions handed out by registerTracedFunction(), keyed by their name, which must be unique per
    /// pipeline. In interpreted mode every worker thread looks this up at runtime, hence the guard. The map is
    /// node-based, so the references handed out stay valid past the lock.
    folly::Synchronized<std::unordered_map<std::string, StoredFunction>> nautilusFunctions;
    uint64_t functionNameCounter = 0;
    /// Set once resolveAfterCompilation() has run; registering further functions afterwards would append a resolver
    /// that never runs, leaving its handle permanently unresolved, so it is a precondition violation.
    bool compiled = false;

public:
    explicit CompilationContext(nautilus::engine::NautilusModule& module) : module(module) { }

    template <typename R, typename... FunctionArguments>
    auto registerFunction(std::function<R(nautilus::val<FunctionArguments>...)> func, const std::string_view namePrefix)
    {
        PRECONDITION(!compiled, "registerFunction() must not be called after the module has been compiled");
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
    auto registerFunction(std::function<R(nautilus::val<FunctionArguments>...)> func)
    {
        return registerFunction(std::move(func), "operatorFunction");
    }

    template <typename R, typename... FunctionArguments>
    auto registerFunction(R (*fnptr)(nautilus::val<FunctionArguments>...), const std::string_view namePrefix)
    {
        return registerFunction(std::function<R(nautilus::val<FunctionArguments>...)>(fnptr), namePrefix);
    }

    template <typename R, typename... FunctionArguments>
    auto registerFunction(R (*fnptr)(nautilus::val<FunctionArguments>...))
    {
        return registerFunction(std::function<R(nautilus::val<FunctionArguments>...)>(fnptr), "operatorFunction");
    }

    /// Returns the pipeline's nautilus function of the given name, creating it from body on first use.
    /// In contrast to registerFunction(), the returned function is called from *inside* traced code: its body is
    /// traced once into the pipeline's module and called instead of being inlined at every call site. Operators
    /// that would otherwise inline the same body N times per pipeline (N aggregations, N joins, ...) derive a
    /// name from the shape of the body and share the single instantiation this hands out.
    ///
    /// The name is the identity: nautilus interns traced functions by name, so two bodies registered under one
    /// name silently collapse into the first. The name must encode every configuration value the body bakes in.
    /// Pass 'bodyIdentity' -- an address that differs whenever the body does, e.g. the proxy's -- to have that
    /// mistake caught here instead of miscompiling the call sites that lose.
    ///
    /// The functions live as long as this context. In interpreted mode the bodies are invoked directly every time
    /// the pipeline runs, which is why this may be called after compilation and from several worker threads.
    template <typename Signature>
    nautilus::NautilusFunction<std::function<Signature>>&
    /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations) the identity is opt-in, so callers that cannot supply one omit it
    registerTracedFunction(const std::string& name, std::function<Signature> body, const void* bodyIdentity = nullptr)
    {
        using Function = nautilus::NautilusFunction<std::function<Signature>>;

        /// In interpreted mode this runs per field per record on every worker, and the function is registered by
        /// then. Serve that case under the read lock so the workers do not serialize on a write lock.
        {
            auto shared = nautilusFunctions.rlock();
            if (const auto stored = shared->find(name); stored != shared->end())
            {
                INVARIANT(
                    stored->second.bodyIdentity == bodyIdentity,
                    "Nautilus function '{}' is already registered with a different body. Nautilus interns traced functions by "
                    "name and would silently drop this one, so the name must encode every value the body bakes in.",
                    name);
                return *std::static_pointer_cast<Function>(stored->second.function);
            }
        }

        auto locked = nautilusFunctions.wlock();
        auto& stored = (*locked)[name];
        if (stored.function == nullptr)
        {
            /// NautilusFunction is neither copyable nor movable, so it is constructed in place and kept behind a
            /// type-erased shared_ptr, which retains the deleter of the concrete type.
            stored = StoredFunction{.function = std::make_shared<Function>(name, std::move(body)), .bodyIdentity = bodyIdentity};
        }
        else
        {
            INVARIANT(
                stored.bodyIdentity == bodyIdentity,
                "Nautilus function '{}' is already registered with a different body. Nautilus interns traced functions by "
                "name and would silently drop this one, so the name must encode every value the body bakes in.",
                name);
        }
        return *std::static_pointer_cast<Function>(stored.function);
    }

    /// registerTracedFunction() for a body that is nothing but a single nautilus::invoke: signature and body are
    /// both derived from the proxy, so the signature is spelled out once. Anything the shared function should do
    /// besides the call belongs in the proxy, where it is plain C++ and costs no trace operations.
    template <typename R, typename... Args>
    /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations) almost every proxy wants the default attributes
    auto& registerTracedInvoke(const std::string& name, R (*proxy)(Args...), const nautilus::FunctionAttributes attributes = {})
    {
        using Signature = nautilus::val<R>(nautilus::val<Args>...);
        return registerTracedFunction<Signature>(
            name,
            [proxy, attributes](const nautilus::val<Args>&... args) { return nautilus::invoke(attributes, proxy, args...); },
            reinterpret_cast<const void*>(proxy));
    }

    /// Called by CompiledExecutablePipelineStage once, directly after compiling the pipeline's module.
    void resolveAfterCompilation(nautilus::engine::CompiledModule& compiledModule)
    {
        for (const auto& resolver : pendingResolvers)
        {
            resolver(compiledModule);
        }
        pendingResolvers.clear();
        compiled = true;
    }
};

/// Memoizes the nautilus function that registerTracedInvoke() hands out for one proxy, so that a caller running
/// per field per record does not repeat the name hash and the read lock.
/// resolve() is called from the pipeline's setup(), which runs once, on one thread, before any worker executes the
/// pipeline; get() only ever reads. That publication-before-threads ordering is the whole thread-safety argument --
/// no atomics are involved.
/// A context other than the resolved one falls through to the registry rather than trusting the cached pointer: a
/// stage restart rebuilds the CompilationContext, possibly at the same address, and formatters driven directly by a
/// test never see a setup() at all. Both must keep working, so this is deliberately not an INVARIANT.
template <typename Proxy>
class TracedInvokeMemo;

template <typename R, typename... Args>
class TracedInvokeMemo<R (*)(Args...)>
{
    using Function = nautilus::NautilusFunction<std::function<nautilus::val<R>(nautilus::val<Args>...)>>;

    Function* function = nullptr;
    const CompilationContext* resolvedAgainst = nullptr;

public:
    void resolve(CompilationContext& compilationContext, const std::string& name, R (*proxy)(Args...))
    {
        function = std::addressof(compilationContext.registerTracedInvoke(name, proxy));
        resolvedAgainst = std::addressof(compilationContext);
    }

    [[nodiscard]] Function& get(CompilationContext& compilationContext, const std::string& name, R (*proxy)(Args...)) const
    {
        return resolvedAgainst == std::addressof(compilationContext) ? *function : compilationContext.registerTracedInvoke(name, proxy);
    }
};
}
