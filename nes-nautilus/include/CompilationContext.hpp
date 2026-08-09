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
#include <type_traits>
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

namespace detail
{
/// Extracts the nautilus signature R(Args...) from the std::function type that CTAD deduces for a callable.
/// The deduced parameter types keep the callable's cv/ref qualifiers (e.g. const val<T>&), but the traced
/// signature must name the plain value types, so they are stripped here.
template <typename>
struct TracedSignature;

template <typename R, typename... Args>
struct TracedSignature<std::function<R(Args...)>>
{
    using type = R(std::remove_cvref_t<Args>...);
};
}

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
    /// Type-erased owners of the functions handed out by registerTracedFunction(), keyed by their name.
    /// In interpreted mode this map is looked up at runtime by every worker thread executing the pipeline, so it
    /// is guarded. The map is node-based, hence the references handed out stay valid past the lock.
    /// The name needs to be unique per pipeline.
    folly::Synchronized<std::unordered_map<std::string, std::shared_ptr<void>>> nautilusFunctions;
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
    /// The name is the identity: nautilus itself interns traced functions by name while tracing, so two bodies
    /// registered under one name collapse into the first one, silently. The name must therefore encode everything
    /// the body captures -- not only its signature, but every configuration value baked into it.
    ///
    /// The functions live as long as this context, i.e. as long as the pipeline stage: in interpreted mode the
    /// bodies are not compiled but invoked directly, every time the pipeline runs. That is also why, unlike
    /// registerFunction(), this may be called after the module has been compiled -- it hands out nothing that
    /// would still need resolving, and why it must be safe to call from several worker threads at once.
    template <typename Signature>
    nautilus::NautilusFunction<std::function<Signature>>& registerTracedFunction(const std::string& name, std::function<Signature> body)
    {
        using Function = nautilus::NautilusFunction<std::function<Signature>>;
        auto locked = nautilusFunctions.wlock();
        auto& stored = (*locked)[name];
        if (stored == nullptr)
        {
            /// NautilusFunction is neither copyable nor movable, so it is constructed in place and kept behind a
            /// type-erased shared_ptr, which retains the deleter of the concrete type.
            stored = std::make_shared<Function>(name, std::move(body));
        }
        return *std::static_pointer_cast<Function>(stored);
    }

    /// Overload of the above that deduces the signature from the callable's call operator, so that the signature
    /// is spelled exactly once: in the parameter list of the body itself. Requires a non-generic callable.
    template <typename Callable>
    requires requires(Callable callable) { std::function{std::move(callable)}; }
    auto& registerTracedFunction(const std::string& name, Callable body)
    {
        using DeducedStdFunction = decltype(std::function{std::declval<Callable>()});
        using Signature = typename detail::TracedSignature<DeducedStdFunction>::type;
        return registerTracedFunction<Signature>(name, std::function<Signature>{std::move(body)});
    }

    /// Returns the pipeline's traced function (see registerTracedFunction) whose body is a single
    /// nautilus::invoke of ProxyFunction. The traced signature val<R>(val<Args>...) is derived from the proxy's
    /// C++ signature, so the common "shared traced function that only forwards to a proxy" pattern spells no
    /// signature at all. Passing no attributes is identical to a plain nautilus::invoke of the proxy.
    template <auto ProxyFunction>
    auto& registerTracedInvoke(const std::string& name, const nautilus::FunctionAttributes attributes = {})
    {
        return registerTracedInvokeImpl<ProxyFunction>(name, attributes, ProxyFunction);
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

private:
    /// The proxy's C++ signature is taken apart by deducing against the function pointer passed as third argument.
    template <auto ProxyFunction, typename R, typename... ProxyArguments>
    auto& registerTracedInvokeImpl(const std::string& name, const nautilus::FunctionAttributes attributes, R (*)(ProxyArguments...))
    {
        static_assert(not std::is_void_v<R>, "registerTracedInvoke does not support void-returning proxies yet");
        using Signature = nautilus::val<R>(nautilus::val<ProxyArguments>...);
        return registerTracedFunction<Signature>(
            name,
            [attributes](const nautilus::val<ProxyArguments>&... arguments)
            { return nautilus::invoke(attributes, ProxyFunction, arguments...); });
    }
};
}
