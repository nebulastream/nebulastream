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
#include <nautilus/Engine.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Base function wrapper class
struct FunctionWrapperBase
{
    FunctionWrapperBase() = default;
    virtual ~FunctionWrapperBase() = default;
};

/// Function wrapper class so that we can store multiple different nautilus functions in a map
template <typename R, typename... FunctionArguments>
class CompiledFunctionWrapper final : public FunctionWrapperBase
{
    nautilus::engine::CallableFunction<R, FunctionArguments...> func;

public:
    explicit CompiledFunctionWrapper(nautilus::engine::CallableFunction<R, FunctionArguments...>&& function)
        : FunctionWrapperBase(), func(std::move(function))
    {
    }
    ~CompiledFunctionWrapper() override = default;

    template <typename... Args>
    requires std::is_void_v<R>
    void callCompiledFunction(Args... arguments)
    {
        func(std::forward<Args>(arguments)...);
    }

    template <typename... Args>
    requires (not std::is_void_v<R>)
    R callCompiledFunction(Args... arguments)
    {
        return func(std::forward<Args>(arguments)...);
    }
};


/// Stores compiled nautilus function for this particular pipeline during the tracing of a pipeline.
/// This class should only change its state during the tracing of a pipeline and assumes it lives as long as the pipeline.
/// The idea behind this class is that sometimes one wants to create nautilus function that are getting called during the query lifetime at
/// specific times. For example a cleanup function, once a slice destructor gets called.
class CompilationContext
{
    std::vector<std::shared_ptr<FunctionWrapperBase>> compiledFunctions;
    std::shared_ptr<nautilus::engine::NautilusEngine> engine;

public:
    explicit CompilationContext(std::shared_ptr<nautilus::engine::NautilusEngine> engine) : engine(std::move(engine)) { }

    /// @brief Checks if the function has been seen, if not the method compiles the function and stores the compiled function into the storage
    /// @return the compiled function
    template <typename R, typename... FunctionArguments>
    nautilus::val<CompiledFunctionWrapper<R, FunctionArguments...>*> registerFunction(std::function<R(nautilus::val<FunctionArguments...>)> function)
    {
        /// Passing the function to the nautilus engine so that we can compile/register the function and get a callable back
        auto compiledFunction = engine->registerFunction(function);
        auto compiledFunctionInWrapper = std::make_shared<CompiledFunctionWrapper<R, FunctionArguments...>>(std::move(compiledFunction));
        compiledFunctions.emplace_back(std::move(compiledFunctionInWrapper));
        const auto compiledFunctionBack = compiledFunctions.back();
        const auto castedFunction = dynamic_cast<CompiledFunctionWrapper<R, FunctionArguments...>*>(compiledFunctionBack.get());
        return castedFunction;
    }
};


// /// Nautilus wrapper around the @class CompiledFunctionStore. In the future, we might add more functions to this class.
// struct CompilationContext {
//     nautilus::val<CompiledFunctionStore*> compiledFunctionStore;
// };

}
