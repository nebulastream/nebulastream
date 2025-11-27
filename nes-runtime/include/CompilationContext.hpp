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
#include <Engine.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Similar to the execution context, this class provides access to functionality for compiling code in a pipeline.
class CompilationContext
{
    /// We assume that a compilation context always outlives the engine
    const nautilus::engine::NautilusEngine& engine; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)

public:
    explicit CompilationContext(const nautilus::engine::NautilusEngine& engine) : engine(engine) { }

    template <typename R, typename... FunctionArguments>
    auto registerFunction(R (*fnptr)(nautilus::val<FunctionArguments>...)) const
    {
        return engine.registerFunction<R, FunctionArguments...>(fnptr);
    }

    template <typename R, typename... FunctionArguments>
    auto registerFunction(std::function<R(nautilus::val<FunctionArguments>...)> func) const
    {
        return engine.registerFunction<R, FunctionArguments...>(func);
    }
};
}
