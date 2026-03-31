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
#include <memory>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <val_concepts.hpp>

namespace NES
{

class CompilationContext
{
    const nautilus::engine::NautilusEngine& engine; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& operatorHandlers;

public:
    explicit CompilationContext(
        const nautilus::engine::NautilusEngine& engine,
        const std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& operatorHandlers)
        : engine(engine), operatorHandlers(operatorHandlers)
    {
    }

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

    [[nodiscard]] OperatorHandler* getGlobalOperatorHandler(const OperatorHandlerId operatorHandlerId) const
    {
        const auto it = operatorHandlers.find(operatorHandlerId);
        PRECONDITION(it != operatorHandlers.end(), "Expected operator handler for {}", operatorHandlerId);
        PRECONDITION(it->second != nullptr, "Expected operator handler for {}", operatorHandlerId);
        return it->second.get();
    }
};
}
