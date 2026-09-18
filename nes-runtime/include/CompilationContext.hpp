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
#include <memory>
#include <unordered_map>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <fmt/format.h>
#include <nautilus/RuntimeBinding.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

using OperatorHandlerBindings = std::unordered_map<OperatorHandlerId, nautilus::RuntimeBinding<OperatorHandler>>;

/// Similar to the execution context, this class provides access to functionality for compiling code in a pipeline.
class CompilationContext
{
public:
    CompilationContext(
        PipelineExecutionContext& pipelineExecutionContext,
        const std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& operatorHandlers,
        nautilus::RuntimeBindings& runtimeBindings,
        OperatorHandlerBindings& operatorHandlerBindings)
        : pipelineExecutionContext(pipelineExecutionContext)
        , runtimeBindings(runtimeBindings)
        , operatorHandlers(operatorHandlers)
        , operatorHandlerBindings(operatorHandlerBindings)
    {
    }

    void registerOperatorHandler(const OperatorHandlerId handlerId)
    {
        if (!operatorHandlerBindings.contains(handlerId))
        {
            auto* handler = operatorHandlers.at(handlerId).get();
            operatorHandlerBindings.emplace(
                handlerId, runtimeBindings.bind<OperatorHandler>(fmt::format("handler/{}", operatorHandlerBindings.size()), handler));
        }
    }

    PipelineExecutionContext& pipelineExecutionContext;
    nautilus::RuntimeBindings& runtimeBindings;
    uint64_t runtimeBindingCounter = 0;

private:
    const std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& operatorHandlers;
    OperatorHandlerBindings& operatorHandlerBindings;
};
}
