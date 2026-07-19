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

#include <iostream>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/Engine.hpp>
#include <nautilus/Module.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutionContext.hpp>
#include <Pipeline.hpp>

namespace NES
{
class DumpHelper;

/// A compiled executable pipeline stage uses nautilus-lib to compile a pipeline to a code snippet.
/// Each pipeline compiles into exactly one nautilus module that contains the main pipeline function
/// alongside all functions that operators registered during setup().
class CompiledExecutablePipelineStage final : public ExecutablePipelineStage
{
public:
    CompiledExecutablePipelineStage(
        std::shared_ptr<Pipeline> pipeline,
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandler,
        nautilus::engine::Options options);
    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    using PipelineSignature = void(PipelineExecutionContext*, const TupleBuffer*, const Arena*);
    static constexpr std::string_view PIPELINE_FUNCTION_NAME = "execute";

    /// Registers the pipeline's main traced function in the pipeline's module.
    void registerPipelineFunction(nautilus::engine::NautilusModule& module) const;

    nautilus::engine::NautilusEngine engine;
    /// Both are created lazily in start(); neither type is default-constructible.
    std::optional<nautilus::engine::CompiledModule> compiledModule;
    std::optional<nautilus::engine::ModuleFunction<PipelineSignature>> compiledPipelineFunction;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
    /// The executable stage must not retain the compiler's Pipeline DAG. Running pipeline nodes are stopped and
    /// destroyed independently on query-engine workers; retaining the DAG here would make those workers concurrently
    /// tear down Pipeline successor/predecessor ownership. Only these immutable execution properties are needed after
    /// lowering.
    PhysicalOperator rootOperator;
    PipelineId pipelineId;
};

}
