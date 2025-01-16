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

#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <nautilus/options.hpp>

namespace NES::Runtime::Execution
{

std::unique_ptr<ExecutablePipelineStage> CompilationPipelineProvider::create(
    std::shared_ptr<PhysicalOperatorPipeline> pipeline,
    std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers,
    nautilus::engine::Options& options)
{
    /// As we are creating here a pipeline that is compiled, we need to set the compilation option to true
    options.setOption("engine.Compilation", true);
    return std::make_unique<CompiledExecutablePipelineStage>(pipeline, std::move(operatorHandlers), options);
}

std::unique_ptr<ExecutablePipelineProvider> RegisterCompilationPipelineProvider()
{
    return std::make_unique<CompilationPipelineProvider>();
}

}
