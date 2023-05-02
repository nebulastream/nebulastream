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
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>

namespace NES::Runtime::Execution {

class CPPCompilerPipelineProvider : public ExecutablePipelineProvider {
  public:
    std::unique_ptr<ExecutablePipelineStage> create(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                                    const Nautilus::CompilationOptions& options) override {
        return std::make_unique<CompiledExecutablePipelineStage>(physicalOperatorPipeline, "CPPCompiler", options);
    }
};

[[maybe_unused]] static ExecutablePipelineProviderRegistry::Add<CPPCompilerPipelineProvider>
    cppPipelineProvider("CPPPipelineCompiler");
}// namespace NES::Runtime::Execution