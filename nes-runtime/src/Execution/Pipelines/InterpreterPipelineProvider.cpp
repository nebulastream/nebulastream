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

#include <Execution/Pipelines/InterpreterPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>

namespace NES::Runtime::Execution {

std::unique_ptr<ExecutablePipelineStage>
InterpreterPipelineProvider::create(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                    const Nautilus::CompilationOptions&) {
    return std::make_unique<NautilusExecutablePipelineStage>(physicalOperatorPipeline);
}

[[maybe_unused]] static ExecutablePipelineProviderRegistry::Add<InterpreterPipelineProvider>
    interpretationPipelineProvider("PipelineInterpreter");

}// namespace NES::Runtime::Execution