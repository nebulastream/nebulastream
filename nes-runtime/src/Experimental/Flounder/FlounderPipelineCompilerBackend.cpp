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
#include "Experimental/Flounder/FlounderLoweringProvider.hpp"
#include "Util/Timer.hpp"
#include <Experimental/Flounder/FlounderExecutablePipeline.hpp>
#include <Experimental/Flounder/FlounderPipelineCompilerBackend.hpp>

namespace NES::ExecutionEngine::Experimental {

std::shared_ptr<ExecutablePipeline>
FlounderPipelineCompilerBackend::compile(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                                         std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                         std::shared_ptr<IR::NESIR> ir) {
    Timer timer("CompilationBasedPipelineExecutionEngine");
    timer.start();
    //NES_DEBUG(ir->toString());
    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    timer.snapshot("Flounder generation");

    auto exec = std::make_shared<FlounderExecutablePipeline>(executionContext, physicalOperatorPipeline, std::move(ex));
    timer.snapshot("Flounder compile");
    timer.pause();
    NES_INFO("FlounderPipelineCompilerBackend TIME: " << timer);
    return exec;
}
}// namespace NES::ExecutionEngine::Experimental