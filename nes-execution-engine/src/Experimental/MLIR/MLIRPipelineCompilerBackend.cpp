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

#include "Util/Timer.hpp"
#include <Experimental/MLIR/MLIRExecutablePipeline.hpp>
#include <Experimental/MLIR/MLIRPipelineCompilerBackend.hpp>
#include <Experimental/MLIR/MLIRUtility.hpp>

namespace NES::ExecutionEngine::Experimental {

std::shared_ptr<ExecutablePipeline>
MLIRPipelineCompilerBackend::compile(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                                     std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                     std::shared_ptr<IR::IRGraph> ir) {
    //std::cout << ir->toString() << std::endl;
    Timer timer("CompilationBasedPipelineExecutionEngine");
    timer.start();
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    MLIR::MLIRUtility::DebugFlags df = {false, false, false};
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
    auto engine = mlirUtility->prepareEngine();
    auto function = (void (*)(void*, void*)) engine->lookup("execute").get();
    timer.snapshot("MLIRGeneration");
    auto exec = std::make_shared<MLIRExecutablePipeline>(executionContext, physicalOperatorPipeline, std::move(engine));
    timer.snapshot("MLIR compile");
    timer.pause();
    NES_INFO("MLIRPipelineCompilerBackend TIME: " << timer);
    return exec;
}
}// namespace NES::ExecutionEngine::Experimental