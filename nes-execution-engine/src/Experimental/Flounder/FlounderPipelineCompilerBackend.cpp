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
    //std::cout << ir->toString() << std::endl;
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