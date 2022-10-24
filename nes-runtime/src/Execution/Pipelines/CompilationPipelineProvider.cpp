
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>

namespace NES::Runtime::Execution {

std::unique_ptr<ExecutablePipelineStage> CompilationPipelineProvider::create(std::shared_ptr<PhysicalOperatorPipeline> pipeline) {
    return std::make_unique<CompiledExecutablePipelineStage>(pipeline);
}

[[maybe_unused]] static ExecutablePipelineProviderRegistry::Add<CompilationPipelineProvider>
    compilationPipelineProvider("PipelineCompiler");

}// namespace NES::Runtime::Execution