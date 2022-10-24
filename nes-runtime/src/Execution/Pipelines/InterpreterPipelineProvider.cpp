#include <Execution/Pipelines/InterpreterPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>

namespace NES::Runtime::Execution {

std::unique_ptr<ExecutablePipelineStage>
InterpreterPipelineProvider::create(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline) {
    return std::make_unique<NautilusExecutablePipelineStage>(physicalOperatorPipeline);
}

}// namespace NES::Runtime::Execution