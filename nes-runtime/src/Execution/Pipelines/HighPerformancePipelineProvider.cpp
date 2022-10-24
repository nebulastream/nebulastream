#include <Execution/Pipelines/HighPerformancePipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>

namespace NES::Runtime::Execution {

std::unique_ptr<ExecutablePipelineStage> HighPerformancePipelineProvider::create(std::shared_ptr<PhysicalOperatorPipeline>) {
    return nullptr;
}

}// namespace NES::Runtime::Execution