#include <Execution/StatisticsCollector/PipelineRuntime.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>

namespace NES::Runtime::Execution {

void PipelineRuntime::collect() {
    auto runtime = nautilusExecutablePipelineStage->getRuntimePerBuffer();
    std::cout << "PipelineRuntime " << runtime << " microseconds" << std::endl;
}

std::string PipelineRuntime::getType() const {
    return "PipelineRuntime";
}

PipelineRuntime::PipelineRuntime(std::shared_ptr<NautilusExecutablePipelineStage> nautilusExecutablePipelineStage, uint64_t pipelineId)
    : nautilusExecutablePipelineStage(nautilusExecutablePipelineStage), pipelineId(pipelineId) {}

} // namespace NES::Runtime::Execution
