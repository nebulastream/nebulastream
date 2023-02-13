#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/StatisticsCollector/PipelineRuntime.hpp>

namespace NES::Runtime::Execution {

void PipelineRuntime::collect() const {
    auto runtime = nautilusExecutablePipelineStage->getRuntimePerBuffer();
    std::cout << "PipelineRuntime " << runtime << " microseconds" << std::endl;
}

PipelineRuntime::PipelineRuntime(std::shared_ptr<NautilusExecutablePipelineStage> nautilusExecutablePipelineStage)
    : nautilusExecutablePipelineStage(nautilusExecutablePipelineStage) {}

} // namespace NES::Runtime::Execution
