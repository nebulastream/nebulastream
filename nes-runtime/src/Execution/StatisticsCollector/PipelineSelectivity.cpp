#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/StatisticsCollector/PipelineSelectivity.hpp>

namespace NES::Runtime::Execution {

void PipelineSelectivity::collect() const {
    auto numberOfInputTuples = nautilusExecutablePipelineStage->getNumberOfInputTuples();
    auto numberOfOutputTuples = nautilusExecutablePipelineStage->getNumberOfOutputTuples();

    if(numberOfInputTuples != 0){
        auto selectivity = (double) numberOfOutputTuples / numberOfInputTuples;
        std::cout << "PipelineSelectivity " << selectivity << std::endl;
    }

}

PipelineSelectivity::PipelineSelectivity(std::shared_ptr<NautilusExecutablePipelineStage> nautilusExecutablePipelineStage)
    : nautilusExecutablePipelineStage(nautilusExecutablePipelineStage) {}

} // namespace NES::Runtime::Execution