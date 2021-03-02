#include <NodeEngine/Execution/DiscardingExecutablePipelineStage.hpp>

uint32_t NES::NodeEngine::Execution::DiscardingExecutablePipelineStage::execute(
    NES::NodeEngine::TupleBuffer&,
    NES::NodeEngine::Execution::PipelineExecutionContext&,
    NES::NodeEngine::WorkerContext&) {
    return 0;
}
