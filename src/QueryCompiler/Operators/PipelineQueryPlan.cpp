#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <algorithm>

namespace NES {
namespace QueryCompilation {

PipelineQueryPlanPtr PipelineQueryPlan::create() { return std::make_shared<PipelineQueryPlan>(); }

void PipelineQueryPlan::addPipeline(OperatorPipelinePtr pipeline) { pipelines.emplace_back(pipeline); }

void PipelineQueryPlan::removePipeline(OperatorPipelinePtr pipeline) {
    pipeline->clearPredecessors();
    pipeline->clearSuccessors();
    pipelines.erase(std::remove(pipelines.begin(), pipelines.end(), pipeline), pipelines.end());
}

std::vector<OperatorPipelinePtr> PipelineQueryPlan::getSinkPipelines() {
    std::vector<OperatorPipelinePtr> sinks;
    std::copy_if(pipelines.begin(), pipelines.end(), std::back_inserter(sinks), [](OperatorPipelinePtr pipeline) {
        return pipeline->getSuccessors().empty();
    });
    return sinks;
}

std::vector<OperatorPipelinePtr> PipelineQueryPlan::getSourcePipelines() {
    std::vector<OperatorPipelinePtr> sinks;
    std::copy_if(pipelines.begin(), pipelines.end(), std::back_inserter(sinks), [](OperatorPipelinePtr pipeline) {
      return pipeline->getPredecessors().empty();
    });
    return sinks;
}

std::vector<OperatorPipelinePtr> PipelineQueryPlan::getPipelines() {
    return pipelines;
}

}// namespace QueryCompilation
}// namespace NES