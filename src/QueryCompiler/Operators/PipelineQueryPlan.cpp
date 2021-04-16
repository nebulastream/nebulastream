#include <QueryCompiler/Operators/PhysicalOperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <algorithm>

namespace NES {
namespace QueryCompilation {

PipelineQueryPlanPtr PipelineQueryPlan::create() { return std::make_shared<PipelineQueryPlan>(); }

void PipelineQueryPlan::addPipeline(PhysicalOperatorPipelinePtr pipeline) { pipelines.emplace_back(pipeline); }

void PipelineQueryPlan::removePipeline(PhysicalOperatorPipelinePtr pipeline) {
    pipeline->clearPredecessors();
    pipeline->clearSuccessors();
    pipelines.erase(std::remove(pipelines.begin(), pipelines.end(), pipeline), pipelines.end());
}

std::vector<PhysicalOperatorPipelinePtr> PipelineQueryPlan::getSinkPipelines() {
    std::vector<PhysicalOperatorPipelinePtr> sinks;
    std::copy_if(pipelines.begin(), pipelines.end(), std::back_inserter(sinks), [](PhysicalOperatorPipelinePtr pipeline) {
        return pipeline->getSuccessors().empty();
    });
    return sinks;
}

std::vector<PhysicalOperatorPipelinePtr> PipelineQueryPlan::getSourcePipelines() {
    std::vector<PhysicalOperatorPipelinePtr> sinks;
    std::copy_if(pipelines.begin(), pipelines.end(), std::back_inserter(sinks), [](PhysicalOperatorPipelinePtr pipeline) {
      return pipeline->getPredecessors().empty();
    });
    return sinks;
}

std::vector<PhysicalOperatorPipelinePtr> PipelineQueryPlan::getPipelines() {
    return pipelines;
}

}// namespace QueryCompilation
}// namespace NES