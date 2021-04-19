#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
namespace NES {
namespace QueryCompilation {

OperatorPipeline::OperatorPipeline(uint64_t pipelineId, Type pipelineType)
    : id(pipelineId), rootOperator(QueryPlan::create()), pipelineType(pipelineType)
{}

OperatorPipelinePtr OperatorPipeline::create() {
    return std::make_shared<OperatorPipeline>(OperatorPipeline(UtilityFunctions::getNextPipelineId(), OperatorPipelineType));
}

OperatorPipelinePtr OperatorPipeline::createSinkPipeline() {
    return std::make_shared<OperatorPipeline>(OperatorPipeline(UtilityFunctions::getNextPipelineId(), SinkPipelineType));
}

OperatorPipelinePtr OperatorPipeline::createSourcePipeline() {
    return std::make_shared<OperatorPipeline>(OperatorPipeline(UtilityFunctions::getNextPipelineId(), SourcePipelineType));
}

bool OperatorPipeline::setType(Type pipelineType) {
   this->pipelineType = pipelineType;
}

bool OperatorPipeline::isOperatorPipeline() {
    return pipelineType == OperatorPipelineType;
}

bool OperatorPipeline::isSinkPipeline(){
    return pipelineType == SinkPipelineType;
}

bool OperatorPipeline::isSourcePipeline() {
    return pipelineType == SourcePipelineType;
}

void OperatorPipeline::addPredecessor(OperatorPipelinePtr pipeline) {
    pipeline->successorPipelines.emplace_back(shared_from_this());
    this->predecessorPipelines.emplace_back(pipeline);
}

void OperatorPipeline::addSuccessor(OperatorPipelinePtr pipeline) {
    if (pipeline) {
        pipeline->predecessorPipelines.emplace_back(weak_from_this());
        this->successorPipelines.emplace_back(pipeline);
    }
}

std::vector<OperatorPipelinePtr> OperatorPipeline::getPredecessors() {
    std::vector<OperatorPipelinePtr> predecessors;
    for (auto predecessor : predecessorPipelines) {
        predecessors.emplace_back(predecessor.lock());
    }
    return predecessors;
}

bool OperatorPipeline::hasOperators() { return this->rootOperator != nullptr; }

void OperatorPipeline::clearPredecessors() {
    for (auto pre : predecessorPipelines) {
        if (auto prePipeline = pre.lock()) {
            prePipeline->removeSuccessor(shared_from_this());
        }
    }
    predecessorPipelines.clear();
}

void OperatorPipeline::removePredecessor(OperatorPipelinePtr pipeline) {
    for (auto iter = predecessorPipelines.begin(); iter != predecessorPipelines.end(); ++iter) {
        if (iter->lock().get() == pipeline.get()) {
            predecessorPipelines.erase(iter);
            return;
        }
    }
}
void OperatorPipeline::removeSuccessor(OperatorPipelinePtr pipeline) {
    for (auto iter = successorPipelines.begin(); iter != successorPipelines.end(); ++iter) {
        if (iter->get() == pipeline.get()) {
            successorPipelines.erase(iter);
            return;
        }
    }
}
void OperatorPipeline::clearSuccessors() {
    for (auto succ : successorPipelines) {
        succ->removePredecessor(shared_from_this());
    }
    successorPipelines.clear();
}

std::vector<OperatorPipelinePtr> OperatorPipeline::getSuccessors() { return successorPipelines; }
void OperatorPipeline::prependOperator(OperatorNodePtr newRootOperator) {
    this->rootOperator->appendOperatorAsNewRoot(newRootOperator);
}

uint64_t OperatorPipeline::getPipelineId() { return id; }
QueryPlanPtr OperatorPipeline::getQueryPlan() { return rootOperator; }

}// namespace QueryCompilation
}// namespace NES