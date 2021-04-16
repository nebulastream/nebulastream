#include <QueryCompiler/Operators/PhysicalOperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <Plans/Query/QueryPlan.hpp>
namespace NES {
namespace QueryCompilation {

PhysicalOperatorPipeline::PhysicalOperatorPipeline(uint64_t pipelineId): id(pipelineId), rootOperator(QueryPlan::create()) {}

PhysicalOperatorPipelinePtr PhysicalOperatorPipeline::create() {
    return std::make_shared<PhysicalOperatorPipeline>(PhysicalOperatorPipeline(UtilityFunctions::getNextPipelineId()));
}

void PhysicalOperatorPipeline::addPredecessor(PhysicalOperatorPipelinePtr pipeline) {
    pipeline->successorPipelines.emplace_back(shared_from_this());
    this->predecessorPipelines.emplace_back(pipeline);
}

void PhysicalOperatorPipeline::addSuccessor(PhysicalOperatorPipelinePtr pipeline) {
    if (pipeline) {
        pipeline->predecessorPipelines.emplace_back(weak_from_this());
        this->successorPipelines.emplace_back(pipeline);
    }
}

std::vector<PhysicalOperatorPipelinePtr> PhysicalOperatorPipeline::getPredecessors() {
    std::vector<PhysicalOperatorPipelinePtr> predecessors;
    for (auto predecessor : predecessorPipelines) {
        predecessors.emplace_back(predecessor.lock());
    }
    return predecessors;
}

bool PhysicalOperatorPipeline::hasOperators() {
    return this->rootOperator != nullptr;
}

void PhysicalOperatorPipeline::clearPredecessors() {
    for(auto pre: predecessorPipelines){
        if(auto prePipeline = pre.lock()){
            prePipeline->removeSuccessor(shared_from_this());
        }
    }
    predecessorPipelines.clear();
}

void PhysicalOperatorPipeline::removePredecessor(PhysicalOperatorPipelinePtr pipeline) {
    for (auto iter = predecessorPipelines.begin(); iter != predecessorPipelines.end(); ++iter) {
        if(iter->lock().get() == pipeline.get()){
            predecessorPipelines.erase(iter);
            return;
        }
    }
}
void PhysicalOperatorPipeline::removeSuccessor(PhysicalOperatorPipelinePtr pipeline) {
    for (auto iter = successorPipelines.begin(); iter != successorPipelines.end(); ++iter) {
        if(iter->get() == pipeline.get()){
            successorPipelines.erase(iter);
            return;
        }
    }
}
void PhysicalOperatorPipeline::clearSuccessors() {
    for(auto succ: successorPipelines){
        succ->removePredecessor(shared_from_this());
    }
    successorPipelines.clear();
}

std::vector<PhysicalOperatorPipelinePtr> PhysicalOperatorPipeline::getSuccessors() { return successorPipelines; }
void PhysicalOperatorPipeline::prependOperator(OperatorNodePtr newRootOperator) {
    this->rootOperator->appendOperatorAsNewRoot(newRootOperator);
}

uint64_t PhysicalOperatorPipeline::getPipelineId() {
    return id;
}
QueryPlanPtr PhysicalOperatorPipeline::getRootOperator() {
    return rootOperator;
}


}// namespace QueryCompilation
}// namespace NES