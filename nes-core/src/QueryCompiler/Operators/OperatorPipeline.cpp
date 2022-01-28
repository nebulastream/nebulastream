/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <utility>
namespace NES::QueryCompilation {

OperatorPipeline::OperatorPipeline(uint64_t pipelineId, Type pipelineType)
    : id(pipelineId), queryPlan(QueryPlan::create()), pipelineType(pipelineType) {}

OperatorPipelinePtr OperatorPipeline::create() {
    return std::make_shared<OperatorPipeline>(OperatorPipeline(Util::getNextPipelineId(), OperatorPipelineType));
}

OperatorPipelinePtr OperatorPipeline::createSinkPipeline() {
    return std::make_shared<OperatorPipeline>(OperatorPipeline(Util::getNextPipelineId(), SinkPipelineType));
}

OperatorPipelinePtr OperatorPipeline::createSourcePipeline() {
    return std::make_shared<OperatorPipeline>(OperatorPipeline(Util::getNextPipelineId(), SourcePipelineType));
}

void OperatorPipeline::setType(Type pipelineType) { this->pipelineType = pipelineType; }

bool OperatorPipeline::isOperatorPipeline() const { return pipelineType == OperatorPipelineType; }

bool OperatorPipeline::isSinkPipeline() const { return pipelineType == SinkPipelineType; }

bool OperatorPipeline::isSourcePipeline() const { return pipelineType == SourcePipelineType; }

void OperatorPipeline::addPredecessor(const OperatorPipelinePtr& pipeline) {
    pipeline->successorPipelines.emplace_back(shared_from_this());
    this->predecessorPipelines.emplace_back(pipeline);
}

void OperatorPipeline::addSuccessor(const OperatorPipelinePtr& pipeline) {
    if (pipeline) {
        pipeline->predecessorPipelines.emplace_back(weak_from_this());
        this->successorPipelines.emplace_back(pipeline);
    }
}

std::vector<OperatorPipelinePtr> OperatorPipeline::getPredecessors() const {
    std::vector<OperatorPipelinePtr> predecessors;
    for (const auto& predecessor : predecessorPipelines) {
        predecessors.emplace_back(predecessor.lock());
    }
    return predecessors;
}

bool OperatorPipeline::hasOperators() const { return !this->queryPlan->getRootOperators().empty(); }

void OperatorPipeline::clearPredecessors() {
    for (const auto& pre : predecessorPipelines) {
        if (auto prePipeline = pre.lock()) {
            prePipeline->removeSuccessor(shared_from_this());
        }
    }
    predecessorPipelines.clear();
}

void OperatorPipeline::removePredecessor(const OperatorPipelinePtr& pipeline) {
    for (auto iter = predecessorPipelines.begin(); iter != predecessorPipelines.end(); ++iter) {
        if (iter->lock().get() == pipeline.get()) {
            predecessorPipelines.erase(iter);
            return;
        }
    }
}
void OperatorPipeline::removeSuccessor(const OperatorPipelinePtr& pipeline) {
    for (auto iter = successorPipelines.begin(); iter != successorPipelines.end(); ++iter) {
        if (iter->get() == pipeline.get()) {
            successorPipelines.erase(iter);
            return;
        }
    }
}
void OperatorPipeline::clearSuccessors() {
    for (const auto& succ : successorPipelines) {
        succ->removePredecessor(shared_from_this());
    }
    successorPipelines.clear();
}

std::vector<OperatorPipelinePtr> const& OperatorPipeline::getSuccessors() const { return successorPipelines; }

void OperatorPipeline::prependOperator(OperatorNodePtr newRootOperator) {
    if (!this->isOperatorPipeline() && this->hasOperators()) {
        throw QueryCompilationException("Sink and Source pipelines can have more then one operator");
    }
    this->queryPlan->appendOperatorAsNewRoot(std::move(newRootOperator));
}

uint64_t OperatorPipeline::getPipelineId() const { return id; }
QueryPlanPtr OperatorPipeline::getQueryPlan() { return queryPlan; }

}// namespace NES::QueryCompilation