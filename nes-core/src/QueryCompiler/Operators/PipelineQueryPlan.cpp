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
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <algorithm>

namespace NES::QueryCompilation {

PipelineQueryPlanPtr PipelineQueryPlan::create(QueryId queryId, QuerySubPlanId querySubPlanId) {
    return std::make_shared<PipelineQueryPlan>(PipelineQueryPlan(queryId, querySubPlanId));
}

PipelineQueryPlan::PipelineQueryPlan(QueryId queryId, QuerySubPlanId querySubPlanId)
    : queryId(queryId), querySubPlanId(querySubPlanId){};

void PipelineQueryPlan::addPipeline(const OperatorPipelinePtr& pipeline) { pipelines.emplace_back(pipeline); }

void PipelineQueryPlan::removePipeline(const OperatorPipelinePtr& pipeline) {
    pipeline->clearPredecessors();
    pipeline->clearSuccessors();
    pipelines.erase(std::remove(pipelines.begin(), pipelines.end(), pipeline), pipelines.end());
}

std::vector<OperatorPipelinePtr> PipelineQueryPlan::getSinkPipelines() const {
    std::vector<OperatorPipelinePtr> sinks;
    std::copy_if(pipelines.begin(), pipelines.end(), std::back_inserter(sinks), [](const OperatorPipelinePtr& pipeline) {
        return pipeline->getSuccessors().empty();
    });
    return sinks;
}

std::vector<OperatorPipelinePtr> PipelineQueryPlan::getSourcePipelines() const {
    std::vector<OperatorPipelinePtr> sources;
    std::copy_if(pipelines.begin(), pipelines.end(), std::back_inserter(sources), [](const OperatorPipelinePtr& pipeline) {
        return pipeline->getPredecessors().empty();
    });
    return sources;
}

std::vector<OperatorPipelinePtr> const& PipelineQueryPlan::getPipelines() const { return pipelines; }
QueryId PipelineQueryPlan::getQueryId() const { return queryId; }
QuerySubPlanId PipelineQueryPlan::getQuerySubPlanId() const { return querySubPlanId; }

}// namespace NES::QueryCompilation