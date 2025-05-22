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

#include <cstddef>
#include <memory>
#include <ostream>
#include <ranges>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/ExecutionMode.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES
{

PipelinedQueryPlan::PipelinedQueryPlan(QueryId id, Nautilus::Configurations::ExecutionMode executionMode)
    : queryId(id), executionMode(executionMode) { };

static void printPipelineRecursive(const Pipeline* pipeline, std::ostream& os, int indentLevel)
{
    const std::string indent(indentLevel * 2, ' ');
    os << indent << *pipeline << "\n";
    for (const auto& succ : pipeline->getSuccessors())
    {
        os << indent << "Successor Pipeline:\n";
        printPipelineRecursive(succ.get(), os, indentLevel + 1);
    }
}

std::ostream& operator<<(std::ostream& os, const PipelinedQueryPlan& plan)
{
    os << "PipelinedQueryPlan for Query: " << plan.getQueryId() << "\n";
    os << "Number of root pipelines: " << plan.getPipelines().size() << "\n";
    for (size_t i = 0; i < plan.getPipelines().size(); ++i)
    {
        os << "------------------\n";
        os << "Root Pipeline " << i << ":\n";
        printPipelineRecursive(plan.getPipelines()[i].get(), os, 1);
    }
    return os;
}

void PipelinedQueryPlan::removePipeline(Pipeline& pipeline)
{
    pipeline.clearSuccessors();
    pipeline.clearPredecessors();
    std::erase_if(pipelines, [&pipeline](const auto& ptr) { return ptr->getPipelineId() == pipeline.getPipelineId(); });
}

std::vector<std::shared_ptr<Pipeline>> PipelinedQueryPlan::getSourcePipelines() const
{
    return std::views::filter(pipelines, [](const auto& pipelinePtr) { return pipelinePtr->isSourcePipeline(); })
        | std::ranges::to<std::vector>();
}

QueryId PipelinedQueryPlan::getQueryId() const
{
    return queryId;
}

Nautilus::Configurations::ExecutionMode PipelinedQueryPlan::getExecutionMode() const
{
    return executionMode;
}

const std::vector<std::shared_ptr<Pipeline>>& PipelinedQueryPlan::getPipelines() const
{
    return pipelines;
}

void PipelinedQueryPlan::addPipeline(const std::shared_ptr<Pipeline>& pipeline)
{
    pipeline->setExecutionMode(executionMode);
    pipelines.push_back(pipeline);
}

}
