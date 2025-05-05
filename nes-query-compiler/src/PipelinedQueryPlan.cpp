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

#include <Phases/LowerToCompiledQueryPlanPhase.hpp>
#include <PipelinedQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>

namespace NES
{

PipelinedQueryPlan::PipelinedQueryPlan(QueryId id) : queryId(id) {};

void printPipelineRecursive(const Pipeline* pipeline, std::ostream& os, int indentLevel)
{
    std::string indent(indentLevel * 2, ' ');
    os << indent << *pipeline << "\n";
    for (const auto& succ : pipeline->getSuccessors())
    {
        os << indent << "Successor Pipeline:\n";
        printPipelineRecursive(succ.get(), os, indentLevel + 1);
    }
}

std::ostream& operator<<(std::ostream& os, const PipelinedQueryPlan& plan)
{
    os << "PipelinedQueryPlan for Query: " << plan.queryId << "\n";
    os << "Number of root pipelines: " << plan.pipelines.size() << "\n";
    for (size_t i = 0; i < plan.pipelines.size(); ++i)
    {
        os << "------------------\n";
        os << "Root Pipeline " << i << ":\n";
        printPipelineRecursive(plan.pipelines[i].get(), os, 1);
    }
    return os;
}

void PipelinedQueryPlan::removePipeline(Pipeline& pipeline)
{
    pipeline.clearSuccessors();
    pipeline.clearPredecessors();
    pipelines.erase(
        std::remove_if(pipelines.begin(), pipelines.end(),
            [&pipeline](const auto& ptr) { return ptr->pipelineId == pipeline.pipelineId; }),
        pipelines.end());
}

std::vector<std::shared_ptr<Pipeline>> PipelinedQueryPlan::getSourcePipelines()
{
    std::vector<std::shared_ptr<Pipeline>> sourcePipelines;
    auto it = pipelines.begin();
    while (it != pipelines.end())
    {
        if (it->get()->isSourcePipeline())
        {
            sourcePipelines.push_back(*it);
            it = pipelines.erase(it);
        }
        else
        {
            ++it;
        }
    }
    return sourcePipelines;
}

}
