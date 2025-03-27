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
#pragma once

#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Pipeline.hpp>

namespace NES
{
/// Representation of a query plan of pipelines
struct PipelinedQueryPlan final
{
    PipelinedQueryPlan(QueryId id) : queryId(id) {};
    [[nodiscard]] std::string toString() const
    {
        std::stringstream ss;
        ss << "PipelinedQueryPlan for Query: " << queryId << "\n";
        ss << "Number of root pipelines: " << pipelines.size() << "\n";
        for (size_t i = 0; i < pipelines.size(); ++i)
        {
            ss << "------------------\n";
            ss << "Root Pipeline " << i << ":\n";
            printPipelineRecursive(pipelines[i].get(), ss, 1);
        }
        return ss.str();
    }

    void printPipelineRecursive(const NES::Pipeline* pipe, std::stringstream& ss, int indentLevel) const
    {
        std::string indent(indentLevel * 2, ' ');
        ss << indent << pipe->toString() << "\n";
        for (const auto& succ : pipe->successorPipelines)
        {
            ss << indent << "Successor Pipeline:\n";
            printPipelineRecursive(succ.get(), ss, indentLevel + 1);
        }
    }

    friend std::ostream& operator<<(std::ostream& os, const PipelinedQueryPlan& t);

    std::vector<std::shared_ptr<Pipeline>> getSourcePipelines()
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

    const QueryId queryId;
    std::vector<std::shared_ptr<Pipeline>> pipelines;
};
}
FMT_OSTREAM(NES::PipelinedQueryPlan);
