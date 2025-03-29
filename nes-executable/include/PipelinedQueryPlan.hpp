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

#include <vector>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Pipeline.hpp>

namespace NES
{
/// Representation of a query plan of pipelines
struct PipelinedQueryPlan : std::enable_shared_from_this<PipelinedQueryPlan>
{
    PipelinedQueryPlan(QueryId id) : queryId(id){
    }

    [[nodiscard]] std::string toString() const;
    [[nodiscard]] std::vector<std::shared_ptr<Pipeline>> getSourcePipelines() const
    {
        std::vector<std::shared_ptr<Pipeline>> sourcePipelines;
        for (const auto &pipeline : pipelines)
        {
            if (Util::instanceOf<SourcePipeline>(pipeline))
            {
                sourcePipelines.emplace_back(pipeline);
            }
        }
        return sourcePipelines;
    }
    [[nodiscard]] std::vector<std::shared_ptr<Pipeline>> getSinkPipelines() const
    {
        std::vector<std::shared_ptr<Pipeline>> sinkPipelines;
        for (const auto &pipeline : pipelines)
        {
            if (Util::instanceOf<SinkPipeline>(pipeline))
            {
                sinkPipelines.emplace_back(pipeline);
            }
        }
        return sinkPipelines;
    }

    const QueryId queryId;
    std::vector<std::shared_ptr<Pipeline>> pipelines;
};
}
