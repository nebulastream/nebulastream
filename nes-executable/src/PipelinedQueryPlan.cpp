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

#include <PipelinedQueryPlan.hpp>

namespace NES
{

PipelinedQueryPlan::PipelinedQueryPlan(QueryId id) : queryId(id){
}

std::string PipelinedQueryPlan::toString() const {
    std::ostringstream oss;
    oss << "PipelinedQueryPlan(queryId: " << queryId << ", pipelines: [";
    for (size_t i = 0; i < pipelines.size(); ++i) {
        if (pipelines[i]) {
            oss << pipelines[i]->toString();
        } else {
            oss << "null";
        }
        if (i < pipelines.size() - 1) {
            oss << ", ";
        }
    }
    oss << "])";
    return oss.str();
}

std::ostream& operator<<(std::ostream& os, const PipelinedQueryPlan& t)
{
    return os << t.toString();
}

std::vector<std::unique_ptr<Pipeline>> PipelinedQueryPlan::getSourcePipelines() const
{
    std::vector<std::unique_ptr<Pipeline>> sourcePipelines;
    for (const auto &pipeline : pipelines)
    {
        if (dynamic_cast<SourcePipeline*>(pipeline.get()))
        {
            sourcePipelines.emplace_back(pipeline);
        }
    }
    return sourcePipelines;
}

std::vector<std::unique_ptr<Pipeline>> PipelinedQueryPlan::getSinkPipelines() const
{
    std::vector<std::unique_ptr<Pipeline>> sinkPipelines;
    for (const auto &pipeline : pipelines)
    {
        if (dynamic_cast<SinkPipeline*>(pipeline.get()))
        {
            sinkPipelines.emplace_back(pipeline);
        }
    }
    return sinkPipelines;
}

}
