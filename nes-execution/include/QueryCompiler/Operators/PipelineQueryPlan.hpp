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

#include <Nodes/Node.hpp>

#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>

namespace NES::QueryCompilation
{
/**
 * @brief Representation of a query plan, which consists of a set of OperatorPipelines.
 */
class PipelineQueryPlan
{
public:
    static std::shared_ptr<PipelineQueryPlan> create(QueryId queryId = INVALID_QUERY_ID);

    void addPipeline(const OperatorPipelinePtr& pipeline);
    void removePipeline(const OperatorPipelinePtr& pipeline);

    [[nodiscard]] std::vector<OperatorPipelinePtr> getSourcePipelines() const;
    [[nodiscard]] std::vector<OperatorPipelinePtr> getSinkPipelines() const;
    [[nodiscard]] std::vector<OperatorPipelinePtr> const& getPipelines() const;

    [[nodiscard]] QueryId getQueryId() const;
    [[nodiscard]] std::string toString() const;

private:
    PipelineQueryPlan(QueryId queryId);
    const QueryId queryId;
    std::vector<OperatorPipelinePtr> pipelines;
};
using PipelineQueryPlanPtr = std::shared_ptr<PipelineQueryPlan>;
}
