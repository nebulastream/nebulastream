/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PIPELINEQUERYPLAN_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PIPELINEQUERYPLAN_HPP_

#include <Nodes/Node.hpp>
#include <Operators/OperatorId.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <memory>
#include <vector>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Representation of a query plan, which consists of a set of OperatorPipelines.
 */
class PipelineQueryPlan {
  public:
    /**
     * @brief Creates a new pipelined query plan
     * @param queryId
     * @param querySubPlanId
     * @return PipelineQueryPlanPtr
     */
    static PipelineQueryPlanPtr create(QueryId queryId = 0, QuerySubPlanId querySubPlanId = 0);

    /**
     * @brief Add a pipeline to the query plan
     * @param pipeline
     */
    void addPipeline(const OperatorPipelinePtr& pipeline);

    /**
     * @brief Gets a list of source pipelines, which only contain a single physical source operator
     * @return std::vector<OperatorPipelinePtr>
     */
    [[nodiscard]] std::vector<OperatorPipelinePtr> getSourcePipelines() const;

    /**
     * @brief Gets a list of sink pipelines, which only contain a single physical sink operator
     * @return std::vector<OperatorPipelinePtr>
     */
    [[nodiscard]] std::vector<OperatorPipelinePtr> getSinkPipelines() const;

    /**
     * @brief Gets a list of all pipelines.
     * @return std::vector<OperatorPipelinePtr>
     */
    [[nodiscard]] std::vector<OperatorPipelinePtr> const& getPipelines() const;

    /**
     * @brief Remove a particular pipeline from the query plan
     * @param pipeline
     */
    void removePipeline(const OperatorPipelinePtr& pipeline);

    /**
     * @brief Gets the query id
     * @return QueryId
     */
    [[nodiscard]] QueryId getQueryId() const;

    /**
     * @brief Gets the query sub plan id
     * @return QuerySubPlanId
     */
    [[nodiscard]] QuerySubPlanId getQuerySubPlanId() const;

  private:
    PipelineQueryPlan(QueryId queryId, QuerySubPlanId querySubPlanId);
    const QueryId queryId;
    const QuerySubPlanId querySubPlanId;
    std::vector<OperatorPipelinePtr> pipelines;
};
}// namespace QueryCompilation

}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_OPERATORS_PIPELINEQUERYPLAN_HPP_
