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
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Operators/OperatorId.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <memory>
#include <vector>
namespace NES {
namespace QueryCompilation {

class PipelineQueryPlan {
  public:
    static PipelineQueryPlanPtr create();
    void addPipeline(OperatorPipelinePtr pipeline);
    std::vector<OperatorPipelinePtr> getSourcePipelines();
    std::vector<OperatorPipelinePtr> getSinkPipelines();
    std::vector<OperatorPipelinePtr> getPipelines();
    void removePipeline(OperatorPipelinePtr pipeline);

    QueryId getQueryId() const;
    QuerySubPlanId getQuerySubPlanId() const;

  private:
    std::vector<OperatorPipelinePtr> pipelines;
    QueryId queryId;
    QuerySubPlanId querySubPlanId;
};
}// namespace QueryCompilation

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PIPELINEQUERYPLAN_HPP_
