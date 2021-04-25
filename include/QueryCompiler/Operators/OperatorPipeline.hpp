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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATORPIPELINE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATORPIPELINE_HPP_

#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Nodes/Node.hpp>
#include <vector>
#include <memory>
namespace NES {
namespace QueryCompilation {

class OperatorPipeline : public std::enable_shared_from_this<OperatorPipeline> {

  public:
    enum Type{
        SourcePipelineType,
        SinkPipelineType,
        OperatorPipelineType
    };
    static OperatorPipelinePtr create();
    static OperatorPipelinePtr createSourcePipeline();
    static OperatorPipelinePtr createSinkPipeline();
    void addSuccessor(OperatorPipelinePtr pipeline);
    void addPredecessor(OperatorPipelinePtr pipeline);
    void removePredecessor(OperatorPipelinePtr pipeline);
    void removeSuccessor(OperatorPipelinePtr pipeline);
    std::vector<OperatorPipelinePtr> getPredecessors();
    std::vector<OperatorPipelinePtr> getSuccessors();
    void prependOperator(OperatorNodePtr newRootOperator);
    bool hasOperators();
    void clearPredecessors();
    void clearSuccessors();
    QueryPlanPtr getQueryPlan();
    uint64_t getPipelineId();
    bool isSourcePipeline();
    bool isSinkPipeline();
    bool isOperatorPipeline();
    void setType(Type pipelineType);
  protected:
    OperatorPipeline(uint64_t pipelineId, Type pipelineType);
  private:
    uint64_t id;
    std::vector<std::shared_ptr<OperatorPipeline>> successorPipelines;
    std::vector<std::weak_ptr<OperatorPipeline>> predecessorPipelines;
    QueryPlanPtr queryPlan;
    Type pipelineType;

};
}// namespace QueryCompilation

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATORPIPELINE_HPP_
