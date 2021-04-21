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
