#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PIPELINEQUERYPLAN_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PIPELINEQUERYPLAN_HPP_

#include <Nodes/Node.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <memory>
#include <vector>
namespace NES {
namespace QueryCompilation {

class PipelineQueryPlan {

  public:
    static PipelineQueryPlanPtr create();
    void addPipeline(PhysicalOperatorPipelinePtr pipeline);
    std::vector<PhysicalOperatorPipelinePtr> getSourcePipelines();
    std::vector<PhysicalOperatorPipelinePtr> getSinkPipelines();
    void removePipeline(PhysicalOperatorPipelinePtr pipeline);

  private:
    std::vector<PhysicalOperatorPipelinePtr> pipelines;
};
}// namespace QueryCompilation

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PIPELINEQUERYPLAN_HPP_
