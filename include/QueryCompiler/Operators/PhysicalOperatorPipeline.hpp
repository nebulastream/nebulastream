#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATORPIPELINE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATORPIPELINE_HPP_

#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Nodes/Node.hpp>
#include <vector>
#include <memory>
namespace NES {
namespace QueryCompilation {

class PhysicalOperatorPipeline : public std::enable_shared_from_this<PhysicalOperatorPipeline> {

  public:
    static PhysicalOperatorPipelinePtr create();
    void addSuccessor(PhysicalOperatorPipelinePtr pipeline);
    void addPredecessor(PhysicalOperatorPipelinePtr pipeline);
    void removePredecessor(PhysicalOperatorPipelinePtr pipeline);
    void removeSuccessor(PhysicalOperatorPipelinePtr pipeline);
    std::vector<PhysicalOperatorPipelinePtr> getPredecessors();
    std::vector<PhysicalOperatorPipelinePtr> getSuccessors();
    void prependOperator(OperatorNodePtr newRootOperator);
    bool hasOperators();
    void clearPredecessors();
    void clearSuccessors();
    QueryPlanPtr getRootOperator();
    uint64_t getPipelineId();
  protected:
    PhysicalOperatorPipeline(uint64_t pipelineId);
  private:
    uint64_t id;
    std::vector<std::shared_ptr<PhysicalOperatorPipeline>> successorPipelines;
    std::vector<std::weak_ptr<PhysicalOperatorPipeline>> predecessorPipelines;
    QueryPlanPtr rootOperator;

};
}// namespace QueryCompilation

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATORPIPELINE_HPP_
