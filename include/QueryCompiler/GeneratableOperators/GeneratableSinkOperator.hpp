#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESINKOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESINKOPERATOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {

class GeneratableSinkOperator : public SinkLogicalOperatorNode, public GeneratableOperator {
  public:
    static GeneratableSinkOperatorPtr create(SinkLogicalOperatorNodePtr);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;

  private:
    explicit GeneratableSinkOperator(SinkDescriptorPtr sinkDescriptor);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESINKOPERATOR_HPP_
