#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PHYSICALOPERATORPIPELINE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PHYSICALOPERATORPIPELINE_HPP_
#include <Experimental/Interpreter/Operators/Operator.hpp>
namespace NES::ExecutionEngine::Experimental {

class PhysicalOperatorPipeline {
  public:
    Interpreter::Operator* getRootOperator() const { return rootOperator; }
    void setRootOperator(Interpreter::Operator* rootOperator) { PhysicalOperatorPipeline::rootOperator = rootOperator; }

  private:
      Interpreter::Operator* rootOperator;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PHYSICALOPERATORPIPELINE_HPP_
