#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINEBREAKERPOLICY_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINEBREAKERPOLICY_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {
class PipelineBreakerPolicy {
  public:
    virtual bool isFusible(PhysicalOperators::PhysicalOperatorPtr physicalOperator) = 0;
};
}}
#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINEBREAKERPOLICY_HPP_
