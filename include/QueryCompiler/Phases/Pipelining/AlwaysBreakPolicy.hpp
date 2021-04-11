#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_OPERATORATATIMEPOLICY_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_OPERATORATATIMEPOLICY_HPP_
#include <QueryCompiler/Phases/Pipelining/PipelineBreakerPolicy.hpp>
namespace NES {
namespace QueryCompilation {
class AlwaysBreakPolicy : public PipelineBreakerPolicy {
  public:
    static PipelineBreakerPolicyPtr create();
    bool isFusible(PhysicalOperators::PhysicalOperatorPtr physicalOperator) override;
};
}}

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_OPERATORATATIMEPOLICY_HPP_
