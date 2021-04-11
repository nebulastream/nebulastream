#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_FUSEIFPOSSIBLEPOLICY_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_FUSEIFPOSSIBLEPOLICY_HPP_
#include <QueryCompiler/Phases/Pipelining/PipelineBreakerPolicy.hpp>
namespace NES {
namespace QueryCompilation {
class FuseIfPossiblePolicy : public PipelineBreakerPolicy {
  public:
    static PipelineBreakerPolicyPtr create();
    bool isFusible(PhysicalOperators::PhysicalOperatorPtr physicalOperator) override;
};
}}

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_FUSEIFPOSSIBLEPOLICY_HPP_
