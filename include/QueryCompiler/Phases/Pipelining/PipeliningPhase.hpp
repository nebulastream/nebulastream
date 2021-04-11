#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELININGPHASE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELININGPHASE_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {
class PipeliningPhase {
  public:
    virtual PipelineQueryPlanPtr apply(QueryPlanPtr queryPlan) = 0;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELININGPHASE_HPP_
