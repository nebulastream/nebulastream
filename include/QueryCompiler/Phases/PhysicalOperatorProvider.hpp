#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PHYSICALOPERATORPROVIDER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PHYSICALOPERATORPROVIDER_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {
class PhysicalOperatorProvider {
  public:
    virtual void lower(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) = 0;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_PHYSICALOPERATORPROVIDER_HPP_
