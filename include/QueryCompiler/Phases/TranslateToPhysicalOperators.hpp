#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATETOPHYSICALOPERATORS_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATETOPHYSICALOPERATORS_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <vector>
namespace NES{
namespace QueryCompilation {

class TranslateToPhysicalOperators{

  public:
    TranslateToPhysicalOperators(PhysicalOperatorProviderPtr provider);
    static TranslateToPhysicalOperatorsPtr create(PhysicalOperatorProviderPtr provider);
    PhysicalQueryPlanPtr apply(QueryPlanPtr queryPlan);

  private:
    PhysicalOperatorProviderPtr provider;

};

}
}

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATETOPHYSICALOPERATORS_HPP_
