#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATETOPHYSICALOPERATORS_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATETOPHYSICALOPERATORS_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <vector>
namespace NES{
namespace QueryCompilation {

class TranslateToPhysicalOperators{

  public:
    TranslateToPhysicalOperatorsPtr create();
    PhysicalQueryPlanPtr apply(QueryPlanPtr queryPlan);

  private:
    void processOperator(std::vector<OperatorNodePtr> operatorNode);

};

}
}

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATETOPHYSICALOPERATORS_HPP_
