
#include <QueryCompiler/Phases/TranslateToPhysicalOperators.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES{
namespace QueryCompilation{

TranslateToPhysicalOperatorsPtr TranslateToPhysicalOperators::TranslateToPhysicalOperators::create() {}

QueryCompilation::PhysicalQueryPlanPtr TranslateToPhysicalOperators::apply(QueryPlanPtr queryPlan) {

    auto operators = queryPlan->getRootOperators();


}

void TranslateToPhysicalOperators::
    processOperator(std::vector<OperatorNodePtr> operators) {
    for(auto currentOperator: operators){



    }


}




}
}