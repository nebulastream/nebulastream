
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <QueryCompiler/Phases/TranslateToPhysicalOperators.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <QueryCompiler/Phases/PhysicalOperatorProvider.hpp>

namespace NES {
namespace QueryCompilation {

TranslateToPhysicalOperatorsPtr TranslateToPhysicalOperators::TranslateToPhysicalOperators::create(PhysicalOperatorProviderPtr provider) {
    return std::make_shared<TranslateToPhysicalOperators>(provider);
}

TranslateToPhysicalOperators::TranslateToPhysicalOperators(PhysicalOperatorProviderPtr provider): provider(provider) {

}

QueryCompilation::PhysicalQueryPlanPtr TranslateToPhysicalOperators::apply(QueryPlanPtr queryPlan) {

    auto iterator = QueryPlanIterator(queryPlan);
    std::vector<NodePtr> nodes;
    for (auto node : iterator) {
        nodes.emplace_back(node);
    }
    for (auto node : nodes) {
        provider->lower(queryPlan, node->as<LogicalOperatorNode>());
    }
}


}// namespace QueryCompilation
}// namespace NES