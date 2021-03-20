/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
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