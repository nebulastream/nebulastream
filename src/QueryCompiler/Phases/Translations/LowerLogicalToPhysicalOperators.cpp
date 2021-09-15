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
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/PhysicalOperatorProvider.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <utility>

namespace NES::QueryCompilation {

LowerLogicalToPhysicalOperatorsPtr
LowerLogicalToPhysicalOperators::LowerLogicalToPhysicalOperators::create(const PhysicalOperatorProviderPtr& provider) {
    return std::make_shared<LowerLogicalToPhysicalOperators>(provider);
}

LowerLogicalToPhysicalOperators::LowerLogicalToPhysicalOperators(PhysicalOperatorProviderPtr provider)
    : provider(std::move(provider)) {}

QueryPlanPtr LowerLogicalToPhysicalOperators::apply(QueryPlanPtr queryPlan) {
    std::vector<NodePtr> nodes = QueryPlanIterator(queryPlan).snapshot();
    for (const auto& node : nodes) {
        if(node->instanceOf<PhysicalOperators::PhysicalOperator>()){
            NES_DEBUG("Skipped node: " << node->toString() << " as it is already a physical operator.");
            continue;
        }
        provider->lower(queryPlan, node->as<LogicalOperatorNode>());
    }
    return queryPlan;
}

}// namespace NES::QueryCompilation