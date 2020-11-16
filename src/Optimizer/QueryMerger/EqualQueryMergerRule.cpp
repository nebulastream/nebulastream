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
#include <Optimizer/QueryMerger/EqualQueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

EqualQueryMergerRule::EqualQueryMergerRule(z3::ContextPtr context) : context(context) {}

EqualQueryMergerRule::~EqualQueryMergerRule() { NES_DEBUG("~EqualQueryMergerRule()"); }

EqualQueryMergerRulePtr EqualQueryMergerRule::create(z3::ContextPtr context) {
    return std::make_shared<EqualQueryMergerRule>(EqualQueryMergerRule(context));
}

void EqualQueryMergerRule::apply(QueryPlanPtr queryPlan1, QueryPlanPtr queryPlan2) {
    auto roots1 = queryPlan1->getRootOperators();
    auto roots2 = queryPlan2->getRootOperators();

    z3::expr_vector expressions(*context);
    for (auto& root : roots1) {
        auto family = root->getAndFlattenAllChildren();
        for(auto& member: family){
            member->as<LogicalOperatorNode>()->getZ3Expression();
        }
    }
}

}// namespace NES::Optimizer
