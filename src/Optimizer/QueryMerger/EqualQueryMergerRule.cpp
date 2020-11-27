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

#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/EqualQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Signature/QueryPlanSignature.hpp>
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

bool EqualQueryMergerRule::apply(QueryPlanPtr queryPlan1, QueryPlanPtr queryPlan2) {
    auto roots1 = queryPlan1->getRootOperators();
    auto roots2 = queryPlan2->getRootOperators();

    z3::expr_vector expressions1(*context);

    for (auto& root : roots1) {
        auto querySig = root->as<LogicalOperatorNode>()->getSignature();
        expressions1.push_back(*querySig->getConds());
    }
    z3::expr mkAnd1 = z3::mk_and(expressions1);

    z3::expr_vector expressions2(*context);
    for (auto& root : roots2) {
        auto querySig = root->as<LogicalOperatorNode>()->getSignature();
        expressions2.push_back(*querySig->getConds());
    }

    z3::expr mkAnd2 = z3::mk_and(expressions2);
    NES_INFO(mkAnd1);
    NES_INFO(mkAnd2);

    auto expr = z3::to_expr(*context, Z3_mk_eq(*context, mkAnd1, mkAnd2));

    z3::solver solver(*context);
    solver.add(expr && !expr);
    if (solver.check() == z3::sat) {
        NES_INFO("Model " << solver.get_model());
        return false;
    } else {
        return true;
    }
}

}// namespace NES::Optimizer
