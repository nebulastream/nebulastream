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

#include <Optimizer/QueryMerger/StringSignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Phases/QueryMergerPhase.hpp>
#include <Util/Logger.hpp>

namespace NES::Optimizer {

QueryMergerPhasePtr QueryMergerPhase::create(z3::ContextPtr context, Optimizer::QueryMergerRule queryMergerRule) {
    return std::make_shared<QueryMergerPhase>(QueryMergerPhase(context, queryMergerRule));
}

QueryMergerPhase::QueryMergerPhase(z3::ContextPtr context, Optimizer::QueryMergerRule queryMergerRuleName) {

    switch (queryMergerRuleName) {
        case QueryMergerRule::SyntaxBasedCompleteQueryMergerRule:
            queryMergerRule = SyntaxBasedCompleteQueryMergerRule::create();
            break;
        case QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule:
            queryMergerRule = Z3SignatureBasedCompleteQueryMergerRule::create(context);
            break;
        case QueryMergerRule::StringSignatureBasedCompleteQueryMergerRule:
        case QueryMergerRule::ImprovedStringSignatureBasedCompleteQueryMergerRule:
            queryMergerRule = StringSignatureBasedCompleteQueryMergerRule::create();
            break;
        case QueryMergerRule::SyntaxBasedPartialQueryMergerRule:
        case QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule:
        case QueryMergerRule::StringSignatureBasedPartialQueryMergerRule: NES_NOT_IMPLEMENTED();
    }
}

bool QueryMergerPhase::execute(GlobalQueryPlanPtr globalQueryPlan) {
    NES_DEBUG("QueryMergerPhase: Executing query merger phase.");
    return queryMergerRule->apply(globalQueryPlan);
}

}// namespace NES::Optimizer
