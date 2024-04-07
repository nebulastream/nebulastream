/*
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

#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>
#include <z3++.h>

namespace NES::Optimizer {

SignatureInferencePhase::SignatureInferencePhase(z3::ContextPtr context, Optimizer::QueryMergerRule queryMergerRule)
    : context(context), queryMergerRule(queryMergerRule) {
    NES_DEBUG("SignatureInferencePhase()");
}

SignatureInferencePhasePtr SignatureInferencePhase::create(z3::ContextPtr context, Optimizer::QueryMergerRule queryMergerRule) {
    return std::make_shared<SignatureInferencePhase>(SignatureInferencePhase(std::move(context), queryMergerRule));
}

void SignatureInferencePhase::execute(const QueryPlanPtr& queryPlan) {
    if (queryMergerRule == QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule
        || queryMergerRule == QueryMergerRule::HashSignatureBasedPartialQueryMergerRule
        || queryMergerRule == QueryMergerRule::ImprovedHashSignatureBasedCompleteQueryMergerRule
        || queryMergerRule == QueryMergerRule::ImprovedHashSignatureBasedPartialQueryMergerRule) {
        NES_INFO("SignatureInferencePhase: computing String based signature for the query {}", queryPlan->getQueryId());
        auto sinkOperators = queryPlan->getRootOperators();
        for (auto& sinkOperator : sinkOperators) {
            sinkOperator->as<LogicalOperator>()->inferStringSignature();
        }
    } else if (queryMergerRule == QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule
               || queryMergerRule == QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule
               || queryMergerRule == QueryMergerRule::Z3SignatureBasedPartialQueryMergerBottomUpRule
               || queryMergerRule == QueryMergerRule::Z3SignatureBasedBottomUpQueryContainmentRule
               || queryMergerRule == QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule) {
        NES_INFO("SignatureInferencePhase: computing Z3 based signature for the query {}", queryPlan->getQueryId());
        auto sinkOperators = queryPlan->getRootOperators();
        for (auto& sinkOperator : sinkOperators) {
            sinkOperator->as<LogicalOperator>()->inferZ3Signature(context);
        }
    } else if (queryMergerRule == QueryMergerRule::HybridCompleteQueryMergerRule) {
        NES_INFO("SignatureInferencePhase: computing Z3 based signature for the query {}", queryPlan->getQueryId());
        auto sinkOperators = queryPlan->getRootOperators();
        for (auto& sinkOperator : sinkOperators) {
            sinkOperator->as<LogicalOperator>()->inferStringSignature();
        }
    } else {
        NES_INFO("Skipping signature creation");
    }
}

z3::ContextPtr SignatureInferencePhase::getContext() const { return context.getContext(); }

}// namespace NES::Optimizer
