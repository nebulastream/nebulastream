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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedContainmentBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

Z3SignatureBasedContainmentBasedCompleteQueryMergerRule::Z3SignatureBasedContainmentBasedCompleteQueryMergerRule(
    const z3::ContextPtr& contextSig1Contained)
    : BaseQueryMergerRule() {
    signatureContainmentUtil = SignatureContainmentUtil::create(std::move(contextSig1Contained));
}

Z3SignatureBasedContainmentBasedCompleteQueryMergerRulePtr
Z3SignatureBasedContainmentBasedCompleteQueryMergerRule::create(const z3::ContextPtr& contextSig1Contained) {
    return std::make_shared<Z3SignatureBasedContainmentBasedCompleteQueryMergerRule>(
        Z3SignatureBasedContainmentBasedCompleteQueryMergerRule(std::move(contextSig1Contained)));
}

bool Z3SignatureBasedContainmentBasedCompleteQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    NES_INFO("Z3SignatureBasedContainmentBasedCompleteQueryMergerRule: Applying Signature Based Equal Query Merger Rule to the "
             "Global Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING(
            "Z3SignatureBasedContainmentBasedCompleteQueryMergerRule: Found no new query plan to add in the global query plan."
            " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("Z3SignatureBasedContainmentBasedCompleteQueryMergerRule: Iterating over all Shared Query MetaData in the Global "
              "Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
    bool foundMatch = false;
    for (const auto& targetQueryPlan : queryPlansToAdd) {
        bool matched = false;
        auto hostSharedQueryPlans = globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();
            // Prepare a map of matching address and target sink global query nodes
            // if there are no matching global query nodes then the shared query metadata are not matched
            std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
            auto targetSink = targetQueryPlan->getSinkOperators()[0];
            auto hostSink = hostQueryPlan->getSinkOperators()[0];

            //Check if the host and target sink operator signatures match each other
            auto containment = signatureContainmentUtil->checkContainment(hostSink->getZ3Signature(), targetSink->getZ3Signature());
            NES_TRACE("Z3SignatureBasedContainmentBasedCompleteQueryMergerRule: containment: " << containment);
            if (containment != NO_CONTAINMENT) {
                targetToHostSinkOperatorMap[targetSink] = hostSink;
                foundMatch = true;
                matched = true;
            }
        }
        if (!matched) {
            NES_DEBUG("Z3SignatureBasedCompleteQueryMergerRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return foundMatch;
}
const SignatureContainmentUtilPtr& Z3SignatureBasedContainmentBasedCompleteQueryMergerRule::getSignatureContainmentUtil() const {
    return signatureContainmentUtil;
}
}// namespace NES::Optimizer
