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
#include <Optimizer/QueryMerger/Z3SignatureBasedQueryContainmentRule.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Optimizer {

Z3SignatureBasedQueryContainmentRule::Z3SignatureBasedQueryContainmentRule(const z3::ContextPtr& context)
    : BaseQueryMergerRule() {
    signatureContainmentUtil = SignatureContainmentUtil::create(std::move(context));
}

Z3SignatureBasedQueryContainmentRulePtr Z3SignatureBasedQueryContainmentRule::create(const z3::ContextPtr& context) {
    return std::make_shared<Z3SignatureBasedQueryContainmentRule>(Z3SignatureBasedQueryContainmentRule(std::move(context)));
}

bool Z3SignatureBasedQueryContainmentRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    NES_INFO("Z3SignatureBasedQueryContainmentRule: Applying Signature Based Equal Query Merger Rule to the "
             "Global Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("Z3SignatureBasedQueryContainmentRule: Found no new query plan to add in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("Z3SignatureBasedQueryContainmentRule: Iterating over all Shared Query MetaData in the Global "
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
            auto targetSink = targetQueryPlan->getSinkOperators()[0];
            auto hostSink = hostQueryPlan->getSinkOperators()[0];

            //Check if the host and target sink operator signatures match each other
            auto containment =
                signatureContainmentUtil->checkContainment(hostSink->getZ3Signature(), targetSink->getZ3Signature());
            NES_TRACE("Z3SignatureBasedQueryContainmentRule: containment: " << magic_enum::enum_name(containment));
            //todo: #3503 create a containment based query merger to update the GQP based on containment relationships
            if (containment != ContainmentType::NO_CONTAINMENT) {
                foundMatch = true;
                matched = true;
            }
        }
        if (!matched) {
            NES_DEBUG("Z3SignatureBasedQueryContainmentRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return foundMatch;
}
}// namespace NES::Optimizer
