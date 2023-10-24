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
#include <Optimizer/QueryMerger/MatchedOperatorPair.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Util/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

Z3SignatureBasedCompleteQueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule(z3::ContextPtr context) : BaseQueryMergerRule() {
    signatureEqualityUtil = SignatureEqualityUtil::create(std::move(context));
}

Z3SignatureBasedCompleteQueryMergerRulePtr Z3SignatureBasedCompleteQueryMergerRule::create(z3::ContextPtr context) {
    return std::make_shared<Z3SignatureBasedCompleteQueryMergerRule>(Z3SignatureBasedCompleteQueryMergerRule(std::move(context)));
}

bool Z3SignatureBasedCompleteQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    NES_INFO(
        "Z3SignatureBasedCompleteQueryMergerRule: Applying Signature Based Equal Query Merger Rule to the Global Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("Z3SignatureBasedCompleteQueryMergerRule: Found no new query plan to add in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("Z3SignatureBasedCompleteQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
    for (const auto& targetQueryPlan : queryPlansToAdd) {

        bool matched = false;
        auto hostSharedQueryPlans =
            globalQueryPlan->getSharedQueryPlansConsumingSourcesAndPlacementStrategy(targetQueryPlan->getSourceConsumed(),
                                                                                     targetQueryPlan->getPlacementStrategy());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();
            // Prepare a map of matching address and target sink global query nodes
            // if there are no matching global query nodes then the shared query metadata are not matched
            std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
            auto targetSink = targetQueryPlan->getSinkOperators()[0];
            auto hostSink = hostQueryPlan->getSinkOperators()[0];
            bool foundMatch = false;

            //Check if the host and target sink operator signatures match each other
            if (signatureEqualityUtil->checkEquality(hostSink->getZ3Signature(), targetSink->getZ3Signature())) {
                targetToHostSinkOperatorMap[targetSink] = hostSink;
                foundMatch = true;
            }

            //Not all sinks found an equivalent entry in the target shared query metadata
            if (foundMatch) {
                NES_TRACE("Z3SignatureBasedCompleteQueryMergerRule: Merge target Shared metadata into address metadata");

                //Compute matched operator pairs
                std::vector<MatchedOperatorPairPtr> matchedOperatorPairs;
                matchedOperatorPairs.reserve(targetToHostSinkOperatorMap.size());

                //Iterate over all matched pairs of sink operators and merge the query plan
                for (auto& [targetSinkOperator, hostSinkOperator] : targetToHostSinkOperatorMap) {
                    //add to the matched pair
                    matchedOperatorPairs.emplace_back(MatchedOperatorPair::create(hostSinkOperator->as<LogicalOperatorNode>(),
                                                                                  targetSinkOperator->as<LogicalOperatorNode>(),
                                                                                  ContainmentRelationship::EQUALITY));
                }

                //add matched operators to the host shared query plan
                hostSharedQueryPlan->addQuery(targetQueryPlan->getQueryId(), matchedOperatorPairs);

                //Update the shared query meta data
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query meta data
                matched = true;
                break;
            }
        }

        if (!matched) {
            NES_DEBUG("Z3SignatureBasedCompleteQueryMergerRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

}// namespace NES::Optimizer
