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
#include <Optimizer/QueryMerger/HashSignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/MatchedOperatorPair.hpp>
#include <Util/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

HashSignatureBasedCompleteQueryMergerRulePtr HashSignatureBasedCompleteQueryMergerRule::create() {
    return std::make_shared<HashSignatureBasedCompleteQueryMergerRule>();
}

bool HashSignatureBasedCompleteQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {
    NES_INFO(
        "HashSignatureBasedCompleteQueryMergerRule: Applying Signature Based Equal Query Merger Rule to the Global Query Plan");
    auto queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("HashSignatureBasedCompleteQueryMergerRule: Found no new query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("HashSignatureBasedCompleteQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all query plans to identify the potential sharing opportunities
    for (auto& targetQueryPlan : queryPlansToAdd) {
        bool merged = false;
        auto hostSharedQueryPlans =
            globalQueryPlan->getSharedQueryPlansConsumingSourcesAndPlacementStrategy(targetQueryPlan->getSourceConsumed(),
                                                                                     targetQueryPlan->getPlacementStrategy());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();
            auto hostSignature = hostSharedQueryPlan->getHashBasedSignature();

            // Prepare a map of matching address and target sink global query nodes
            // if there are no matching global query nodes then the shared query metadata are not matched
            std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
            bool foundMatch = false;
            auto targetSinkOperators = targetQueryPlan->getSinkOperators();
            auto targetSignature = targetSinkOperators[0]->getHashBasedSignature();
            auto targetSignatureHashValue = targetSignature.begin()->first;
            auto targetSignatureStringValue = *targetSignature.begin()->second.begin();

            if (hostSignature.find(targetSignatureHashValue) != hostSignature.end()) {
                auto hostSignatureStringValues = hostSignature[targetSignatureHashValue];
                auto match = std::find_if(hostSignatureStringValues.begin(),
                                          hostSignatureStringValues.end(),
                                          [&](const std::string& hostSignatureStringValue) {
                                              return hostSignatureStringValue == targetSignatureStringValue;
                                          });
                if (match != hostSignatureStringValues.end()) {
                    targetToHostSinkOperatorMap[targetSinkOperators[0]] = hostSharedQueryPlan->getSinkOperators()[0];
                    foundMatch = true;
                }
            }

            if (foundMatch) {
                NES_TRACE("HashSignatureBasedCompleteQueryMergerRule: Merge target Shared metadata into address metadata");
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
                merged = true;
                break;
            }
        }

        if (!merged) {
            NES_DEBUG("HashSignatureBasedCompleteQueryMergerRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

}// namespace NES::Optimizer
