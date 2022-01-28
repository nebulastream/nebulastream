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
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/Utils/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <utility>
#include <z3++.h>

namespace NES::Optimizer {

Z3SignatureBasedPartialQueryMergerRule::Z3SignatureBasedPartialQueryMergerRule(z3::ContextPtr context) {
    signatureEqualityUtil = SignatureEqualityUtil::create(std::move(context));
}

Z3SignatureBasedPartialQueryMergerRulePtr Z3SignatureBasedPartialQueryMergerRule::create(z3::ContextPtr context) {
    return std::make_shared<Z3SignatureBasedPartialQueryMergerRule>(Z3SignatureBasedPartialQueryMergerRule(std::move(context)));
}

bool Z3SignatureBasedPartialQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    NES_INFO("Z3SignatureBasedPartialQueryMergerRule: Applying Signature Based Equal Query Merger Rule to the Global Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("Z3SignatureBasedPartialQueryMergerRule: Found only a single query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("Z3SignatureBasedPartialQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
    for (const auto& targetQueryPlan : queryPlansToAdd) {
        bool matched = false;
        auto hostSharedQueryPlans = globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {

            //Fetch the host query plan to merge
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();

            //Initialized the target and host matched pair
            std::map<OperatorNodePtr, OperatorNodePtr> matchedTargetToHostOperatorMap;
            //Initialized the vector containing iterated matched host operator
            std::vector<NodePtr> matchedHostOperators;

            //Iterate over the target query plan and compare the operator signatures with the host query plan
            //When a match is found then store the matching operators in the matchedTargetToHostOperatorMap
            for (const auto& targetRootOperator : targetQueryPlan->getRootOperators()) {
                //Iterate the target query plan in DFS order.
                auto targetChildren = targetRootOperator->getChildren();
                std::deque<NodePtr> targetOperators = {targetChildren.begin(), targetChildren.end()};
                //Iterate till target operators are remaining to be matched
                while (!targetOperators.empty()) {

                    //Extract the front of the queue and check if there is a matching operator in the
                    // host query plan
                    bool foundMatch = false;
                    auto targetOperator = targetOperators.front()->as<LogicalOperatorNode>();
                    targetOperators.pop_front();

                    //Skip if the target operator is already matched
                    if (matchedTargetToHostOperatorMap.find(targetOperator) != matchedTargetToHostOperatorMap.end()) {
                        continue;
                    }

                    //Initialize the visited host operator list
                    std::vector<NodePtr> visitedHostOperators;

                    //Iterate the host query plan in BFS order and check if an operator with matching signature with the target operator
                    // exists.
                    for (const auto& hostRootOperator : hostQueryPlan->getRootOperators()) {
                        //Initialize the host operators to traverse
                        std::deque<NodePtr> hostOperators;
                        for (const auto& hostChildren : hostRootOperator->getChildren()) {
                            //Only add the host operators which were not traversed earlier
                            if (std::find(visitedHostOperators.begin(), visitedHostOperators.end(), hostChildren)
                                == visitedHostOperators.end()) {
                                hostOperators.push_back(hostChildren);
                            }
                        }

                        //Iterate till a matching host operator is not found or till the host operators are available to
                        // perform matching
                        while (!hostOperators.empty()) {
                            //Take out the front of the queue and add the host operator to the visited list
                            auto hostOperator = hostOperators.front()->as<LogicalOperatorNode>();
                            visitedHostOperators.emplace_back(hostOperator);
                            hostOperators.pop_front();

                            //Skip matching if the host operator is already matched
                            if (std::find(matchedHostOperators.begin(), matchedHostOperators.end(), hostOperator)
                                != matchedHostOperators.end()) {
                                continue;
                            }

                            //Match the target and host operator signatures to see if a match is present
                            if (signatureEqualityUtil->checkEquality(targetOperator->getZ3Signature(),
                                                                     hostOperator->getZ3Signature())) {
                                //Add the matched host operator to the map
                                matchedTargetToHostOperatorMap[targetOperator] = hostOperator;
                                //Mark the host operator as matched
                                matchedHostOperators.emplace_back(hostOperator);
                                foundMatch = true;
                                break;
                            }

                            //Check for the children operators if a host operator with matching is found on the host
                            for (const auto& hostChild : hostOperator->getChildren()) {
                                //Only add the host operators in the back of the queue which were not traversed earlier
                                if (std::find(visitedHostOperators.begin(), visitedHostOperators.end(), hostChild)
                                    == visitedHostOperators.end()) {
                                    hostOperators.push_back(hostChild);
                                }
                            }
                        }
                        if (foundMatch) {
                            break;
                        }
                    }

                    //If a match is found then no need to look for a matching downstream operator chain
                    if (foundMatch) {
                        continue;
                    }

                    //Check for the children operators if a host operator with matching is found on the host
                    for (const auto& targetChild : targetOperator->getChildren()) {
                        targetOperators.push_front(targetChild);
                    }
                }
            }

            if (!matchedTargetToHostOperatorMap.empty()) {
                NES_TRACE("Z3SignatureBasedPartialQueryMergerRule: Merge target Shared metadata into address metadata");
                hostSharedQueryPlan->addQueryIdAndSinkOperators(targetQueryPlan);

                // As we merge partially equivalent queries, we can potentially find matches across multiple operators.
                // As upstream matched operators are covered by downstream matched operators. We need to retain only the
                // downstream matched operator containing any upstream matched operator. This will prevent in computation
                // of inconsistent shared query plans.

                if (matchedTargetToHostOperatorMap.size() > 1) {
                    //Fetch all the matched target operators.
                    std::vector<OperatorNodePtr> matchedTargetOperators;
                    matchedTargetOperators.reserve(matchedTargetToHostOperatorMap.size());
                    for (auto& mapEntry : matchedTargetToHostOperatorMap) {
                        matchedTargetOperators.emplace_back(mapEntry.first);
                    }

                    //Iterate over the target operators and remove the upstream operators covered by downstream matched operators
                    for (uint64_t i = 0; i < matchedTargetOperators.size(); i++) {
                        for (uint64_t j = 0; j < matchedTargetOperators.size(); j++) {
                            if (i == j) {
                                continue;//Skip chk with itself
                            }

                            if (matchedTargetOperators[i]->containAsGrandChild(matchedTargetOperators[j])) {
                                matchedTargetToHostOperatorMap.erase(matchedTargetOperators[j]);
                            } else if (matchedTargetOperators[i]->containAsGrandParent(matchedTargetOperators[j])) {
                                matchedTargetToHostOperatorMap.erase(matchedTargetOperators[i]);
                                break;
                            }
                        }
                    }
                }

                //Iterate over all matched pairs of operators and merge the query plan
                for (auto [targetOperator, hostOperator] : matchedTargetToHostOperatorMap) {
                    for (const auto& targetParent : targetOperator->getParents()) {
                        bool addedNewParent = hostOperator->addParent(targetParent);
                        if (!addedNewParent) {
                            NES_WARNING("Z3SignatureBasedPartialQueryMergerRule: Failed to add new parent");
                        }
                        hostSharedQueryPlan->addAdditionToChangeLog(hostOperator, targetParent->as<OperatorNode>());
                        targetOperator->removeParent(targetParent);
                    }
                }

                //Add all root operators from target query plan to host query plan
                for (const auto& targetRootOperator : targetQueryPlan->getRootOperators()) {
                    hostQueryPlan->addRootOperator(targetRootOperator);
                }

                //Update the shared query meta data
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query meta data
                matched = true;
                break;
            }
        }

        if (!matched) {
            NES_DEBUG("Z3SignatureBasedPartialQueryMergerRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeEmptySharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}
}// namespace NES::Optimizer
