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
#include <Operators/LogicalOperators/Sinks/LogicalSinkOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/LogicalWatermarkAssignerOperator.hpp>
#include <Optimizer/QueryMerger/MatchedOperatorPair.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedTreeBasedQueryContainmentMergerRule.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentCheck.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QuerySignatures/QuerySignature.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <utility>

namespace NES::Optimizer {

Z3SignatureBasedTreeBasedQueryContainmentMergerRule::Z3SignatureBasedTreeBasedQueryContainmentMergerRule(
    z3::ContextPtr context,
    bool allowExhaustiveContainmentCheck) {
    SignatureContainmentUtil = SignatureContainmentCheck::create(std::move(context), allowExhaustiveContainmentCheck);
}

Z3SignatureBasedTreeBasedQueryContainmentMergerRulePtr
Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(z3::ContextPtr context, bool allowExhaustiveContainmentCheck) {
    return std::make_shared<Z3SignatureBasedTreeBasedQueryContainmentMergerRule>(
        Z3SignatureBasedTreeBasedQueryContainmentMergerRule(std::move(context), allowExhaustiveContainmentCheck));
}

bool Z3SignatureBasedTreeBasedQueryContainmentMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    NES_INFO("Applying Z3SignatureBasedPartialQueryContainmentMergerRule to the Global Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("Found only a single query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
    for (const auto& targetQueryPlan : queryPlansToAdd) {
        bool matched = false;
        auto hostSharedQueryPlans =
            globalQueryPlan->getSharedQueryPlansConsumingSourcesAndPlacementStrategy(targetQueryPlan->getSourceConsumed(),
                                                                                     targetQueryPlan->getPlacementStrategy());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
            ContainmentRelationshipAndOperatorChainPtr relationshipAndOperators;

            //Fetch the host query plan to merge
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();

            //Initialized the target and host matched pair
            std::map<OperatorNodePtr, std::tuple<OperatorNodePtr, ContainmentRelationship, std::vector<LogicalOperatorNodePtr>>>
                matchedTargetToHostOperatorMap;
            //Initialized the vector containing iterated matched host operator
            std::vector<NodePtr> matchedHostOperators;

            //Iterate over the target query plan from sink to source and compare the operator signatures with the host query plan
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
                    auto targetOperator = targetOperators.front()->as<LogicalOperator>();
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
                        auto children = hostRootOperator->getChildren();
                        for (const auto& hostChildren : children) {
                            //Only add the host operators which were not traversed earlier
                            if (std::find(visitedHostOperators.begin(), visitedHostOperators.end(), hostChildren)
                                == visitedHostOperators.end()) {
                                hostOperators.push_back(hostChildren);
                            }
                        }

                        //Iterate till a matching host operator is not found or till the host operators are available to
                        //perform matching
                        while (!hostOperators.empty()) {
                            //Take out the front of the queue and add the host operator to the visited list
                            auto hostOperator = hostOperators.front()->as<LogicalOperator>();
                            visitedHostOperators.emplace_back(hostOperator);
                            hostOperators.pop_front();

                            //Skip matching if the host operator is already matched
                            if (std::find(matchedHostOperators.begin(), matchedHostOperators.end(), hostOperator)
                                != matchedHostOperators.end()) {
                                continue;
                            }

                            //Match the target and host operator signatures to see if a match is present
                            relationshipAndOperators =
                                SignatureContainmentUtil->checkContainmentRelationshipForTopDownMerging(hostOperator,
                                                                                                        targetOperator);
                            if (relationshipAndOperators->containmentRelationship != ContainmentRelationship::NO_CONTAINMENT) {
                                //Add the matched host operator to the map
                                matchedTargetToHostOperatorMap[targetOperator] =
                                    std::tuple(hostOperator,
                                               relationshipAndOperators->containmentRelationship,
                                               relationshipAndOperators->containedOperatorChain);
                                //Mark the host operator as matched
                                matchedHostOperators.emplace_back(hostOperator);
                                foundMatch = true;
                                break;
                            }

                            //Check for the children operators of a host operator with matching is found on the host
                            auto hostOperatorChildren = hostOperator->getChildren();
                            for (const auto& hostChild : hostOperatorChildren) {
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

                    //Check for the children operators if no host operator with matching is found on the host
                    for (const auto& targetChild : targetOperator->getChildren()) {
                        targetOperators.push_front(targetChild);
                    }
                }
            }

            if (!matchedTargetToHostOperatorMap.empty()) {
                NES_TRACE("Z3SignatureBasedPartialQueryMergerRule: Merge target Shared metadata into address metadata");

                // As we merge partially equivalent queryIdAndCatalogEntryMapping, we can potentially find matches across multiple operators.
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

                std::vector<MatchedOperatorPairPtr> matchedOperatorPairs;
                matchedOperatorPairs.reserve(matchedTargetToHostOperatorMap.size());

                NES_TRACE("Size of matchedTargetToHostOperatorMap: {}", std::to_string(matchedTargetToHostOperatorMap.size()));

                //Iterate over all matched pairs of operators and merge the query plan
                for (auto [targetOperator, hostOperatorContainmentRelationshipContainedOperatorChain] :
                     matchedTargetToHostOperatorMap) {
                    //In case of equivalence detection, just add the matched operator pair to the list with the correct containment
                    //relationship
                    if (get<1>(hostOperatorContainmentRelationshipContainedOperatorChain) == ContainmentRelationship::EQUALITY) {
                        LogicalOperatorNodePtr hostOperator =
                            get<0>(hostOperatorContainmentRelationshipContainedOperatorChain)->as<LogicalOperator>();
                        matchedOperatorPairs.emplace_back(MatchedOperatorPair::create(hostOperator,
                                                                                      targetOperator->as<LogicalOperator>(),
                                                                                      ContainmentRelationship::EQUALITY));
                        //In case the host contains the target, we first need to prepare the query plan and then add the matched operators
                    } else if (get<1>(hostOperatorContainmentRelationshipContainedOperatorChain)
                               == ContainmentRelationship::RIGHT_SIG_CONTAINED) {
                        auto containerOperator = get<0>(hostOperatorContainmentRelationshipContainedOperatorChain);
                        auto containedOperatorChain = get<2>(hostOperatorContainmentRelationshipContainedOperatorChain);
                        //prepare the new query plan with the correct containment relationship, i.e., add the containedOperatorChain to
                        //the containerOperator and set the targetOperator's parents as parents of the containedOperatorChain
                        addContainmentOperatorChain(hostSharedQueryPlan,
                                                    containerOperator,
                                                    targetOperator,
                                                    containedOperatorChain);
                        matchedOperatorPairs.emplace_back(
                            MatchedOperatorPair::create(containerOperator->as<LogicalOperator>(),
                                                        containedOperatorChain.back()->as<LogicalOperator>(),
                                                        ContainmentRelationship::RIGHT_SIG_CONTAINED));
                        //In case the target contains the host, we first need to prepare the query plan and then add the matched operators
                        //if allowExhaustiveContainmentCheck is not explicitly set to true, this case will not be reached
                    } else if (get<1>(hostOperatorContainmentRelationshipContainedOperatorChain)
                               == ContainmentRelationship::LEFT_SIG_CONTAINED) {
                        auto containedOperatorChain = get<2>(hostOperatorContainmentRelationshipContainedOperatorChain);
                        auto containedOperator = get<0>(hostOperatorContainmentRelationshipContainedOperatorChain);
                        //prepare the new query plan with the correct containment relationship, i.e., add the containedOperatorChain to
                        //the targetOperator and set the containedOperator's parents as parents of the containedOperatorChain
                        addContainmentOperatorChain(hostSharedQueryPlan,
                                                    targetOperator,
                                                    containedOperator,
                                                    containedOperatorChain);
                        matchedOperatorPairs.emplace_back(
                            MatchedOperatorPair::create(targetOperator->as<LogicalOperator>(),
                                                        containedOperatorChain.back()->as<LogicalOperator>(),
                                                        ContainmentRelationship::LEFT_SIG_CONTAINED));
                    }
                }

                hostSharedQueryPlan->addQuery(targetQueryPlan->getQueryId(), matchedOperatorPairs);

                //Update the shared query metadata
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query metadata
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
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

void Z3SignatureBasedTreeBasedQueryContainmentMergerRule::addContainmentOperatorChain(
    SharedQueryPlanPtr& containerQueryPlan,
    const OperatorNodePtr& containerOperator,
    const OperatorNodePtr& containedOperator,
    const std::vector<LogicalOperatorNodePtr> containedOperatorChain) const {

    auto downstreamOperator = containedOperatorChain.front();
    auto upstreamContainedOperator = containedOperatorChain.back();
    NES_TRACE("ContainerQueryPlan: {}; ContainerOperator: {}; DownstreamOperator: {}; ContainedOperator: {}; "
              "UpstreamContainedOperator: {}; DownstreamOperator's children size: {}",
              containerQueryPlan->getQueryPlan()->toString(),
              containerOperator->toString(),
              downstreamOperator->toString(),
              containedOperator->toString(),
              upstreamContainedOperator->toString(),
              downstreamOperator->getChildren().size());
    //extract the parents of the containedOperator
    auto parents = containedOperator->getParents();
    for (const auto& parent : parents) {
        //for each parent from the containedOperator, remove all children and add the downstreamOperator
        // from the contained operator chain as a child
        parent->removeChild(containedOperator);
        NES_TRACE("Parent: {}", parent->toString());
        downstreamOperator->addParent(parent);
    }
    //add the contained operator chain to the host operator
    bool addedNewParent = containerOperator->addParent(upstreamContainedOperator);
    NES_TRACE("Children upstreamContainedOperator size: {}; Children upstreamContainedOperator: {}",
              upstreamContainedOperator->getChildren().size(),
              upstreamContainedOperator->getChildren()[0]->toString());
    if (!addedNewParent) {
        NES_WARNING("Z3SignatureBasedPartialQueryMergerRule: Failed to add new parent");
    }
}
}// namespace NES::Optimizer
