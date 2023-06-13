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

#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryContainmentMergerRule.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

Z3SignatureBasedPartialQueryContainmentMergerRule::Z3SignatureBasedPartialQueryContainmentMergerRule(z3::ContextPtr context) {
    SignatureContainmentUtil = SignatureContainmentUtil::create(std::move(context));
}

Z3SignatureBasedPartialQueryContainmentMergerRulePtr
Z3SignatureBasedPartialQueryContainmentMergerRule::create(z3::ContextPtr context) {
    return std::make_shared<Z3SignatureBasedPartialQueryContainmentMergerRule>(
        Z3SignatureBasedPartialQueryContainmentMergerRule(std::move(context)));
}

bool Z3SignatureBasedPartialQueryContainmentMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    NES_INFO2("Applying Signature Based Equal Query Merger Rule to the Global Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING2("Found only a single query metadata in the global query plan."
                     " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG2("Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
    for (const auto& targetQueryPlan : queryPlansToAdd) {
        bool matched = false;
        auto hostSharedQueryPlans =
            globalQueryPlan->getSharedQueryPlansConsumingSourcesAndPlacementStrategy(targetQueryPlan->getSourceConsumed(),
                                                                                     targetQueryPlan->getPlacementStrategy());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {

            std::tuple<ContainmentType, std::vector<LogicalOperatorNodePtr>> relationshipAndOperators;

            //Fetch the host query plan to merge
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();

            //Initialized the target and host matched pair
            std::map<OperatorNodePtr, std::tuple<OperatorNodePtr, ContainmentType, std::vector<LogicalOperatorNodePtr>>>
                matchedTargetToHostOperatorMap;
            //Initialized the vector containing iterated matched host operator
            std::vector<NodePtr> matchedHostOperators;

            //Iterate over the target query plan from sink to source and compare the operator signatures with the host query plan
            //When a match is found then store the matching operators in the matchedTargetToHostOperatorMap
            for (const auto& targetRootOperator : targetQueryPlan->getRootOperators()) {
                //Iterate the target query plan in BFS order.
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
                        auto children = hostRootOperator->getChildren();
                        for (const auto& hostChildren : children) {
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
                            relationshipAndOperators =
                                SignatureContainmentUtil->checkContainmentRelationshipTopDown(hostOperator, targetOperator);
                            if (get<0>(relationshipAndOperators) != ContainmentType::NO_CONTAINMENT) {
                                //Add the matched host operator to the map
                                matchedTargetToHostOperatorMap[targetOperator] =
                                    std::tuple(hostOperator, get<0>(relationshipAndOperators), get<1>(relationshipAndOperators));
                                //Mark the host operator as matched
                                matchedHostOperators.emplace_back(hostOperator);
                                foundMatch = true;
                                break;
                            }

                            //Check for the children operators if a host operator with matching is found on the host
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

                    //Check for the children operators if a host operator with matching is found on the host
                    for (const auto& targetChild : targetOperator->getChildren()) {
                        targetOperators.push_front(targetChild);
                    }
                }
            }

            if (!matchedTargetToHostOperatorMap.empty()) {
                NES_TRACE2("Z3SignatureBasedPartialQueryMergerRule: Merge target Shared metadata into address metadata");
                hostSharedQueryPlan->addQueryIdAndSinkOperators(targetQueryPlan);

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

                //Iterate over all matched pairs of operators and merge the query plan
                for (auto [targetOperator, hostOperatorContainmentRelationshipContainedOperatorChain] :
                     matchedTargetToHostOperatorMap) {
                    if (get<1>(hostOperatorContainmentRelationshipContainedOperatorChain) == ContainmentType::EQUALITY) {
                        for (const auto& targetParent : targetOperator->getParents()) {
                            bool addedNewParent =
                                get<0>(hostOperatorContainmentRelationshipContainedOperatorChain)->addParent(targetParent);
                            if (!addedNewParent) {
                                NES_WARNING2("Z3SignatureBasedPartialQueryMergerRule: Failed to add new parent");
                            }
                            hostSharedQueryPlan->addAdditionToChangeLog(
                                get<0>(hostOperatorContainmentRelationshipContainedOperatorChain),
                                targetParent->as<OperatorNode>());
                            targetOperator->removeParent(targetParent);
                        }
                    } else if (get<1>(hostOperatorContainmentRelationshipContainedOperatorChain)
                               == ContainmentType::RIGHT_SIG_CONTAINED) {
                        auto containerOperator = get<0>(hostOperatorContainmentRelationshipContainedOperatorChain);
                        auto containedOperatorChain = get<2>(hostOperatorContainmentRelationshipContainedOperatorChain);
                        addContainmentOperatorChain(hostSharedQueryPlan,
                                                    containerOperator,
                                                    targetOperator,
                                                    containedOperatorChain);
                    } else if (get<1>(hostOperatorContainmentRelationshipContainedOperatorChain)
                               == ContainmentType::LEFT_SIG_CONTAINED) {
                        auto containedOperatorChain = get<2>(hostOperatorContainmentRelationshipContainedOperatorChain);
                        auto containedOperator = get<0>(hostOperatorContainmentRelationshipContainedOperatorChain);
                        addContainmentOperatorChain(hostSharedQueryPlan,
                                                    targetOperator,
                                                    containedOperator,
                                                    containedOperatorChain);
                    }
                }

                //Add all root operators from target query plan to host query plan
                for (const auto& targetRootOperator : targetQueryPlan->getRootOperators()) {
                    hostQueryPlan->addRootOperator(targetRootOperator);
                }

                //Update the shared query metadata
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query metadata
                matched = true;
                break;
            }
        }

        if (!matched) {
            NES_DEBUG2("Z3SignatureBasedPartialQueryMergerRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    for (const auto& item : globalQueryPlan->getAllSharedQueryPlans()) {
        NES_TRACE2("Shared Query Plans after merging: {}", item->getQueryPlan()->toString());
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

void Z3SignatureBasedPartialQueryContainmentMergerRule::addContainmentOperatorChain(
    SharedQueryPlanPtr& containerQueryPlan,
    const OperatorNodePtr& containerOperator,
    const OperatorNodePtr& containedOperator,
    const std::vector<LogicalOperatorNodePtr> containedOperatorChain) const {

    auto downstreamOperator = containedOperatorChain.front();
    auto upstreamContainedOperator = containedOperatorChain.back();
    NES_TRACE2("ContainerQueryPlan: {}", containerQueryPlan->getQueryPlan()->toString());
    NES_TRACE2("ContainerOperator: {}", containerOperator->toString());
    NES_TRACE2("DownstreamOperator: {}", downstreamOperator->toString());
    NES_TRACE2("ContainedOperator: {}", containedOperator->toString());
    NES_TRACE2("upstreamContainedOperator: {}", upstreamContainedOperator->toString());
    if (!containedOperator->equal(downstreamOperator)) {
        downstreamOperator->removeAllParent();
        if (containedOperator->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
            containedOperator->removeChildren();
            downstreamOperator->addParent(containedOperator);
        } else {
            auto parents = containedOperator->getParents();
            for (const auto& parent : parents) {
                parent->removeChildren();
                NES_TRACE2("Parent: {}", parent->toString());
                downstreamOperator->addParent(parent);
            }
        }
    }
    //remove the children of the upstreamOperator
    upstreamContainedOperator->removeChildren();
    //If we deal with contained filter operators, we have to correctly prepare the operator chain to add to the container's
    //equivalent operator chain.
    //To do so, we add the most upstream filter operator as a parent to the container's equivalent operator chain.
    //Then, we add iteratively add all downstream filter operators to the corresponding upstream filter operators as parents.
    if (upstreamContainedOperator->instanceOf<FilterLogicalOperatorNode>()) {
        NES_TRACE2("FilterOperator: {}", upstreamContainedOperator->toString());
        for (const auto& filterOperator : containedOperatorChain) {
            NES_TRACE2("FilterOperators: {}", filterOperator->toString());
            //if downstreamOperator and filterOperator are not equal, add the downstream filter operation as a parent
            //to the currently traversed filter operator
            filterOperator->removeChildren();
            if (downstreamOperator->equal(filterOperator)) {
                continue;
            }
            filterOperator->removeAllParent();
            filterOperator->addParent(downstreamOperator);
            downstreamOperator = filterOperator->as<LogicalOperatorNode>();
        }
    }
    //If we encounter a watermark operator, we have to obtain its children and add the contained
    //operator chain to the child not the watermark
    if (containerOperator->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        //Watermark operators only have one child
        auto containersChild = containerOperator->getChildren()[0];
        //add the contained operator chain to the host operator's child
        bool addedNewParent = containersChild->addParent(upstreamContainedOperator);
        if (!addedNewParent) {
            NES_WARNING2("Z3SignatureBasedPartialQueryMergerRule: Failed to add new parent");
        }
        containerQueryPlan->addAdditionToChangeLog(containersChild->as<OperatorNode>(),
                                                   upstreamContainedOperator->as<OperatorNode>());
    } else {
        //add the contained operator chain to the host operator
        bool addedNewParent = containerOperator->addParent(upstreamContainedOperator);
        if (!addedNewParent) {
            NES_WARNING2("Z3SignatureBasedPartialQueryMergerRule: Failed to add new parent");
        }
        containerQueryPlan->addAdditionToChangeLog(containerOperator, upstreamContainedOperator->as<OperatorNode>());
    }
}
}// namespace NES::Optimizer
