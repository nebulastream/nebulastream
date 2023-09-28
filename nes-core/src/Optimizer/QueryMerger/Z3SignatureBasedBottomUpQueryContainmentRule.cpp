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

#include <API/Schema.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedBottomUpQueryContainmentRule.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentCheck.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/Watermark.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <utility>

namespace NES::Optimizer {

Z3SignatureBasedBottomUpQueryContainmentRule::Z3SignatureBasedBottomUpQueryContainmentRule(const z3::ContextPtr& context, bool allowExhaustiveContainmentCheck)
    : BaseQueryMergerRule() {
    // For the bottom up case, we always allow the SQP as containee and therefore pass true as the second parameter
    signatureContainmentUtil = SignatureContainmentCheck::create(std::move(context), allowExhaustiveContainmentCheck);
}

Z3SignatureBasedBottomUpQueryContainmentRulePtr
Z3SignatureBasedBottomUpQueryContainmentRule::create(const z3::ContextPtr& context, bool allowExhaustiveContainmentCheck) {
    return std::make_shared<Z3SignatureBasedBottomUpQueryContainmentRule>(
        Z3SignatureBasedBottomUpQueryContainmentRule(std::move(context), allowExhaustiveContainmentCheck));
}

//FIXME:  we have issues in this logic and this will be taken care of in #3856
bool Z3SignatureBasedBottomUpQueryContainmentRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

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
    for (const auto& targetQueryPlan : queryPlansToAdd) {
        bool matched = false;
        auto hostSharedQueryPlans =
            globalQueryPlan->getSharedQueryPlansConsumingSourcesAndPlacementStrategy(targetQueryPlan->getSourceConsumed(),
                                                                                     targetQueryPlan->getPlacementStrategy());
        NES_DEBUG("HostSharedQueryPlans empty? {}", hostSharedQueryPlans.empty());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
            //Fetch the host query plan to merge
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();
            NES_DEBUG("HostSharedQueryPlan: {}", hostQueryPlan->toString());
            NES_DEBUG("TargetQueryPlan: {}", targetQueryPlan->toString());
            //Check if the host and target sink operator signatures match each other
            std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
            auto targetSink = targetQueryPlan->getSinkOperators()[0];
            auto hostSink = hostQueryPlan->getSinkOperators()[0];
            bool foundMatch = false;
            //Before the bottom up check, we first check the whole query for equality.
            if (signatureContainmentUtil->checkContainmentForBottomUpMerging(hostSink, targetSink)->containmentRelationship
                == ContainmentRelationship::EQUALITY) {
                NES_TRACE("Z3SignatureBasedCompleteQueryMergerRule: Merge target Shared metadata into address metadata");
                //Get children of target and host sink operators
                auto targetSinkChildren = targetSink->getChildren();
                auto hostSinkChildren = hostSink->getChildren();
                //Iterate over target children operators and migrate their parents to the host children operators.
                // Once done, remove the target parent from the target children.
                for (auto& targetSinkChild : targetSinkChildren) {
                    for (auto& hostChild : hostSinkChildren) {
                        bool addedNewParent = hostChild->addParent(targetSink);
                        if (!addedNewParent) {
                            NES_WARNING("Z3SignatureBasedCompleteQueryMergerRule: Failed to add new parent");
                        }
                        //hostSharedQueryPlan->addAdditionToChangeLog(hostChild->as<OperatorNode>(), targetSink);
                    }
                    targetSinkChild->removeParent(targetSink);
                }
                //Add target sink operator as root to the host query plan.
                hostQueryPlan->addRootOperator(targetSink);
            } else {
                //create a map of matching target to address operator id map
                auto matchedTargetToHostOperatorMap = areQueryPlansContained(targetQueryPlan, hostQueryPlan);
                NES_DEBUG("matchedTargetToHostOperatorMap empty? {}", matchedTargetToHostOperatorMap.empty());
                if (!matchedTargetToHostOperatorMap.empty()) {
                    bool unionOrJoin = false;
                    if (matchedTargetToHostOperatorMap.size() > 1) {
                        //Fetch all the matched target operators.
                        std::vector<LogicalOperatorNodePtr> matchedTargetOperators;
                        matchedTargetOperators.reserve(matchedTargetToHostOperatorMap.size());
                        for (auto& [leftQueryOperators, rightQueryOperatorsAndRelationship] : matchedTargetToHostOperatorMap) {
                            if (std::get<1>(rightQueryOperatorsAndRelationship) == ContainmentRelationship::EQUALITY) {
                                matchedTargetOperators.emplace_back(leftQueryOperators);
                            }
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
                    for (auto [targetOp, hostOperatorAndRelationship] : matchedTargetToHostOperatorMap) {
                        LogicalOperatorNodePtr targetOperator = targetOp;
                        LogicalOperatorNodePtr hostOperator = std::get<0>(hostOperatorAndRelationship);
                        ContainmentRelationship containmentType = std::get<1>(hostOperatorAndRelationship);
                        if (containmentType == ContainmentRelationship::EQUALITY) {
                            NES_TRACE("Current host sqp {}; Output schema equality target {}; Output schema equality host {}; "
                                      "Target parent size {}",
                                      hostSharedQueryPlan->getQueryPlan()->toString(),
                                      targetOperator->getOutputSchema()->toString(),
                                      hostOperator->getOutputSchema()->toString(),
                                      targetOperator->getParents().size());
                            auto targetOperatorParents = targetOperator->getParents();
                            for (const auto& targetParent : targetOperatorParents) {
                                NES_DEBUG("Removing parent {}", targetParent->toString());
                                NES_DEBUG("from {}", targetOperator->toString());
                                bool addedNewParent = hostOperator->addParent(targetParent);
                                if (!addedNewParent) {
                                    NES_WARNING("Failed to add new parent");
                                }
                                targetOperator->removeParent(targetParent);
                            }
                        } else if (std::get<1>(hostOperatorAndRelationship) == ContainmentRelationship::RIGHT_SIG_CONTAINED
                                   && checkWindowContainmentPossible(hostOperator, targetOperator)) {
                            //if we're adding a window, we first need to obtain the watermark for that window
                            if (targetOperator->instanceOf<WindowOperatorNode>()) {
                                targetOperator = targetOperator->getChildren()[0]->as<LogicalOperatorNode>();
                            }
                            //obtain the child operation of the sink operator to merge the correct containment relationship
                            if (hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
                                //sink operator should only have one child
                                if (hostOperator->getChildren().size() != 1) {
                                    NES_DEBUG("Sink operator has more than one child");
                                    continue;
                                }
                                hostOperator = hostOperator->getChildren()[0]->as<LogicalOperatorNode>();
                            }
                            NES_TRACE("Adding parent {} to {}", targetOperator->toString(), hostOperator->toString());
                            //in case target operator has more than one child (e.g. join or union) obtain the parent operator
                            if (targetOperator->getChildren().size() > 1) {
                                //will only have one parent operator
                                targetOperator = targetOperator->getParents()[0]->as<LogicalOperatorNode>();
                            }
                            targetOperator->removeChildren();
                            NES_TRACE("Current host operator: {}", hostOperator->toString());
                            bool addedNewParent = hostOperator->addParent(targetOperator);
                            if (!addedNewParent) {
                                NES_WARNING("Failed to add new parent");
                            }
                            //hostSharedQueryPlan->addAdditionToChangeLog(std::get<0>(hostOperatorAndRelationship), targetOperator);
                            NES_TRACE("New shared query plan: {}", hostSharedQueryPlan->getQueryPlan()->toString());
                        } else if (std::get<1>(hostOperatorAndRelationship) == ContainmentRelationship::LEFT_SIG_CONTAINED
                                   && checkWindowContainmentPossible(targetOperator, hostOperator)) {
                            //if we're adding a window, we first need to obtain the watermark for that window
                            if (hostOperator->instanceOf<WindowOperatorNode>()) {
                                hostOperator = hostOperator->getChildren()[0]->as<LogicalOperatorNode>();
                            }
                            //obtain the child operation of the sink operator to merge the correct containment relationship
                            if (targetOperator->instanceOf<SinkLogicalOperatorNode>()) {
                                //sink operator should only have one child
                                if (targetOperator->getChildren().size() != 1) {
                                    NES_DEBUG("Sink operator has more than one child");
                                    continue;
                                }
                                targetOperator = targetOperator->getChildren()[0]->as<LogicalOperatorNode>();
                            }
                            NES_TRACE("Adding parent {} to {}", hostOperator->toString(), targetOperator->toString());
                            //we cannot match union or logical operator nodes because they cannot be safely merged as the sqp is not a tree
                            if (hostOperator->instanceOf<UnionLogicalOperatorNode>()
                                || hostOperator->instanceOf<JoinLogicalOperatorNode>()) {
                                unionOrJoin = true;
                                break;
                            }
                            hostOperator->removeChildren();
                            NES_TRACE("Current host operator: {}", targetOperator->toString());
                            bool addedNewParent = targetOperator->addParent(hostOperator);
                            if (!addedNewParent) {
                                NES_WARNING("Failed to add new parent");
                            }
                            //hostSharedQueryPlan->addAdditionToChangeLog(targetOperator, hostOperator);
                            NES_DEBUG("New shared query plan: {}", hostSharedQueryPlan->getQueryPlan()->toString());
                        }
                    }
                    //Add all root operators from target query plan to host query plan
                    for (const auto& targetRootOperator : targetQueryPlan->getRootOperators()) {
                        NES_DEBUG("Adding root operator {} to host query plan {}",
                                  targetRootOperator->toString(),
                                  hostQueryPlan->toString());
                        hostQueryPlan->addRootOperator(targetRootOperator);
                        NES_DEBUG("Adding root operator {} to host query plan {}",
                                  targetRootOperator->toString(),
                                  hostQueryPlan->toString());
                    }
                    if (!unionOrJoin) {
                        matched = true;
                    }
                }
            }
            //Update the shared query metadata
            globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
            // exit the for loop as we found a matching address shared query metadata
            break;
        }
        if (!matched) {
            NES_DEBUG("Z3SignatureBasedQueryContainmentRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

bool Z3SignatureBasedBottomUpQueryContainmentRule::checkWindowContainmentPossible(const LogicalOperatorNodePtr& container,
                                                                                  const LogicalOperatorNodePtr& containee) const {
    //check that containee is a WindowOperatorNode if yes, go on, if no, return false
    if (containee->instanceOf<WindowOperatorNode>()) {
        auto containeeWindowDefinition = containee->as<WindowOperatorNode>()->getWindowDefinition();
        //check that containee is a time based window, else return false
        if (containeeWindowDefinition->getWindowType()->isTimeBasedWindowType()) {
            auto containeeTimeBasedWindow =
                containeeWindowDefinition->getWindowType()->asTimeBasedWindowType(containeeWindowDefinition->getWindowType());
            //we need to set the time characteristic field to start because the previous timestamp will not exist anymore
            auto field = container->getOutputSchema()->hasFieldName("start");
            //return false if this is not possible
            if (field == nullptr) {
                return false;
            }
            containeeTimeBasedWindow->getTimeCharacteristic()->setField(field);
            NES_TRACE("Window containment possible.");
            return true;
        }
        NES_TRACE("Window containment impossible.");
        return false;
    }
    return true;
}

std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentRelationship>>
Z3SignatureBasedBottomUpQueryContainmentRule::areQueryPlansContained(const QueryPlanPtr& hostQueryPlan,
                                                                     const QueryPlanPtr& targetQueryPlan) {

    std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentRelationship>> targetHostOperatorMap;
    NES_DEBUG("Check if the target and address query plans are syntactically "
              "contained.");
    auto targetSourceOperators = targetQueryPlan->getSourceOperators();
    auto hostSourceOperators = hostQueryPlan->getSourceOperators();

    if (targetSourceOperators.size() != hostSourceOperators.size()) {
        NES_WARNING("Not matched as number of Sources in target and host query plans are "
                    "different.");
        return {};
    }

    //Fetch the first source operator and find a corresponding matching source operator in the address source operator list
    for (auto& targetSourceOperator : targetSourceOperators) {
        NES_DEBUG("TargetSourceOperator: {}", targetSourceOperator->toString());
        for (auto& hostSourceOperator : hostSourceOperators) {
            NES_DEBUG("HostSourceOperator: {}", hostSourceOperator->toString());
            auto matchedOperators = areOperatorsContained(hostSourceOperator, targetSourceOperator);
            if (!matchedOperators.empty()) {
                targetHostOperatorMap.merge(matchedOperators);
                break;
            }
        }
    }
    return targetHostOperatorMap;
}

std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentRelationship>>
Z3SignatureBasedBottomUpQueryContainmentRule::areOperatorsContained(const LogicalOperatorNodePtr& hostOperator,
                                                                    const LogicalOperatorNodePtr& targetOperator) {

    std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentRelationship>> targetHostOperatorMap;
    if (targetOperator->instanceOf<SinkLogicalOperatorNode>() && hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
        NES_DEBUG("Both target and host operators are of sink type.");
        return {};
    }

    NES_DEBUG("Compare target {} and host {} operators.", targetOperator->toString(), hostOperator->toString());
    auto containmentInformation = signatureContainmentUtil->checkContainmentForBottomUpMerging(hostOperator, targetOperator);
    auto containmentType = containmentInformation->containmentRelationship;
    if (containmentType == ContainmentRelationship::EQUALITY) {
        NES_DEBUG("Check containment relationship for parents of target operator.");
        uint16_t matchCount = 0;
        for (const auto& targetParent : targetOperator->getParents()) {
            NES_DEBUG("TargetParent: {}", targetParent->toString());
            for (const auto& hostParent : hostOperator->getParents()) {
                NES_DEBUG("HostParent: {}", hostParent->toString());
                auto matchedOperators =
                    areOperatorsContained(hostParent->as<LogicalOperatorNode>(), targetParent->as<LogicalOperatorNode>());
                if (!matchedOperators.empty()) {
                    targetHostOperatorMap.merge(matchedOperators);
                    matchCount++;
                    break;
                }
            }
        }

        if (matchCount < targetOperator->getParents().size()) {
            targetHostOperatorMap[targetOperator] = {hostOperator, containmentType};
        }
        return targetHostOperatorMap;
    } else if (containmentType != ContainmentRelationship::NO_CONTAINMENT) {
        NES_TRACE("Target and host operators are contained. Host (leftSig): {}, Target (rightSig): {}, ContainmentType: {}",
                  hostOperator->toString(),
                  targetOperator->toString(),
                  hostOperator->toString(),
                  magic_enum::enum_name(containmentType));
        if (targetOperator->instanceOf<JoinLogicalOperatorNode>() && hostOperator->instanceOf<JoinLogicalOperatorNode>()) {
            return targetHostOperatorMap;
        }
        targetHostOperatorMap[targetOperator] = {hostOperator, containmentType};
        return targetHostOperatorMap;
    }
    NES_WARNING("Target and host operators are not matched.");
    return {};
}
}// namespace NES::Optimizer
