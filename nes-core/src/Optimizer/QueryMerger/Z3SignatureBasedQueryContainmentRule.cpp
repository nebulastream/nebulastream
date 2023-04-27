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
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedQueryContainmentRule.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <utility>

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
    for (const auto& targetQueryPlan : queryPlansToAdd) {
        bool matched = false;
        auto hostSharedQueryPlans = globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());
        NES_DEBUG2("HostSharedQueryPlans empty? {}", hostSharedQueryPlans.empty());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
            //Fetch the host query plan to merge
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();
            NES_DEBUG2("HostSharedQueryPlan: {}", hostQueryPlan->toString());
            NES_DEBUG2("TargetQueryPlan: {}", targetQueryPlan->toString());
            //create a map of matching target to address operator id map
            auto matchedTargetToHostOperatorMap = areQueryPlansContained(hostQueryPlan, targetQueryPlan);

            NES_DEBUG2("matchedTargetToHostOperatorMap empty? {}", matchedTargetToHostOperatorMap.empty());
            if (!matchedTargetToHostOperatorMap.empty()) {
                hostSharedQueryPlan->addQueryIdAndSinkOperators(targetQueryPlan);
                if (matchedTargetToHostOperatorMap.size() > 1) {
                    //Fetch all the matched target operators.
                    std::vector<LogicalOperatorNodePtr> matchedTargetOperators;
                    matchedTargetOperators.reserve(matchedTargetToHostOperatorMap.size());
                    for (auto& [leftQueryOperators, rightQueryOperatorsAndRelationship] : matchedTargetToHostOperatorMap) {
                        if (std::get<1>(rightQueryOperatorsAndRelationship) == ContainmentType::EQUALITY) {
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
                for (auto [targetOperator, hostOperatorAndRelationship] : matchedTargetToHostOperatorMap) {
                    if (std::get<1>(hostOperatorAndRelationship) == ContainmentType::EQUALITY) {
                        for (const auto& targetParent : targetOperator->getParents()) {
                            NES_DEBUG2("Removing parent {} from {}", targetParent->toString(), targetOperator->toString());
                            bool addedNewParent = std::get<0>(hostOperatorAndRelationship)->addParent(targetParent);
                            if (!addedNewParent) {
                                NES_WARNING2("Failed to add new parent");
                            }
                            hostSharedQueryPlan->addAdditionToChangeLog(std::get<0>(hostOperatorAndRelationship),
                                                                        targetParent->as<OperatorNode>());
                            targetOperator->removeParent(targetParent);
                        }
                    } else if (std::get<1>(hostOperatorAndRelationship) == ContainmentType::RIGHT_SIG_CONTAINED
                               && checkWindowContainmentPossible(targetOperator, std::get<0>(hostOperatorAndRelationship))) {
                            NES_DEBUG2("Adding parent {} to {}",
                                       targetOperator->toString(),
                                       std::get<0>(hostOperatorAndRelationship)->toString());
                            targetOperator->removeChildren();
                            NES_DEBUG2("Current host operator: {}", std::get<0>(hostOperatorAndRelationship)->toString());
                            bool addedNewParent = std::get<0>(hostOperatorAndRelationship)->addParent(targetOperator);
                            if (!addedNewParent) {
                                NES_WARNING2("Failed to add new parent");
                            }
                            hostSharedQueryPlan->addAdditionToChangeLog(std::get<0>(hostOperatorAndRelationship), targetOperator);
                            NES_DEBUG2("New shared query plan: {}", hostSharedQueryPlan->getQueryPlan()->toString());
                    } else if (std::get<1>(hostOperatorAndRelationship) == ContainmentType::LEFT_SIG_CONTAINED
                               && checkWindowContainmentPossible(targetOperator, std::get<0>(hostOperatorAndRelationship))) {
                        NES_DEBUG2("Adding parent {} to {}",
                                   std::get<0>(hostOperatorAndRelationship)->toString(),
                                   targetOperator->toString());
                        std::get<0>(hostOperatorAndRelationship)->removeChildren();
                        NES_DEBUG2("Current host operator: {}", targetOperator->toString());
                        bool addedNewParent = targetOperator->addParent(std::get<0>(hostOperatorAndRelationship));
                        if (!addedNewParent) {
                            NES_WARNING2("Failed to add new parent");
                        }
                        hostSharedQueryPlan->addAdditionToChangeLog(targetOperator, std::get<0>(hostOperatorAndRelationship));
                        NES_DEBUG2("New shared query plan: {}", hostSharedQueryPlan->getQueryPlan()->toString());
                    }
                }
                //Add all root operators from target query plan to host query plan
                for (const auto& targetRootOperator : targetQueryPlan->getRootOperators()) {
                    NES_DEBUG2("Adding root operator {} to host query plan {}",
                               targetRootOperator->toString(),
                               hostQueryPlan->toString());
                    hostQueryPlan->addRootOperator(targetRootOperator);
                    NES_DEBUG2("Adding root operator {} to host query plan {}",
                               targetRootOperator->toString(),
                               hostQueryPlan->toString());
                }

                //Update the shared query metadata
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query metadata
                matched = true;
                break;
            }
        }
        if (!matched) {
            NES_DEBUG2("Z3SignatureBasedQueryContainmentRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

bool Z3SignatureBasedQueryContainmentRule::checkWindowContainmentPossible(const LogicalOperatorNodePtr& targetOperator,
                                                                          const LogicalOperatorNodePtr& hostOperator) const {
    if (targetOperator->instanceOf<WindowOperatorNode>()) {
        auto windowDefinition = targetOperator->as<WindowOperatorNode>()->getWindowDefinition();
        if (windowDefinition->getWindowType()->isTimeBasedWindowType()) {
            auto targetTimeBasedWindow =
                windowDefinition->getWindowType()->asTimeBasedWindowType(windowDefinition->getWindowType());
            auto hostWindowType = hostOperator->as<WindowOperatorNode>()->getWindowDefinition()->getWindowType();
            auto hostTimeBasedWindow =
                hostOperator->as<WindowOperatorNode>()->getWindowDefinition()->getWindowType()->asTimeBasedWindowType(
                    hostWindowType);
            if (targetTimeBasedWindow->getSize().getTime() % hostTimeBasedWindow->getSize().getTime() == 0
                && targetTimeBasedWindow->getSlide().getTime() % hostTimeBasedWindow->getSlide().getTime() == 0) {
                return true;
            }
        }
        return false;
    }
    return true;
}

std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentType>>
Z3SignatureBasedQueryContainmentRule::areQueryPlansContained(const QueryPlanPtr& hostQueryPlan,
                                                             const QueryPlanPtr& targetQueryPlan) {

    std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentType>> targetHostOperatorMap;
    NES_DEBUG2("Check if the target and address query plans are syntactically "
               "contained.");
    auto targetSourceOperators = targetQueryPlan->getSourceOperators();
    auto hostSourceOperators = hostQueryPlan->getSourceOperators();

    if (targetSourceOperators.size() != hostSourceOperators.size()) {
        NES_WARNING2("Not matched as number of Sources in target and host query plans are "
                     "different.");
        return {};
    }

    //Fetch the first source operator and find a corresponding matching source operator in the address source operator list
    for (auto& targetSourceOperator : targetSourceOperators) {
        NES_DEBUG2("TargetSourceOperator: {}", targetSourceOperator->toString());
        for (auto& hostSourceOperator : hostSourceOperators) {
            NES_DEBUG2("HostSourceOperator: {}", hostSourceOperator->toString());
            auto matchedOperators = areOperatorsContained(hostSourceOperator, targetSourceOperator);
            if (!matchedOperators.empty()) {
                targetHostOperatorMap.merge(matchedOperators);
                break;
            }
        }
    }
    return targetHostOperatorMap;
}

std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentType>>
Z3SignatureBasedQueryContainmentRule::areOperatorsContained(const LogicalOperatorNodePtr& hostOperator,
                                                            const LogicalOperatorNodePtr& targetOperator) {

    std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentType>> targetHostOperatorMap;
    if (targetOperator->instanceOf<SinkLogicalOperatorNode>() && hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
        NES_DEBUG2("Both target and host operators are of sink type.");
        return {};
    }

    NES_DEBUG2("Compare target and host operators.");
    auto containmentType =
        signatureContainmentUtil->checkContainment(hostOperator->getZ3Signature(), targetOperator->getZ3Signature());
    if (containmentType == ContainmentType::EQUALITY) {
        NES_DEBUG2("Check containment relationship for parents of target operator.");
        uint16_t matchCount = 0;
        for (const auto& targetParent : targetOperator->getParents()) {
            NES_DEBUG2("TargetParent: {}", targetParent->toString());
            for (const auto& hostParent : hostOperator->getParents()) {
                NES_DEBUG2("HostParent: {}", hostParent->toString());
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
    } else if (containmentType != ContainmentType::NO_CONTAINMENT) {
        NES_DEBUG2("Target and host operators are contained. Target: {}",
                   targetOperator->toString(),
                   " Host: ",
                   hostOperator->toString());
        targetHostOperatorMap[targetOperator] = {hostOperator, containmentType};
        return targetHostOperatorMap;
    }
    NES_WARNING2("Target and host operators are not matched.");
    return {};
}
}// namespace NES::Optimizer
