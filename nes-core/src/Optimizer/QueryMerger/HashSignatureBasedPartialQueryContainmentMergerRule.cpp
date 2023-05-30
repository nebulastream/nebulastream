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
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Optimizer/QueryMerger/HashSignatureBasedPartialQueryContainmentMergerRule.hpp>
#include <Optimizer/QuerySignatures/HashSignatureContainmentUtil.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
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

HashSignatureBasedPartialQueryContainmentMergerRule::HashSignatureBasedPartialQueryContainmentMergerRule(z3::ContextPtr context)
    : BaseQueryMergerRule() {
    signatureContainmentUtil = HashSignatureContainmentUtil::create(std::move(context));
}

HashSignatureBasedPartialQueryContainmentMergerRulePtr
HashSignatureBasedPartialQueryContainmentMergerRule::create(z3::ContextPtr context) {
    return std::make_shared<HashSignatureBasedPartialQueryContainmentMergerRule>(
        HashSignatureBasedPartialQueryContainmentMergerRule(std::move(context)));
}

bool HashSignatureBasedPartialQueryContainmentMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {
    auto queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING2("Found no new query metadata in the global query plan."
                     " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG2("Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all query plans to identify the potential sharing opportunities
    for (auto& targetQueryPlan : queryPlansToAdd) {
        bool merged = false;
        auto hostSharedQueryPlans = globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();
            //create a map of matching target to address operator id map
            auto matchedTargetToHostOperatorMap = areQueryPlansContained(targetQueryPlan, hostQueryPlan);

            //Check if the target and address query plan are equal and return the target and address operator mappings
            if (!matchedTargetToHostOperatorMap.empty()) {
                NES_TRACE2("Merge target Shared metadata into address metadata");
                hostSharedQueryPlan->addQueryIdAndSinkOperators(targetQueryPlan);

                // As we merge partially equivalent queryIdAndCatalogEntryMapping, we can potentially find matches across multiple operators.
                // As upstream matched operators are covered by downstream matched operators. We need to retain only the
                // downstream matched operator containing any upstream matched operator. This will prevent in computation
                // of inconsistent shared query plans.

                if (matchedTargetToHostOperatorMap.size() > 1) {
                    //Fetch all the matched target operators.
                    std::vector<LogicalOperatorNodePtr> matchedTargetOperators;
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
                for (auto [targetOp, hostOperatorAndRelationship] : matchedTargetToHostOperatorMap) {
                    LogicalOperatorNodePtr targetOperator = targetOp;
                    LogicalOperatorNodePtr hostOperator = std::get<0>(hostOperatorAndRelationship);
                    ContainmentType containmentType = std::get<1>(hostOperatorAndRelationship);
                    if (containmentType == ContainmentType::EQUALITY) {
                        NES_TRACE2("Output schema equality target {}", targetOperator->getOutputSchema()->toString());
                        NES_TRACE2("Output schema equality host {}", hostOperator->getOutputSchema()->toString());
                        for (const auto& targetParent : targetOperator->getParents()) {
                            NES_DEBUG2("Removing parent {} from {}", targetParent->toString(), targetOperator->toString());
                            bool addedNewParent = hostOperator->addParent(targetParent);
                            if (!addedNewParent) {
                                NES_WARNING2("Failed to add new parent");
                            }
                            hostSharedQueryPlan->addAdditionToChangeLog(hostOperator, targetParent->as<OperatorNode>());
                            targetOperator->removeParent(targetParent);
                        }
                    } else if (std::get<1>(hostOperatorAndRelationship) == ContainmentType::RIGHT_SIG_CONTAINED
                               && setTimestampForWindowContainment(hostOperator, targetOperator)) {
                        //if we're adding a window, we first need to obtain the watermark for that window
                        if (targetOperator->instanceOf<WindowOperatorNode>()) {
                            targetOperator = targetOperator->getChildren()[0]->as<LogicalOperatorNode>();
                        }
                        //obtain the child operation of the sink operator to merge the correct containment relationship
                        if (hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
                            //sink operator should only have one child
                            if (hostOperator->getChildren().size() != 1) {
                                NES_DEBUG2("Sink operator has more than one child");
                                continue;
                            }
                            hostOperator = hostOperator->getChildren()[0]->as<LogicalOperatorNode>();
                        }
                        NES_TRACE2("Adding parent {} to {}", targetOperator->toString(), hostOperator->toString());
                        targetOperator->removeChildren();
                        NES_TRACE2("Current host operator: {}", hostOperator->toString());
                        bool addedNewParent = hostOperator->addParent(targetOperator);
                        if (!addedNewParent) {
                            NES_WARNING2("Failed to add new parent");
                        }
                        hostSharedQueryPlan->addAdditionToChangeLog(std::get<0>(hostOperatorAndRelationship), targetOperator);
                        NES_TRACE2("New shared query plan: {}", hostSharedQueryPlan->getQueryPlan()->toString());
                    } else if (std::get<1>(hostOperatorAndRelationship) == ContainmentType::LEFT_SIG_CONTAINED
                               && setTimestampForWindowContainment(targetOperator, hostOperator)) {
                        //if we're adding a window, we first need to obtain the watermark for that window
                        if (hostOperator->instanceOf<WindowOperatorNode>()) {
                            hostOperator = hostOperator->getChildren()[0]->as<LogicalOperatorNode>();
                        }
                        //obtain the child operation of the sink operator to merge the correct containment relationship
                        if (targetOperator->instanceOf<SinkLogicalOperatorNode>()) {
                            //sink operator should only have one child
                            if (targetOperator->getChildren().size() != 1) {
                                NES_DEBUG2("Sink operator has more than one child");
                                continue;
                            }
                            targetOperator = targetOperator->getChildren()[0]->as<LogicalOperatorNode>();
                        }
                        NES_TRACE2("Adding parent {} to {}", hostOperator->toString(), targetOperator->toString());
                        hostOperator->removeChildren();
                        NES_TRACE2("Current host operator: {}", targetOperator->toString());
                        bool addedNewParent = targetOperator->addParent(hostOperator);
                        if (!addedNewParent) {
                            NES_WARNING2("Failed to add new parent");
                        }
                        hostSharedQueryPlan->addAdditionToChangeLog(targetOperator, hostOperator);
                        NES_DEBUG2("New shared query plan: {}", hostSharedQueryPlan->getQueryPlan()->toString());
                    }
                }

                //Add all root operators from target query plan to host query plan
                for (const auto& targetRootOperator : targetQueryPlan->getRootOperators()) {
                    hostQueryPlan->addRootOperator(targetRootOperator);
                }

                //Update the shared query meta data
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query meta data
                merged = true;
                break;
            }
        }

        if (!merged) {
            NES_DEBUG2("computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

bool HashSignatureBasedPartialQueryContainmentMergerRule::setTimestampForWindowContainment(
    const LogicalOperatorNodePtr& container,
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
            NES_TRACE2("Window containment possible.");
            return true;
        }
        NES_TRACE2("Window containment impossible.");
        return false;
    }
    return true;
}

std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentType>>
HashSignatureBasedPartialQueryContainmentMergerRule::areQueryPlansContained(const QueryPlanPtr& hostQueryPlan,
                                                                            const QueryPlanPtr& targetQueryPlan) {
    std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentType>> targetHostOperatorMap;
    NES_DEBUG2("check if the target and address query plans are syntactically equal or not");
    auto targetSourceOperators = targetQueryPlan->getSourceOperators();
    auto hostSourceOperators = hostQueryPlan->getSourceOperators();

    if (targetSourceOperators.size() != hostSourceOperators.size()) {
        NES_WARNING2("Not matched as number of sink in target and host query plans are "
                     "different.");
        return {};
    }

    //Fetch the first source operator and find a corresponding matching source operator in the address source operator list
    for (auto& targetSourceOperator : targetSourceOperators) {
        for (auto& hostSourceOperator : hostSourceOperators) {
            auto matchedOperators = areOperatorsContained(targetSourceOperator, hostSourceOperator);
            if (!matchedOperators.empty()) {
                targetHostOperatorMap.merge(matchedOperators);
                break;
            }
        }
    }
    return targetHostOperatorMap;
}

std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentType>>
HashSignatureBasedPartialQueryContainmentMergerRule::areOperatorsContained(const LogicalOperatorNodePtr& hostOperator,
                                                                           const LogicalOperatorNodePtr& targetOperator) {
    std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentType>> targetHostOperatorMap;
    if (targetOperator->instanceOf<SinkLogicalOperatorNode>() && hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
        NES_TRACE2("Both target and host operators are of sink type.");
        return {};
    }

    NES_TRACE2("HashSignatureBasedPartialQueryMergerRule: Compare target and host operators.");
    auto containmentType = signatureContainmentUtil->checkContainment(hostOperator, targetOperator);
    if (containmentType == ContainmentType::EQUALITY) {
        NES_TRACE2("Check if parents of target and address operators are equal.");
        uint16_t matchCount = 0;
        for (const auto& targetParent : targetOperator->getParents()) {
            for (const auto& hostParent : hostOperator->getParents()) {
                auto matchedOperators =
                    areOperatorsContained(targetParent->as<LogicalOperatorNode>(), hostParent->as<LogicalOperatorNode>());
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
        NES_DEBUG2("Target and host operators are contained. Target: {}, Host: {}, ContainmentType: {}",
                   targetOperator->toString(),
                   hostOperator->toString(),
                   magic_enum::enum_name(containmentType));
        if (targetOperator->instanceOf<JoinLogicalOperatorNode>() && hostOperator->instanceOf<JoinLogicalOperatorNode>()) {
            return targetHostOperatorMap;
        }
        targetHostOperatorMap[targetOperator] = {hostOperator, containmentType};
        return targetHostOperatorMap;
    }
    NES_WARNING2("Target and host operators are not matched.");
    return {};
}
}// namespace NES::Optimizer