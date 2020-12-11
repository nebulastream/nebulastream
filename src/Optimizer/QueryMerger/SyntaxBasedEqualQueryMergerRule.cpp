/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/QueryMerger/SyntaxBasedEqualQueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

SyntaxBasedEqualQueryMergerRule::SyntaxBasedEqualQueryMergerRule() : processedNodes() {}

SyntaxBasedEqualQueryMergerRulePtr SyntaxBasedEqualQueryMergerRule::create() { return std::make_shared<SyntaxBasedEqualQueryMergerRule>(SyntaxBasedEqualQueryMergerRule()); }

bool SyntaxBasedEqualQueryMergerRule::apply(const GlobalQueryPlanPtr& globalQueryPlan) {

    NES_INFO("SyntaxBasedEqualQueryMergerRule: Applying L0 Merging rule to the Global Query Plan");

    std::vector<SharedQueryMetaDataPtr> allGQMs = globalQueryPlan->getAllSharedQueryMetaData();
    if (allGQMs.size() == 1) {
        NES_WARNING("SyntaxBasedEqualQueryMergerRule: Found only a single query metadata in the global query plan."
                    " Skipping the L0 Query Merging.");
        return true;
    }

    NES_DEBUG("SyntaxBasedEqualQueryMergerRule: Iterating over all GQMs in the Global Query Plan");
    for (uint16_t i = 0; i < allGQMs.size() - 1; i++) {
        for (uint16_t j = i + 1; j < allGQMs.size(); j++) {

            auto hostGQM = allGQMs[i];
            auto targetGQM = allGQMs[j];

            if (targetGQM->getSharedQueryId() == hostGQM->getSharedQueryId()) {
                continue;
            }

            //TODO: we need to check how this will pan out when we will have more than 1 sink
            auto hostQueryPlan = hostGQM->getQueryPlan();
            auto targetQueryPlan = targetGQM->getQueryPlan();

            std::map<uint64_t, uint64_t> targetHostOperatorMap;
            if (areQueryPlansEqual(targetQueryPlan, hostQueryPlan, targetHostOperatorMap)) {

                std::set<GlobalQueryNodePtr> hostSinkGQNs = hostGQM->getSinkGlobalQueryNodes();
                for (auto targetSinkGQN : targetGQM->getSinkGlobalQueryNodes()) {
                    uint64_t hostSinkOperatorId = targetHostOperatorMap[targetSinkGQN->getOperator()->getId()];

                    auto found = std::find_if(hostSinkGQNs.begin(), hostSinkGQNs.end(),
                                              [hostSinkOperatorId](GlobalQueryNodePtr hostSinkGQN) {
                                                  return hostSinkGQN->getOperator()->getId() == hostSinkOperatorId;
                                              });

                    if (found == hostSinkGQNs.end()) {
                        NES_THROW_RUNTIME_ERROR("Unexpected behaviour");
                    }
                    targetSinkGQN->removeChildren();
                    for (auto hostChild : (*found)->getChildren()) {
                        hostChild->addParent(targetSinkGQN);
                    }
                }
                hostGQM->addSharedQueryMetaData(targetGQM);
                targetGQM->clear();
                globalQueryPlan->updateSharedQueryMetadata(hostGQM);
            }
        }
    }
    globalQueryPlan->removeEmptySharedQueryMetaData();
    return true;
}

bool SyntaxBasedEqualQueryMergerRule::areQueryPlansEqual(QueryPlanPtr targetQueryPlan, QueryPlanPtr hostQueryPlan,
                                           std::map<uint64_t, uint64_t>& targetHostOperatorMap) {

    NES_DEBUG("SyntaxBasedEqualQueryMergerRule: check if the target and host query plans are syntactically equal or not");
    std::vector<OperatorNodePtr> targetSourceOperators = targetQueryPlan->getLeafOperators();
    std::vector<OperatorNodePtr> hostSourceOperators = hostQueryPlan->getLeafOperators();

    if (targetSourceOperators.size() != hostSourceOperators.size()) {
        NES_WARNING("SyntaxBasedEqualQueryMergerRule: Not matched as number of sources in target and host query plans are different.");
        return false;
    }

    //Fetch the first source operator
    auto& targetSourceOperator = targetSourceOperators[0];
    for (auto& hostSourceOperator : hostSourceOperators) {
        targetHostOperatorMap.clear();
        if (areOperatorEqual(targetSourceOperator, hostSourceOperator, targetHostOperatorMap)) {
            return true;
        }
    }
    return false;
}

bool SyntaxBasedEqualQueryMergerRule::areOperatorEqual(OperatorNodePtr targetOperator, OperatorNodePtr hostOperator,
                                         std::map<uint64_t, uint64_t>& targetHostOperatorMap) {

    NES_TRACE("SyntaxBasedEqualQueryMergerRule: Check if the target and host operator are syntactically equal or not.");
    if (targetHostOperatorMap.find(targetOperator->getId()) != targetHostOperatorMap.end()) {
        if (targetHostOperatorMap[targetOperator->getId()] == hostOperator->getId()) {
            NES_TRACE("SyntaxBasedEqualQueryMergerRule: Already matched so skipping rest of the check.");
            return true;
        } else {
            NES_WARNING("SyntaxBasedEqualQueryMergerRule: Not matched as target operator was matched to another number of sources in target and host query plans are different.");
            return false;
        }
    }

    if (targetOperator->instanceOf<SinkLogicalOperatorNode>() && hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
        NES_TRACE("SyntaxBasedEqualQueryMergerRule: Both host and target operators are of sink type.");
        targetHostOperatorMap[targetOperator->getId()] = hostOperator->getId();
        return true;
    }

    NES_TRACE("SyntaxBasedEqualQueryMergerRule: Compare host and target operators.");
    if (targetOperator->equal(hostOperator)) {
        targetHostOperatorMap[targetOperator->getId()] = hostOperator->getId();

        NES_TRACE("SyntaxBasedEqualQueryMergerRule: Check if parents of target and host operators are equal.");
        bool areParentsEqual = false;
        for (auto& targetParent : targetOperator->getParents()) {
            for (auto& hostParent : hostOperator->getParents()) {
                if (areOperatorEqual(targetParent->as<OperatorNode>(), hostParent->as<OperatorNode>(), targetHostOperatorMap)) {
                    areParentsEqual = true;
                    break;
                }
            }
            if (!areParentsEqual) {
                targetHostOperatorMap.erase(targetOperator->getId());
                return false;
            }
        }

        NES_TRACE("SyntaxBasedEqualQueryMergerRule: Check if children of target and host operators are equal.");
        bool areChildrenEqual = false;
        for (auto& targetChild : targetOperator->getChildren()) {
            for (auto& hostChild : hostOperator->getChildren()) {
                if (areOperatorEqual(targetChild->as<OperatorNode>(), hostChild->as<OperatorNode>(), targetHostOperatorMap)) {
                    areChildrenEqual = true;
                    break;
                }
            }
            if (!areChildrenEqual) {
                targetHostOperatorMap.erase(targetOperator->getId());
                return false;
            }
        }
        return true;
    }
    NES_WARNING("SyntaxBasedEqualQueryMergerRule: Target and host operators are not matched.");
    return false;
}
}// namespace NES