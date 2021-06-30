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

#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/Utils/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
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
    std::vector<SharedQueryMetaDataPtr> allSharedQueryMetaData = globalQueryPlan->getAllSharedQueryMetaData();
    if (allSharedQueryMetaData.size() == 1) {
        NES_WARNING("Z3SignatureBasedPartialQueryMergerRule: Found only a single query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("Z3SignatureBasedPartialQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
    for (uint16_t i = 0; i < allSharedQueryMetaData.size() - 1; i++) {
        for (uint16_t j = i + 1; j < allSharedQueryMetaData.size(); j++) {

            auto targetSharedQueryMetaData = allSharedQueryMetaData[i];
            auto hostSharedQueryMetaData = allSharedQueryMetaData[j];

            if (targetSharedQueryMetaData->getSharedQueryId() == hostSharedQueryMetaData->getSharedQueryId()) {
                continue;
            }
            auto targetQueryPlan = targetSharedQueryMetaData->getQueryPlan();

            auto hostQueryPlan = hostSharedQueryMetaData->getQueryPlan();
            std::map<OperatorNodePtr, OperatorNodePtr> matchedTargetToHostOperatorMap;

            std::vector<NodePtr> matchedHostOperators;

            //Iterate over the target query plan and compare the operator signatures with the host query plan
            //When a match is found then store the matching operators in the matchedTargetToHostOperatorMap
            for (const auto& targetRootOperator : targetQueryPlan->getRootOperators()) {
                auto targetChildren = targetRootOperator->getChildren();
                std::deque<NodePtr> targetOperators = {targetChildren.begin(), targetChildren.end()};
                while (!targetOperators.empty()) {

                    bool foundMatch = false;
                    auto targetOperator = targetOperators.front()->as<LogicalOperatorNode>();
                    targetOperators.pop_front();

                    if (matchedTargetToHostOperatorMap.find(targetOperator) != matchedTargetToHostOperatorMap.end()) {
                        continue;
                    }

                    std::vector<NodePtr> visitedHostOperators;

                    for (const auto& hostRootOperator : hostQueryPlan->getRootOperators()) {
                        std::deque<NodePtr> hostOperators;

                        for(const auto& hostChildren:  hostRootOperator->getChildren()){
                            if(std::find(visitedHostOperators.begin(), visitedHostOperators.end(), hostChildren) == visitedHostOperators.end()){
                                hostOperators.push_back(hostChildren);
                            }
                        }

                        while (!hostOperators.empty()) {
                            auto hostOperator = hostOperators.front()->as<LogicalOperatorNode>();
                            visitedHostOperators.emplace_back(hostOperator);
                            hostOperators.pop_front();

                            //Skip matching if the host operator is already matched
                            if (std::find(matchedHostOperators.begin(), matchedHostOperators.end(), hostOperator)
                                != matchedHostOperators.end()) {
                                continue;
                            }

                            if (signatureEqualityUtil->checkEquality(targetOperator->getZ3Signature(),
                                                                     hostOperator->getZ3Signature())) {
                                matchedTargetToHostOperatorMap[targetOperator] = hostOperator;
                                matchedHostOperators.emplace_back(hostOperator);
                                foundMatch = true;
                                break;
                            }

                            //Check for the children operators if a host operator with matching is found on the host
                            for (const auto& hostChild : hostOperator->getChildren()) {
                                if(std::find(visitedHostOperators.begin(), visitedHostOperators.end(), hostChild) == visitedHostOperators.end()){
                                    hostOperators.push_back(hostChild);
                                }
                            }
                        }
                    }

                    if (foundMatch) {
                        continue;
                    }

                    //Check for the children operators if a host operator with matching is found on the host
                    for (const auto& targetChild : targetOperator->getChildren()) {
                        targetOperators.push_front(targetChild);
                    }
                }
            }

            /*while (*targetQueryPlanItr) {
                bool foundMatch = false;
                auto targetOperator = (*targetQueryPlanItr)->as<LogicalOperatorNode>();
                if (!targetOperator->instanceOf<SinkLogicalOperatorNode>()) {
                    auto hostQueryPlanItr = QueryPlanIterator(hostQueryPlan).begin();
                    while (*hostQueryPlanItr) {
                        auto hostOperator = (*hostQueryPlanItr)->as<LogicalOperatorNode>();
                        if (!hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
                            if (signatureEqualityUtil->checkEquality(targetOperator->getZ3Signature(),
                                                                     hostOperator->getZ3Signature())) {
                                matchedTargetToHostOperatorMap[targetOperator] = hostOperator;
                                foundMatch = true;
                                break;
                            }
                        }
                        ++hostQueryPlanItr;
                    }
                }
                if (foundMatch) {
                    break;
                }
                ++targetQueryPlanItr;
            }*/

            //Not all sinks found an equivalent entry in the target shared query metadata
            if (matchedTargetToHostOperatorMap.empty()) {
                NES_WARNING("Z3SignatureBasedPartialQueryMergerRule: Target and Host Shared Query MetaData are not equal");
                continue;
            }

            NES_TRACE("Z3SignatureBasedPartialQueryMergerRule: Merge target Shared metadata into address metadata");

            //Iterate over all matched pairs of operators and merge the query plan
            for (auto [targetOperator, hostOperator] : matchedTargetToHostOperatorMap) {
                for (const auto& targetParent : targetOperator->getParents()) {
                    bool addedNewParent = hostOperator->addParent(targetParent);
                    if (!addedNewParent) {
                        NES_WARNING("Z3SignatureBasedPartialQueryMergerRule: Failed to add new parent");
                    }
                    targetOperator->removeParent(targetParent);
                }
            }

            //Add all root operators from target query plan to host query plan
            for (const auto& targetRootOperator : targetQueryPlan->getRootOperators()) {
                hostQueryPlan->addRootOperator(targetRootOperator);
            }

            hostSharedQueryMetaData->addSharedQueryMetaData(targetSharedQueryMetaData);
            //Clear the target shared query metadata
            targetSharedQueryMetaData->clear();
            //Update the shared query meta data
            globalQueryPlan->updateSharedQueryMetadata(hostSharedQueryMetaData);
            // exit the for loop as we found a matching address shared query meta data
            break;
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeEmptySharedQueryMetaData();
    return true;
}
}// namespace NES::Optimizer
