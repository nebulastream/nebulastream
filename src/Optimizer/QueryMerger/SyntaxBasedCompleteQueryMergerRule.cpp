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
#include <Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES::Optimizer {

SyntaxBasedCompleteQueryMergerRulePtr SyntaxBasedCompleteQueryMergerRule::create() {
    return std::make_shared<SyntaxBasedCompleteQueryMergerRule>();
}

bool SyntaxBasedCompleteQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    long qmTime = 0;
    auto startSI =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    NES_INFO("SyntaxBasedCompleteQueryMergerRule: Applying Syntax Based Equal Query Merger Rule to the Global Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("SyntaxBasedCompleteQueryMergerRule: Found no new query metadata in the global query plan."
                    " Skipping the Syntax Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("SyntaxBasedCompleteQueryMergerRule: Iterating over all GQMs in the Global Query Plan");
//    NES_ERROR("--------------------------------------- " << queryPlansToAdd.size());
    for (auto& targetQueryPlan : queryPlansToAdd) {
        bool merged = false;
        auto hostSharedQueryPlans = globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
//            counter++;
//            NES_ERROR("SCHEMA CHK Syntax " << counter);
            //TODO: we need to check how this will pan out when we will have more than 1 sink
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();

            //create a map of matching target to address operator id map
            std::map<uint64_t, uint64_t> targetToHostSinkOperatorMap;

            //Check if the target and address query plan are equal and return the target and address operator mappings
            if (areQueryPlansEqual(targetQueryPlan, hostQueryPlan, targetToHostSinkOperatorMap)) {
                auto startQM =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count();
                NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Merge target Shared metadata into address metadata");
                hostSharedQueryPlan->addQueryIdAndSinkOperators(targetQueryPlan);

                std::vector<OperatorNodePtr> hostSinkOperators = hostSharedQueryPlan->getSinkOperators();
                //Iterate over all target sink global query nodes and try to identify a matching address global query node
                // using the target address operator map
                for (auto& targetSinkOperator : targetQueryPlan->getSinkOperators()) {
                    uint64_t hostSinkOperatorId = targetToHostSinkOperatorMap[targetSinkOperator->getId()];

                    auto hostSinkOperator = std::find_if(hostSinkOperators.begin(),
                                                         hostSinkOperators.end(),
                                                         [hostSinkOperatorId](const OperatorNodePtr& hostOperator) {
                                                             return hostOperator->getId() == hostSinkOperatorId;
                                                         });

                    if (hostSinkOperator == hostSinkOperators.end()) {
                        NES_THROW_RUNTIME_ERROR("SyntaxBasedCompleteQueryMergerRule: Unexpected behaviour! matching host sink "
                                                "pair not found in the host query plan.");
                    }
                    //Remove all children of target sink global query node
                    auto targetSinkChildren = targetSinkOperator->getChildren();
                    auto hostSinkChildren = (*hostSinkOperator)->getChildren();
                    for (auto& targetSinkChild : targetSinkChildren) {
                        for (auto& hostSinkChild : hostSinkChildren) {
                            bool addedNewParent = hostSinkChild->addParent(targetSinkOperator);
                            if (!addedNewParent) {
                                NES_WARNING("SyntaxBasedCompleteQueryMergerRule: Failed to add new parent");
                            }
                            hostSharedQueryPlan->addAdditionToChangeLog(hostSinkChild->as<OperatorNode>(), targetSinkOperator);
                        }
                        targetSinkChild->removeParent(targetSinkOperator);
                    }
                    hostQueryPlan->addRootOperator(targetSinkOperator);
                }
                //Update the shared query meta data
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query meta data
                merged = true;
                auto endQM =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count();
                qmTime = endQM - startQM;
                NES_BM("Query-Merging-Time (micro)," << qmTime);
                break;
            }
        }
        if (!merged) {
            NES_DEBUG("SyntaxBasedCompleteQueryMergerRule: computing a new Shared Query Plan");
            auto startQM =
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
            auto endQM =
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
            qmTime = endQM - startQM;
            NES_BM("Query-Merging-Time (micro)," << qmTime);
        }
        auto endSI =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        NES_BM("Sharing-Identification-Time (micro)," << (endSI - startSI - qmTime));
        startSI =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeEmptySharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

bool SyntaxBasedCompleteQueryMergerRule::areQueryPlansEqual(const QueryPlanPtr& targetQueryPlan,
                                                            const QueryPlanPtr& hostQueryPlan,
                                                            std::map<uint64_t, uint64_t>& targetHostOperatorMap) {

    NES_DEBUG("SyntaxBasedCompleteQueryMergerRule: check if the target and address query plans are syntactically equal or not");
    std::vector<OperatorNodePtr> targetSourceOperators = targetQueryPlan->getLeafOperators();
    std::vector<OperatorNodePtr> hostSourceOperators = hostQueryPlan->getLeafOperators();

    if (targetSourceOperators.size() != hostSourceOperators.size()) {
        NES_WARNING("SyntaxBasedCompleteQueryMergerRule: Not matched as number of sources in target and address query plans are "
                    "different.");
        return false;
    }

    //Fetch the first source operator and find a corresponding matching source operator in the address source operator list
    auto& targetSourceOperator = targetSourceOperators[0];
    for (auto& hostSourceOperator : hostSourceOperators) {
        targetHostOperatorMap.clear();
        if (areOperatorEqual(targetSourceOperator, hostSourceOperator, targetHostOperatorMap)) {
            return true;
        }
    }
    return false;
}

bool SyntaxBasedCompleteQueryMergerRule::areOperatorEqual(const OperatorNodePtr& targetOperator,
                                                          const OperatorNodePtr& hostOperator,
                                                          std::map<uint64_t, uint64_t>& targetHostOperatorMap) {

    NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Check if the target and address operator are syntactically equal or not.");
    if (targetHostOperatorMap.find(targetOperator->getId()) != targetHostOperatorMap.end()) {
        if (targetHostOperatorMap[targetOperator->getId()] == hostOperator->getId()) {
            NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Already matched so skipping rest of the check.");
            return true;
        }
        NES_WARNING("SyntaxBasedCompleteQueryMergerRule: Not matched as target operator was matched to another number of "
                    "sources in target and address query plans are different.");
        return false;
    }

    if (targetOperator->instanceOf<SinkLogicalOperatorNode>() && hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
        NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Both address and target operators are of sink type.");
        targetHostOperatorMap[targetOperator->getId()] = hostOperator->getId();
        return true;
    }

    bool areParentsEqual = 0;
    bool areChildrenEqual = 0;

    NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Compare address and target operators.");
    if (targetOperator->equal(hostOperator)) {
        targetHostOperatorMap[targetOperator->getId()] = hostOperator->getId();

        NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Check if parents of target and address operators are equal.");
        for (const auto& targetParent : targetOperator->getParents()) {
            areParentsEqual = false;
            for (const auto& hostParent : hostOperator->getParents()) {
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

        NES_TRACE("SyntaxBasedCompleteQueryMergerRule: Check if children of target and address operators are equal.");
        for (const auto& targetChild : targetOperator->getChildren()) {
            areChildrenEqual = false;
            for (const auto& hostChild : hostOperator->getChildren()) {
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
    NES_WARNING("SyntaxBasedCompleteQueryMergerRule: Target and address operators are not matched.");
    return false;
}
}// namespace NES::Optimizer