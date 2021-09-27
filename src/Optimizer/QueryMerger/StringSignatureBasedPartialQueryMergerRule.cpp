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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/QueryMerger/StringSignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/Utils/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES::Optimizer {

StringSignatureBasedPartialQueryMergerRulePtr StringSignatureBasedPartialQueryMergerRule::create() {
    return std::make_shared<StringSignatureBasedPartialQueryMergerRule>();
}

bool StringSignatureBasedPartialQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    long qmTime = 0;
    auto startSI =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    NES_INFO(
        "StringSignatureBasedPartialQueryMergerRule: Applying Signature Based Equal Query Merger Rule to the Global Query Plan");
    auto queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("StringSignatureBasedPartialQueryMergerRule: Found no new query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("StringSignatureBasedPartialQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all query plans to identify the potential sharing opportunities
    for (auto& targetQueryPlan : queryPlansToAdd) {
        bool merged = false;
        auto hostSharedQueryPlans = globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {

            //TODO: we need to check how this will pan out when we will have more than 1 sink
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();

            //create a map of matching target to address operator id map
            auto matchedTargetToHostOperatorMap = areQueryPlansEqual(targetQueryPlan, hostQueryPlan);

            //Check if the target and address query plan are equal and return the target and address operator mappings
            if (!matchedTargetToHostOperatorMap.empty()) {
                NES_TRACE("SyntaxBasedPartialQueryMergerRule: Merge target Shared metadata into address metadata");
                auto startQM =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                hostSharedQueryPlan->addQueryIdAndSinkOperators(targetQueryPlan);

                // As we merge partially equivalent queries, we can potentially find matches across multiple operators.
                // As upstream matched operators are covered by downstream matched operators. We need to retain only the
                // downstream matched operator containing any upstream matched operator. This will prevent in computation
                // of inconsistent shared query plans.

                //Fetch all the matched target operators.
                std::vector<LogicalOperatorNodePtr> matchedTargetOperators;
                matchedTargetOperators.reserve(matchedTargetToHostOperatorMap.size());
                for (auto& mapEntry : matchedTargetToHostOperatorMap) {
                    matchedTargetOperators.emplace_back(mapEntry.first);
                }

                //Iterate over the target operators and remove the upstream operators covered by downstream matched operators
                for (uint64_t i = 0; i < matchedTargetOperators.size(); i++) {
                    for (uint64_t j = i; j < matchedTargetOperators.size(); j++) {
                        if (matchedTargetOperators[i]->containAsGrandChild(matchedTargetOperators[j])) {
                            matchedTargetToHostOperatorMap.erase(matchedTargetOperators[j]);
                        } else if (matchedTargetOperators[i]->containAsGrandParent(matchedTargetOperators[j])) {
                            matchedTargetToHostOperatorMap.erase(matchedTargetOperators[i]);
                            break;
                        }
                    }
                }

                //Iterate over all matched pairs of operators and merge the query plan
                for (auto [targetOperator, hostOperator] : matchedTargetToHostOperatorMap) {
                    for (const auto& targetParent : targetOperator->getParents()) {
                        bool addedNewParent = hostOperator->addParent(targetParent);
                        if (!addedNewParent) {
                            NES_WARNING("SyntaxBasedPartialQueryMergerRule: Failed to add new parent");
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
            NES_DEBUG("StringSignatureBasedPartialQueryMergerRule: computing a new Shared Query Plan");
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

std::map<LogicalOperatorNodePtr, LogicalOperatorNodePtr>
StringSignatureBasedPartialQueryMergerRule::areQueryPlansEqual(const QueryPlanPtr& targetQueryPlan,
                                                               const QueryPlanPtr& hostQueryPlan) {

    std::map<LogicalOperatorNodePtr, LogicalOperatorNodePtr> targetHostOperatorMap;
    NES_DEBUG(
        "StringSignatureBasedPartialQueryMergerRule: check if the target and address query plans are syntactically equal or not");
    auto targetSourceOperators = targetQueryPlan->getSourceOperators();
    auto hostSourceOperators = hostQueryPlan->getSourceOperators();

    if (targetSourceOperators.size() != hostSourceOperators.size()) {
        NES_WARNING(
            "StringSignatureBasedPartialQueryMergerRule: Not matched as number of sink in target and host query plans are "
            "different.");
        return {};
    }

    //Fetch the first source operator and find a corresponding matching source operator in the address source operator list
    for (auto& targetSourceOperator : targetSourceOperators) {
        for (auto& hostSourceOperator : hostSourceOperators) {
            auto matchedOperators = areOperatorEqual(targetSourceOperator, hostSourceOperator);
            if (!matchedOperators.empty()) {
                targetHostOperatorMap.merge(matchedOperators);
                break;
            }
        }
    }
    return targetHostOperatorMap;
}

std::map<LogicalOperatorNodePtr, LogicalOperatorNodePtr>
StringSignatureBasedPartialQueryMergerRule::areOperatorEqual(const LogicalOperatorNodePtr& targetOperator,
                                                             const LogicalOperatorNodePtr& hostOperator) {

    std::map<LogicalOperatorNodePtr, LogicalOperatorNodePtr> targetHostOperatorMap;
    if (targetOperator->instanceOf<SinkLogicalOperatorNode>() && hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
        NES_TRACE("StringSignatureBasedPartialQueryMergerRule: Both target and host operators are of sink type.");
        return {};
    }

    NES_TRACE("StringSignatureBasedPartialQueryMergerRule: Compare target and host operators.");
    const std::map<size_t, std::set<std::string>>& targetHashBased = targetOperator->getHashBasedSignature();
    if (targetHashBased == hostOperator->getHashBasedSignature()) {
        NES_TRACE("StringSignatureBasedPartialQueryMergerRule: Check if parents of target and address operators are equal.");
        uint16_t matchCount = 0;
        for (const auto& targetParent : targetOperator->getParents()) {
            for (const auto& hostParent : hostOperator->getParents()) {
                auto matchedOperators =
                    areOperatorEqual(targetParent->as<LogicalOperatorNode>(), hostParent->as<LogicalOperatorNode>());
                if (!matchedOperators.empty()) {
                    targetHostOperatorMap.merge(matchedOperators);
                    matchCount++;
                    break;
                }
            }
        }

        if (matchCount < targetOperator->getParents().size()) {
            targetHostOperatorMap[targetOperator] = hostOperator;
        }
        return targetHostOperatorMap;
    }
    NES_WARNING("StringSignatureBasedPartialQueryMergerRule: Target and host operators are not matched.");
    return {};
}

}// namespace NES::Optimizer
