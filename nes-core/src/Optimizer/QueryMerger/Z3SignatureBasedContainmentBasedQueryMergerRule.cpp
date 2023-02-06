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
#include <Optimizer/QueryMerger/Z3SignatureBasedContainmentBasedQueryMergerRule.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

Z3SignatureBasedContainmentBasedQueryMergerRule::Z3SignatureBasedContainmentBasedQueryMergerRule(
    z3::ContextPtr context,
    z3::ContextPtr contextSQPContainment) {
    signatureContainmentUtil = SignatureContainmentUtil::create(std::move(context), std::move(contextSQPContainment));
}

Z3SignatureBasedContainmentBasedQueryMergerRulePtr
Z3SignatureBasedContainmentBasedQueryMergerRule::create(z3::ContextPtr context, z3::ContextPtr contextSQPContainment) {
    return std::make_shared<Z3SignatureBasedContainmentBasedQueryMergerRule>(
        Z3SignatureBasedContainmentBasedQueryMergerRule(std::move(context), std::move(contextSQPContainment)));
}

bool Z3SignatureBasedContainmentBasedQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {
    NES_INFO(
        "Z3SignatureBasedContainmentBasedQueryMergerRule: Applying Signature Based Containment Query Merger Rule to the Global "
        "Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING(
            "Z3SignatureBasedContainmentBasedQueryMergerRule: Found only a single query metadata in the global query plan."
            " Skipping the Signature Based Containment Query Merger Rule.");
        return true;
    }

    NES_DEBUG(
        "Z3SignatureBasedContainmentBasedQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
    for (const auto& targetQueryPlan : queryPlansToAdd) {
        bool matched = false;
        auto hostSharedQueryPlans = globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {

            //Fetch the host query plan to merge
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();

            //create a map of matching target to address operator id map
            auto matchedTargetToHostOperatorMap = areQueryPlansContained(targetQueryPlan, hostQueryPlan);

            if (!matchedTargetToHostOperatorMap.empty()) {
                NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: Merge target Shared metadata into address metadata");
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
                for (auto [targetOperator, hostOperator] : matchedTargetToHostOperatorMap) {
                    for (const auto& targetParent : targetOperator->getParents()) {
                        bool addedNewParent = hostOperator->addParent(targetParent);
                        if (!addedNewParent) {
                            NES_WARNING("Z3SignatureBasedPartialQueryMergerBottomUpRule: Failed to add new parent");
                        }
                        hostSharedQueryPlan->addAdditionToChangeLog(hostOperator, targetParent->as<OperatorNode>());
                        targetOperator->removeParent(targetParent);
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
            NES_DEBUG("Z3SignatureBasedPartialQueryMergerBottomUpRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

std::map<LogicalOperatorNodePtr, LogicalOperatorNodePtr>
Z3SignatureBasedContainmentBasedQueryMergerRule::areQueryPlansContained(const QueryPlanPtr& targetQueryPlan,
                                                                        const QueryPlanPtr& hostQueryPlan) {

    std::map<LogicalOperatorNodePtr, LogicalOperatorNodePtr> targetHostOperatorMap;
    NES_DEBUG("Z3SignatureBasedContainmentBasedQueryMergerRule: check if the target and address query plans are syntactically "
              "contained or not");
    auto targetSourceOperators = targetQueryPlan->getSourceOperators();
    auto hostSourceOperators = hostQueryPlan->getSourceOperators();

    if (targetSourceOperators.size() != hostSourceOperators.size()) {
        NES_WARNING("Z3SignatureBasedContainmentBasedQueryMergerRule: Not matched as number of Sources in target and host query "
                    "plans are "
                    "different.");
        return {};
    }

    //Fetch the first source operator and find a corresponding matching source operator in the address source operator list
    for (auto& targetSourceOperator : targetSourceOperators) {
        for (auto& hostSourceOperator : hostSourceOperators) {
            auto matchedOperators = isOperatorContained(targetSourceOperator, hostSourceOperator);
            if (!matchedOperators.empty()) {
                targetHostOperatorMap.merge(matchedOperators);
                break;
            }
        }
    }
    return targetHostOperatorMap;
}

std::map<LogicalOperatorNodePtr, LogicalOperatorNodePtr>
Z3SignatureBasedContainmentBasedQueryMergerRule::isOperatorContained(const LogicalOperatorNodePtr& targetOperator,
                                                                     const LogicalOperatorNodePtr& hostOperator) {

    std::map<LogicalOperatorNodePtr, LogicalOperatorNodePtr> targetHostOperatorMap;
    if (targetOperator->instanceOf<SinkLogicalOperatorNode>() && hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
        NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: Both target and host operators are of sink type.");
        return {};
    }

    NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: Compare target and host operators.");
    auto resultMap = signatureContainmentUtil->checkContainment(targetOperator->getZ3Signature(), hostOperator->getZ3Signature());
    for (auto const& [key, val] : resultMap) {
        switch (key) {
            case PROJECTION:
                NES_TRACE("Projection");
                switch (val) {
                    case EQUALITY: NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: EQUALITY."); break;
                    case SQP_CONTAINED: NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: SQP_CONTAINED."); break;
                    case NEW_QUERY_CONTAINED: NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: NEW_QUERY_CONTAINED."); break;
                }
                break;
            case WINDOW:
                NES_TRACE("Window");
                switch (val) {
                    case EQUALITY: NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: EQUALITY."); break;
                    case SQP_CONTAINED: NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: SQP_CONTAINED."); break;
                    case NEW_QUERY_CONTAINED: NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: NEW_QUERY_CONTAINED."); break;
                }
                break;
            case FILTER:
                NES_TRACE("Filter");
                switch (val) {
                    case EQUALITY: NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: EQUALITY."); break;
                    case SQP_CONTAINED: NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: SQP_CONTAINED."); break;
                    case NEW_QUERY_CONTAINED: NES_TRACE("Z3SignatureBasedPartialQueryMergerBottomUpRule: NEW_QUERY_CONTAINED."); break;
                }
                break;
        }
    }
    //todo: remove after containment relationship is handled
    /*if () {
        NES_TRACE("HashSignatureBasedPartialQueryMergerRule: Check if parents of target and address operators are equal.");
        uint16_t matchCount = 0;
        for (const auto& targetParent : targetOperator->getParents()) {
            for (const auto& hostParent : hostOperator->getParents()) {
                auto matchedOperators =
                    isOperatorContained(targetParent->as<LogicalOperatorNode>(), hostParent->as<LogicalOperatorNode>());
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
    }*/
    NES_WARNING("Z3SignatureBasedPartialQueryMergerBottomUpRule: Target and host operators are not matched.");
    return {};
}

}// namespace NES::Optimizer
