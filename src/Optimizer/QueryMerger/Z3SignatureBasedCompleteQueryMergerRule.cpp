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
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/Utils/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

Z3SignatureBasedCompleteQueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule(z3::ContextPtr context) : BaseQueryMergerRule() {
    signatureEqualityUtil = SignatureEqualityUtil::create(std::move(context));
}

Z3SignatureBasedCompleteQueryMergerRulePtr Z3SignatureBasedCompleteQueryMergerRule::create(z3::ContextPtr context) {
    return std::make_shared<Z3SignatureBasedCompleteQueryMergerRule>(Z3SignatureBasedCompleteQueryMergerRule(std::move(context)));
}

bool Z3SignatureBasedCompleteQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    long qmTime = 0;
    auto startSI =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    NES_INFO(
        "Z3SignatureBasedCompleteQueryMergerRule: Applying Signature Based Equal Query Merger Rule to the Global Query Plan");
    std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("Z3SignatureBasedCompleteQueryMergerRule: Found no new query plan to add in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("Z3SignatureBasedCompleteQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
//    NES_ERROR("--------------------------------------- " << queryPlansToAdd.size());
    for (const auto& targetQueryPlan : queryPlansToAdd) {

        bool matched = false;
        auto hostSharedQueryPlans = globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());
        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
//            counter++;
//            NES_ERROR("CHK Z3 " << counter);
            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();
            // Prepare a map of matching address and target sink global query nodes
            // if there are no matching global query nodes then the shared query metadata are not matched
            std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
            auto targetSink = targetQueryPlan->getSinkOperators()[0];
            auto hostSink = hostQueryPlan->getSinkOperators()[0];
            bool foundMatch = false;

            //Check if the host and target sink operator signatures match each other
            if (signatureEqualityUtil->checkEquality(hostSink->getZ3Signature(), targetSink->getZ3Signature())) {
                targetToHostSinkOperatorMap[targetSink] = hostSink;
                foundMatch = true;
            }

            //Not all sinks found an equivalent entry in the target shared query metadata
            if (foundMatch) {
                auto startQM =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                NES_TRACE("Z3SignatureBasedCompleteQueryMergerRule: Merge target Shared metadata into address metadata");

                hostSharedQueryPlan->addQueryIdAndSinkOperators(targetQueryPlan);
                //Iterate over all matched pairs of sink operators and merge the query plan
                for (auto& [targetSinkOperator, hostSinkOperator] : targetToHostSinkOperatorMap) {
                    //Get children of target and host sink operators
                    auto targetSinkChildren = targetSinkOperator->getChildren();
                    auto hostSinkChildren = hostSinkOperator->getChildren();
                    //Iterate over target children operators and migrate their parents to the host children operators.
                    // Once done, remove the target parent from the target children.
                    for (auto& targetSinkChild : targetSinkChildren) {
                        for (auto& hostChild : hostSinkChildren) {
                            bool addedNewParent = hostChild->addParent(targetSinkOperator);
                            if (!addedNewParent) {
                                NES_WARNING("Z3SignatureBasedCompleteQueryMergerRule: Failed to add new parent");
                            }
                            hostSharedQueryPlan->addAdditionToChangeLog(hostChild->as<OperatorNode>(), targetSinkOperator);
                        }
                        targetSinkChild->removeParent(targetSinkOperator);
                    }
                    //Add target sink operator as root to the host query plan.
                    hostQueryPlan->addRootOperator(targetSinkOperator);
                }
                //Update the shared query meta data
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query meta data
                matched = true;
                auto endQM =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                qmTime = endQM - startQM;
                NES_BM("Query-Merging-Time (micro)," << qmTime);
                break;
            }
        }

        if (!matched) {
            NES_DEBUG("Z3SignatureBasedCompleteQueryMergerRule: computing a new Shared Query Plan");
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

}// namespace NES::Optimizer
