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
#include <Optimizer/QueryMerger/SignatureBasedCompleteQueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

SignatureBasedCompleteQueryMergerRule::SignatureBasedCompleteQueryMergerRule() {}

SignatureBasedCompleteQueryMergerRule::~SignatureBasedCompleteQueryMergerRule() { NES_DEBUG("~EqualQueryMergerRule()"); }

SignatureBasedCompleteQueryMergerRulePtr SignatureBasedCompleteQueryMergerRule::create() {
    return std::make_shared<SignatureBasedCompleteQueryMergerRule>(SignatureBasedCompleteQueryMergerRule());
}

bool SignatureBasedCompleteQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    NES_INFO("SignatureBasedCompleteQueryMergerRule: Applying Signature Based Equal Query Merger Rule to the Global Query Plan");
    std::vector<SharedQueryMetaDataPtr> allSharedQueryMetaData = globalQueryPlan->getAllSharedQueryMetaData();
    if (allSharedQueryMetaData.size() == 1) {
        NES_WARNING("SignatureBasedCompleteQueryMergerRule: Found only a single query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("SignatureBasedCompleteQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all shared query metadata to identify equal shared metadata
    for (uint16_t i = 0; i < allSharedQueryMetaData.size() - 1; i++) {
        for (uint16_t j = i + 1; j < allSharedQueryMetaData.size(); j++) {

            auto targetSharedQueryMetaData = allSharedQueryMetaData[i];
            auto hostSharedQueryMetaData = allSharedQueryMetaData[j];

            if (targetSharedQueryMetaData->getSharedQueryId() == hostSharedQueryMetaData->getSharedQueryId()) {
                continue;
            }

            auto hostQueryPlan = hostSharedQueryMetaData->getQueryPlan();
            auto targetQueryPlan = targetSharedQueryMetaData->getQueryPlan();

            // Prepare a map of matching address and target sink global query nodes
            // if there are no matching global query nodes then the shared query metadata are not matched
            std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
            for (auto hostSink : hostQueryPlan->getSinkOperators()) {
                bool foundMatch = false;
                for (auto targetSink : targetQueryPlan->getSinkOperators()) {
                    //Check if the address and target sink operator signatures match each other
                    if (hostSink->getSignature()->isEqual(targetSink->getSignature())) {
                        targetToHostSinkOperatorMap[targetSink] = hostSink;
                        foundMatch = true;
                        break;
                    }
                }
                if (!foundMatch) {
                    NES_WARNING("SignatureBasedCompleteQueryMergerRule: There are not equal Target sink for Host sink "
                                << hostSink->toString());
                    targetToHostSinkOperatorMap.clear();
                    break;
                }
            }

            //Not all sinks found an equivalent entry in the target shared query metadata
            if (targetToHostSinkOperatorMap.empty()) {
                NES_WARNING("SignatureBasedCompleteQueryMergerRule: Target and Host Shared Query MetaData are not equal");
                continue;
            }

            NES_TRACE("SignatureBasedCompleteQueryMergerRule: Merge target Shared metadata into address metadata");

            //Iterate over all matched pairs of sink operators and merge the query plan
            for (auto& [targetSinkOperator, hostSinkOperator] : targetToHostSinkOperatorMap) {
                auto targetSinkChildren = targetSinkOperator->getChildren();
                auto hostSinkChildren = hostSinkOperator->getChildren();
                for (auto childToMerge : targetSinkChildren) {
                    for (auto hostChild : hostSinkChildren) {
                        bool addedNewParent = hostChild->addParent(targetSinkOperator);
                        if (!addedNewParent) {
                            NES_WARNING("SignatureBasedCompleteQueryMergerRule: Failed to add new parent");
                        }
                    }
                    childToMerge->removeParent(targetSinkOperator);
                }
                hostQueryPlan->addRootOperator(targetSinkOperator);
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
