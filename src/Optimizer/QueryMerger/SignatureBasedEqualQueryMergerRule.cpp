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
#include <Optimizer/QueryMerger/SignatureBasedEqualQueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

SignatureBasedEqualQueryMergerRule::SignatureBasedEqualQueryMergerRule() {}

SignatureBasedEqualQueryMergerRule::~SignatureBasedEqualQueryMergerRule() { NES_DEBUG("~EqualQueryMergerRule()"); }

SignatureBasedEqualQueryMergerRulePtr SignatureBasedEqualQueryMergerRule::create() {
    return std::make_shared<SignatureBasedEqualQueryMergerRule>(SignatureBasedEqualQueryMergerRule());
}

bool SignatureBasedEqualQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    NES_INFO("SignatureBasedEqualQueryMergerRule: Applying Signature Based Equal Query Merger Rule to the Global Query Plan");
    std::vector<SharedQueryMetaDataPtr> allSharedQueryMetaData = globalQueryPlan->getAllSharedQueryMetaData();
    if (allSharedQueryMetaData.size() == 1) {
        NES_WARNING("SignatureBasedEqualQueryMergerRule: Found only a single query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("SignatureBasedEqualQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
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

            // Prepare a map of matching host and target sink global query nodes
            // if there are no matching global query nodes then the shared query metadata are not matched
            std::map<uint64_t, uint64_t> targetHostSinkNodeMap;
            bool areEqual;
            for (auto hostSink : hostQueryPlan->getSinkOperators()) {
                areEqual = false;
                for (auto targetSink : targetQueryPlan->getSinkOperators()) {
                    //Check if the host and target sink signatures match each other
                    if (hostSink->getSignature()->isEqual(targetSink->getSignature())) {
                        targetHostSinkNodeMap[targetSink->getId()] = hostSink->getId();
                        areEqual = true;
                        break;
                    }
                }
                if (!areEqual) {
                    NES_WARNING("SignatureBasedEqualQueryMergerRule: There are not equal Target sink for Host sink "
                                << hostSink->toString());
                    break;
                }
            }

            //Not all sinks found an equivalent entry in the target shared query metadata
            if (!areEqual) {
                NES_WARNING("SignatureBasedEqualQueryMergerRule: Target and Host Shared Query MetaData are not equal");
                continue;
            }

            std::set<GlobalQueryNodePtr> hostSinkGQNs = hostSharedQueryMetaData->getSinkGlobalQueryNodes();
            //Iterate over all target sink global query node
            for (auto targetSinkGQN : targetSharedQueryMetaData->getSinkGlobalQueryNodes()) {
                //Check for the target sink global query node the corresponding host sink global query node id in the map
                uint64_t hostSinkOperatorId = targetHostSinkNodeMap[targetSinkGQN->getOperator()->getId()];

                // Find the host sink global query node with the matching id
                auto found =
                    std::find_if(hostSinkGQNs.begin(), hostSinkGQNs.end(), [hostSinkOperatorId](GlobalQueryNodePtr hostSinkGQN) {
                        return hostSinkGQN->getOperator()->getId() == hostSinkOperatorId;
                    });

                if (found == hostSinkGQNs.end()) {
                    NES_THROW_RUNTIME_ERROR("SignatureBasedEqualQueryMergerRule: Unexpected behaviour");
                }

                //Remove all children of target sink global query node
                targetSinkGQN->removeChildren();

                //Add children of matched host sink global query node to the target sink global query node
                for (auto hostChild : (*found)->getChildren()) {
                    hostChild->addParent(targetSinkGQN);
                }
            }

            NES_TRACE("SignatureBasedEqualQueryMergerRule: Merge target Shared metadata into host metadata");
            hostSharedQueryMetaData->addSharedQueryMetaData(targetSharedQueryMetaData);
            //Clear the target shared query metadata
            targetSharedQueryMetaData->clear();
            //Update the shared query meta data
            globalQueryPlan->updateSharedQueryMetadata(hostSharedQueryMetaData);
            // exit the for loop as we found a matching host shared query meta data
            break;
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeEmptySharedQueryMetaData();
    return true;
}

}// namespace NES::Optimizer
