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
#include <z3++.h>

namespace NES::Optimizer {

Z3SignatureBasedPartialQueryMergerRule::Z3SignatureBasedPartialQueryMergerRule(z3::ContextPtr context) {
    signatureEqualityUtil = SignatureEqualityUtil::create(context);
}

Z3SignatureBasedPartialQueryMergerRule::~Z3SignatureBasedPartialQueryMergerRule() {
    NES_DEBUG("~Z3SignatureBasedPartialQueryMergerRule()");
}

Z3SignatureBasedPartialQueryMergerRulePtr Z3SignatureBasedPartialQueryMergerRule::create(z3::ContextPtr context) {
    return std::make_shared<Z3SignatureBasedPartialQueryMergerRule>(Z3SignatureBasedPartialQueryMergerRule(context));
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
            auto targetQueryPlanItr = QueryPlanIterator(targetQueryPlan).begin();

            auto hostQueryPlan = hostSharedQueryMetaData->getQueryPlan();

            std::map<OperatorNodePtr, OperatorNodePtr> targetToHostOperatorMap;
            while (*targetQueryPlanItr) {
                bool foundMatch = false;
                auto targetOperator = (*targetQueryPlanItr)->as<LogicalOperatorNode>();
                if (!targetOperator->instanceOf<SinkLogicalOperatorNode>()) {
                    auto hostQueryPlanItr = QueryPlanIterator(hostQueryPlan).begin();
                    while (*hostQueryPlanItr) {
                        auto hostOperator = (*hostQueryPlanItr)->as<LogicalOperatorNode>();
                        if (!hostOperator->instanceOf<SinkLogicalOperatorNode>()) {
                            if (signatureEqualityUtil->checkEquality(targetOperator->getSignature(),
                                                                     hostOperator->getSignature())) {
                                targetToHostOperatorMap[targetOperator] = hostOperator;
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
            }

            //Not all sinks found an equivalent entry in the target shared query metadata
            if (targetToHostOperatorMap.empty()) {
                NES_WARNING("Z3SignatureBasedPartialQueryMergerRule: Target and Host Shared Query MetaData are not equal");
                continue;
            }

            NES_TRACE("Z3SignatureBasedPartialQueryMergerRule: Merge target Shared metadata into address metadata");

            //Iterate over all matched pairs of operators and merge the query plan
            for (auto [targetOperator, hostOperator] : targetToHostOperatorMap) {
                for (auto targetParent : targetOperator->getParents()) {
                    bool addedNewParent = hostOperator->addParent(targetParent);
                    if (!addedNewParent) {
                        NES_WARNING("Z3SignatureBasedPartialQueryMergerRule: Failed to add new parent");
                    }
                    targetOperator->removeParent(targetParent);
                }
            }

            //Add all root operators from target query plan to host query plan
            for (auto targetRootOperator : targetQueryPlan->getRootOperators()) {
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
