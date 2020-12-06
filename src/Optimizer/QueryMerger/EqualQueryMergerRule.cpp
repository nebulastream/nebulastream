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
#include <Optimizer/QueryMerger/EqualQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Plans/Global/Query/GlobalQueryMetaData.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

EqualQueryMergerRule::EqualQueryMergerRule() {}

EqualQueryMergerRule::~EqualQueryMergerRule() { NES_DEBUG("~EqualQueryMergerRule()"); }

EqualQueryMergerRulePtr EqualQueryMergerRule::create() { return std::make_shared<EqualQueryMergerRule>(EqualQueryMergerRule()); }

bool EqualQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    NES_INFO("EqualQueryMergerRule: Applying L0 Merging rule to the Global Query Plan");

    std::vector<GlobalQueryMetaDataPtr> allGQMs = globalQueryPlan->getAllGlobalQueryMetaData();
    if (allGQMs.size() == 1) {
        NES_WARNING("EqualQueryMergerRule: Found only a single query metadata in the global query plan."
                    " Skipping the Equal Query Merging.");
        return true;
    }

    NES_DEBUG("EqualQueryMergerRule: Iterating over all GQMs in the Global Query Plan");
    for (uint16_t i = 0; i < allGQMs.size() - 1; i++) {
        for (uint16_t j = i + 1; j < allGQMs.size(); j++) {

            auto hostGQM = allGQMs[i];
            auto targetGQM = allGQMs[j];

            if (targetGQM->getGlobalQueryId() == hostGQM->getGlobalQueryId()) {
                continue;
            }

            auto hostQueryPlan = hostGQM->getQueryPlan();
            auto targetQueryPlan = targetGQM->getQueryPlan();

            std::map<uint64_t, uint64_t> targetHostOperatorMap;

            bool areEqual;
            for (auto hostSink : hostQueryPlan->getSinkOperators()) {
                areEqual = false;
                for (auto targetSink : targetQueryPlan->getSinkOperators()) {
                    if (hostSink->getSignature()->isEqual(targetSink->getSignature())) {
                        targetHostOperatorMap[targetSink->getId()] = hostSink->getId();
                        areEqual = true;
                        break;
                    }
                }
                if (!areEqual) {
                    NES_WARNING("There are not equal Target sink for Host sink "<< hostSink->toString());
                    break;
                }
            }

            if(!areEqual){
                NES_WARNING("Target and Host GQM are not equal");
                break;
            }

            std::set<GlobalQueryNodePtr> hostSinkGQNs = hostGQM->getSinkGlobalQueryNodes();
            for (auto targetSinkGQN : targetGQM->getSinkGlobalQueryNodes()) {
                uint64_t hostSinkOperatorId = targetHostOperatorMap[targetSinkGQN->getOperator()->getId()];

                auto found =
                    std::find_if(hostSinkGQNs.begin(), hostSinkGQNs.end(), [hostSinkOperatorId](GlobalQueryNodePtr hostSinkGQN) {
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
            hostGQM->addGlobalQueryMetaData(targetGQM);
            targetGQM->clear();
            globalQueryPlan->updateGlobalQueryMetadata(hostGQM);
        }
    }
    globalQueryPlan->removeEmptyMetaData();
    return true;
}

}// namespace NES::Optimizer
