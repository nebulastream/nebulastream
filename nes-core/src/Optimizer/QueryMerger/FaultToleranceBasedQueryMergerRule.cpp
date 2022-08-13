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

#include "Components/NesWorker.hpp"
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/FaultToleranceBasedQueryMergerRule.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

FaultToleranceBasedQueryMergerRulePtr FaultToleranceBasedQueryMergerRule::create() {
    return std::make_shared<FaultToleranceBasedQueryMergerRule>();
}


bool FaultToleranceBasedQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {









    //NES_INFO("HashSignatureBasedCompleteQueryMergerRule: Applying Signature Based Equal Query Merger Rule to the Global Query Plan");
    auto queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    if (queryPlansToAdd.empty()) {
        NES_WARNING("HashSignatureBasedCompleteQueryMergerRule: Found no new query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }

    NES_DEBUG("HashSignatureBasedCompleteQueryMergerRule: Iterating over all Shared Query MetaData in the Global Query Plan");
    //Iterate over all query plans to identify the potential sharing opportunities
    for (auto& targetQueryPlan : queryPlansToAdd) {

        NES_INFO("Fault-Tolerance Type: " + toString(targetQueryPlan->getFaultToleranceType()));

        bool merged = false;
        auto hostSharedQueryPlans = globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());

        if(hostSharedQueryPlans.empty()){
            NES_WARNING("hostSharedQueryPlans empty")
        }

        for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {

            NES_INFO("SharedQueryPlan ID: " + std::to_string(hostSharedQueryPlan->getSharedQueryId()) + " | SharedQueryPlan to String: \n" + hostSharedQueryPlan->getQueryPlan()->toString());

            for(auto& sinkOperator : hostSharedQueryPlan->getSinkOperators()){

                NES_INFO("Sink Operator: " + sinkOperator->toString());

                NES_INFO("Sink Operator ID: " + std::to_string(hostSharedQueryPlan->getSinkOperators()[0]->getId()) );


            }

            /*

            auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();
            auto hostSignature = hostSharedQueryPlan->getHashBasedSignature();

            // Prepare a map of matching address and target sink global query nodes
            // if there are no matching global query nodes then the shared query metadata are not matched
            std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
            bool foundMatch = false;
            auto targetSinkOperators = targetQueryPlan->getSinkOperators();
            auto targetSignature = targetSinkOperators[0]->getHashBasedSignature();
            auto targetSignatureHashValue = targetSignature.begin()->first;
            auto targetSignatureStringValue = *targetSignature.begin()->second.begin();

            if (hostSignature.find(targetSignatureHashValue) != hostSignature.end()) {
                auto hostSignatureStringValues = hostSignature[targetSignatureHashValue];
                auto match = std::find_if(hostSignatureStringValues.begin(),
                                          hostSignatureStringValues.end(),
                                          [&](const std::string& hostSignatureStringValue) {
                                              return hostSignatureStringValue == targetSignatureStringValue;
                                          });
                if (match != hostSignatureStringValues.end()) {
                    targetToHostSinkOperatorMap[targetSinkOperators[0]] = hostSharedQueryPlan->getSinkOperators()[0];
                    foundMatch = true;
                }
            }

            if (foundMatch) {
                NES_TRACE("HashSignatureBasedCompleteQueryMergerRule: Merge target Shared metadata into address metadata");
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
                                NES_WARNING("HashSignatureBasedCompleteQueryMergerRule: Failed to add new parent");
                            }
                            hostSharedQueryPlan->addAdditionToChangeLog(hostChild->as<OperatorNode>(), targetSinkOperator);
                        }
                        targetSinkChild->removeParent(targetSinkOperator);
                    }
                    //Add target sink operator as root to the host query plan.
                    hostQueryPlan->addRootOperator(targetSinkOperator);
                }

                hostSharedQueryPlan->addQueryIdAndSinkOperators(targetQueryPlan);
                //Update the shared query meta data
                globalQueryPlan->updateSharedQueryPlan(hostSharedQueryPlan);
                // exit the for loop as we found a matching address shared query meta data
                merged = true;
                break;
            }*/
        }

        if (!merged) {
            NES_DEBUG("HashSignatureBasedCompleteQueryMergerRule: computing a new Shared Query Plan");
            globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
        }
    }
    //Remove all empty shared query metadata
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    return globalQueryPlan->clearQueryPlansToAdd();
}

void FaultToleranceBasedQueryMergerRule::calculateCheckpointing() {


}

}// namespace NES::Optimizer



/*for (auto queryPlan : globalQueryPlan->getQueryPlansToAdd()){

std::cout << "===================queryPlan : globalQueryPlan.getQueryPlantsToAdd()==========================\n";
std::cout << "getQuerySubPlanId()\n";
std::cout << queryPlan->getQuerySubPlanId();
std::cout << "\n";

for (auto on : queryPlan->getRootOperators()){

    std::cout << "===================on : queryPlan.getRootOperators()==========================\n";
    std::cout << "getId()\n";
    std::cout << on->getId();
    std::cout << "\n";
    std::cout << "=============================================\n";
    std::cout << "getOutputSchema()\n";
    std::cout << on->getOutputSchema()->toString();
    std::cout << "\n";
    std::cout << "=============================================\n";
    std::cout << "getAllRootNodes()[0]\n";
    std::cout << on->getAllRootNodes()[0];
    std::cout << "\n";
    std::cout << "=============================================\n";
    std::cout << "getAllLeafNodes()[0]\n";
    std::cout << on->getAllLeafNodes()[0];
    std::cout << "\n";
    std::cout << "=============================================\n";
    std::cout << "getNodeSourceLocation()\n";
    std::cout << on->getNodeSourceLocation();
    std::cout << "\n";
    std::cout << "=============================================\n";
    std::cout << "getChildren()[0]\n";
    std::cout << on->getChildren()[0];
    std::cout << "\n";
    std::cout << "=============================================\n";



}



}*/
