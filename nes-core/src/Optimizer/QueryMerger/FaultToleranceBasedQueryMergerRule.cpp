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

#include <Optimizer/QueryMerger/FaultToleranceBasedQueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

FaultToleranceBasedQueryMergerRulePtr FaultToleranceBasedQueryMergerRule::create() {
    return std::make_shared<FaultToleranceBasedQueryMergerRule>();
}


bool FaultToleranceBasedQueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    for (int i = 0; i < 20; ++i){
        NES_INFO("XDXDXDXDXDXDXDXDXDXXDXDXD")


        for (auto sharedPlan : globalQueryPlan->getAllSharedQueryPlans()){
            for ( auto queryId : sharedPlan->getQueryIds()){
                NES_INFO(queryId);
                NES_INFO(queryId);
                NES_INFO(queryId);
                NES_INFO(queryId);
                NES_INFO(queryId);
                NES_INFO(queryId);
                NES_INFO(queryId);
            }
        }

    }




    auto queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();

    if (queryPlansToAdd.empty()) {
        NES_WARNING("HashSignatureBasedCompleteQueryMergerRule: Found no new query metadata in the global query plan."
                    " Skipping the Signature Based Equal Query Merger Rule.");
        return true;
    }
    for (auto& targetQueryPlan : queryPlansToAdd) {
        bool merged = false;
        auto hostSharedQueryPlans = globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());
        for (auto hostSharedPlan : hostSharedQueryPlans){
            for(auto queryId : hostSharedPlan->getQueryIds()){
                NES_INFO(queryId);
                NES_INFO(queryId);
                NES_INFO(queryId);
                NES_INFO(queryId);
                NES_INFO(queryId);
                NES_INFO(queryId);
                NES_INFO(queryId);
            }

        }
    }

    //For each query plan to add we compute a new SharedQueryPlan and add it to the global query plan
    /*auto queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();
    for (auto queryPlan : queryPlansToAdd) {
        globalQueryPlan->createNewSharedQueryPlan(queryPlan);
    }*/
    return globalQueryPlan->clearQueryPlansToAdd();
}

}// namespace NES::Optimizer
