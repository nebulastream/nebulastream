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

#include <Optimizer/Phases/StatisticIdInferencePhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <unordered_set>

namespace NES::Optimizer {

StatisticIdInferencePhasePtr StatisticIdInferencePhase::create() {
    return std::make_shared<StatisticIdInferencePhase>(StatisticIdInferencePhase());
}

StatisticIdInferencePhase::StatisticIdInferencePhase() {}

QueryPlanPtr StatisticIdInferencePhase::execute(QueryPlanPtr queryPlan) {
    performInference(queryPlan->getAllOperators());
    return queryPlan;
}

DecomposedQueryPlanPtr StatisticIdInferencePhase::execute(DecomposedQueryPlanPtr decomposedQueryPlan) {
    performInference(decomposedQueryPlan->getAllOperators());
    return decomposedQueryPlan;
}

void StatisticIdInferencePhase::performInference(std::unordered_set<OperatorPtr> allOperators) {
    for (auto& op : allOperators) {
        if (op->instanceOf<SourceLogicalOperatorPtr>()) {
            NES_DEBUG("Not setting the statisticId of a logical source, as we first have to expand the logical source"
                      "into physical source.");
            continue;
        }
        op->setStatisticId(getNextStatisticId());
    }
}

}// NES::Optimizer