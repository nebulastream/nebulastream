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

#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/StatisticCollection/LogicalStatisticWindowOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticIdsExtractor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Statistic {
std::vector<StatisticId> StatisticIdsExtractor::extractStatisticIdsFromQueryId(Catalogs::Query::QueryCatalogPtr queryCatalog,
                                                                               const QueryId& queryId,
                                                                               const std::chrono::milliseconds& timeout) {
    // Waiting for the query to be in the RUNNING state before extracting the statistic ids
    const auto endTimestamp = std::chrono::system_clock::now() + timeout;
    const auto sleepTime = std::chrono::milliseconds(1000);
    while (std::chrono::system_clock::now() < endTimestamp) {
        const auto queryState = queryCatalog->getQueryState(queryId);
        NES_TRACE("Query {} is now in status {}", queryId, magic_enum::enum_name(queryState));
        switch (queryState) {
            case QueryState::MARKED_FOR_HARD_STOP:
            case QueryState::MARKED_FOR_SOFT_STOP:
            case QueryState::SOFT_STOP_COMPLETED:
            case QueryState::SOFT_STOP_TRIGGERED:
            case QueryState::STOPPED:
            case QueryState::RUNNING: {
                // We search for the queryPlan in the queryCatalog and then extract the statisticIds from the queryPlan
                const auto queryPlanCopy = queryCatalog->getCopyOfExecutedQueryPlan(queryId);
                return extractStatisticIdsFromQueryPlan(*queryPlanCopy);
            }
            case QueryState::FAILED: {
                NES_ERROR("Query failed to start. Expected: Running but found {}", magic_enum::enum_name(queryState));
                return {};
            }
            default: {
                NES_WARNING("Expected: Running but found {}", magic_enum::enum_name(queryState));
                break;
            }
        }

        std::this_thread::sleep_for(sleepTime);
    }

    NES_ERROR("Timeout while waiting for query {} to be in RUNNING state", queryId);
    return {};
}

std::vector<StatisticId> StatisticIdsExtractor::extractStatisticIdsFromQueryPlan(const QueryPlan& queryPlan) {
    std::vector<StatisticId> extractedStatisticIds;
    const auto allStatisticBuildOperator = queryPlan.getOperatorByType<LogicalStatisticWindowOperator>();

    /* We iterator over all LogicalStatisticWindowOperator in the queryPlan and store the statistic id of
     * the operator before in extractedStatisticIds
     */
    for (const auto& statisticBuildOperator : allStatisticBuildOperator) {
        // Children are the operator before
        auto operatorsToTrack = statisticBuildOperator->getChildren();

        /* We check if the operator before is a WatermarkAssignerLogicalOperator and if so, we have to add the
         * statistic ids of all of its children. Due to the fact that we might add a watermark assigner to each
         * logical query plan, if it contains a window operator, i.e., LogicalStatisticWindowOperator
         */
        for (const auto& childOp : operatorsToTrack) {
            if (childOp->instanceOf<const WatermarkAssignerLogicalOperator>()) {
                const auto& childrensChildren = childOp->getChildren();
                std::transform(childrensChildren.begin(),
                               childrensChildren.end(),
                               std::back_inserter(extractedStatisticIds),
                               [](const auto& op) {
                                   return op->template as<Operator>()->getStatisticId();
                               });
            } else {
                // If the operator before is not a WatermarkAssignerLogicalOperator, we add the statistic id of the operator
                extractedStatisticIds.emplace_back(childOp->template as<Operator>()->getStatisticId());
            }
        }
    }

    return extractedStatisticIds;
}

}// namespace NES::Statistic