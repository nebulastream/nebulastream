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
#include <Plans/Query/QueryPlan.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticIdsExtractor.hpp>

namespace NES::Statistic {
std::vector<StatisticId> StatisticIdsExtractor::extractStatisticIdsFromQueryId(Catalogs::Query::QueryCatalogPtr queryCatalog,
                                                                               const QueryId& queryId) {
    // We search for the queryPlan in the queryCatalog and then extract the statisticIds from the queryPlan
    const auto queryPlanCopy = queryCatalog->getCopyOfExecutedQueryPlan(queryId);
    return extractStatisticIdsFromQueryPlan(*queryPlanCopy);
}

std::vector<StatisticId> StatisticIdsExtractor::extractStatisticIdsFromQueryPlan(const QueryPlan& queryPlan) {
    std::vector<StatisticId> extractedStatisticIds;
    const auto allStatisticBuildOperator = queryPlan.getOperatorByType<LogicalStatisticWindowOperator>();

    // We iterator over all LogicalStatisticWindowOperator in the queryPlan and store the statistic id of
    // the operator before in extractedStatisticIds
    for (const auto& statisticBuildOperator : allStatisticBuildOperator) {
        // Children are the operator before
        const auto& buildOperatorChildren = statisticBuildOperator->getChildren();

        // Iterates over all children and extracts the statistic id for each child and writes the statistic id in extractedStatisticIds
        std::transform(buildOperatorChildren.begin(),
                       buildOperatorChildren.end(),
                       std::back_inserter(extractedStatisticIds),
                       [](const auto& childOp) {
                           return childOp->template as<Operator>()->getStatisticId();
                       });
    }

    return extractedStatisticIds;
}

}// namespace NES::Statistic