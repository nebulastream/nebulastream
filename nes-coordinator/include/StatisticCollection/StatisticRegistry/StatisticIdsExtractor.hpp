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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_STATISTICIDSEXTRACTOR_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_STATISTICIDSEXTRACTOR_HPP_

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Identifiers.hpp>

namespace NES {
namespace Catalogs::Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}//namespace Catalogs::Query

namespace Statistic {

class StatisticIdsExtractor {
  public:
    static constexpr auto DEFAULT_TIMEOUT_QUERY_STARTED = std::chrono::milliseconds(10000);
    /**
     * @brief Extracts the statistic ids of the operator over which some statistic is being built
     * @param queryCatalog: In this catalog, we search for the queryPlan with the queryId and then extract statistic ids in this queryPlan
     * @param queryId: QueryId to extract the statistic ids from its queryPlan
     * @param timeout: Timeout for the query to be in the RUNNING state. Default is 10000ms
     */
    static std::vector<StatisticId> extractStatisticIdsFromQueryId(Catalogs::Query::QueryCatalogPtr queryCatalog,
                                                                   const QueryId& queryId,
                                                                   const std::chrono::milliseconds& timeout = DEFAULT_TIMEOUT_QUERY_STARTED);

    /**
     * @brief Extracts the statistic ids of the operator over which some statistic is being built
     * @param queryPlan: QueryPlan to extract the statistic ids from
     */
    static std::vector<StatisticId> extractStatisticIdsFromQueryPlan(const QueryPlan& queryPlan);
};

}// namespace Statistic
}// namespace NES

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICREGISTRY_STATISTICIDSEXTRACTOR_HPP_
