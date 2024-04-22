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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_QUERY_SHAREDQUERYCATALOGENTRY_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_QUERY_SHAREDQUERYCATALOGENTRY_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Util/QueryState.hpp>
#include <Util/QueryStateHistory.hpp>
#include <map>
#include <set>
#include <memory>

namespace NES {

class DecomposedQueryPlanMetaData;
using DecomposedQueryPlanMetaDataPtr = std::shared_ptr<DecomposedQueryPlanMetaData>;

namespace Catalogs::Query {

/**
 * @brief class to handle the shared query plan entry in the query catalog
 */
class SharedQueryCatalogEntry {
  public:
    explicit SharedQueryCatalogEntry(SharedQueryId sharedQueryId, std::set<QueryId> queryIds, QueryState queryState);

    /**
     * @brief method to get the id of the query
     * @return query id
     */
    [[nodiscard]] SharedQueryId getSharedQueryId() const noexcept;

    /**
     * @brief method to get the status of the query
     * @return query status
     */
    [[nodiscard]] QueryState getQueryState() const;

    /**
     * @brief get contained query ids
     * @return vector of query ids
     */
    [[nodiscard]] std::set<QueryId> getContainedQueryIds();

    /**
     * @brief Add a new query id tot he catalog entry
     * @param queryId : id to add
     */
    void addQueryId(QueryId queryId);

    /**
     * @brief method to set the status of the query
     * @param query status
     */
    void setQueryState(QueryState queryStatus);

    /**
     * @brief Set additional message indicating the reason why query went down
     * @param terminationReason
     */
    void setTerminationReason(std::string terminationReason);

    /**
     * @brief Get the termination reason for the query if any
     * @return string representing termination reason
     */
    std::string getTerminationReason();

    /**
     * Add decomposed query plan metadata
     * @param decomposedQueryPlanId : the sub query plan id
     * @param decomposedQueryPlanVersion: the decomposed query plan version
     * @param workerId : the worker node on which the query is running
     * @param queryState : the state of the decomposed plan
     */
    void addDecomposedQueryPlanMetaData(DecomposedQueryPlanId decomposedQueryPlanId,
                                        DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                        WorkerId workerId,
                                        QueryState queryState);

    /**
     * Get sub query plan meta data
     * @param decomposedQueryPlanId : the decomposed query plan id
     */
    DecomposedQueryPlanMetaDataPtr getDecomposedQueryPlanMetaData(DecomposedQueryPlanId decomposedQueryPlanId);

    /**
     * Get all sub query plan mea data
     * @return vector of decomposed query plan meta data
     */
    std::vector<DecomposedQueryPlanMetaDataPtr> getAllDecomposedQueryPlanMetaData();

    /**
     * @brief Retrieve a timestamped history of query status changes.
     */
    const QueryStateHistory& getHistory() const;

  private:
    SharedQueryId sharedQueryId;
    std::set<QueryId> containedQueryIds;
    QueryState queryState;
    std::map<DecomposedQueryPlanId, DecomposedQueryPlanMetaDataPtr> decomposedQueryPlanMetaData;
    std::string terminationReason;
    QueryStateHistory history;
};
}// namespace Catalogs::Query
}// namespace NES

#endif // NES_CATALOGS_INCLUDE_CATALOGS_QUERY_SHAREDQUERYCATALOGENTRY_HPP_
