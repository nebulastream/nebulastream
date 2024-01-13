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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYSUBPLANMETADATA_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYSUBPLANMETADATA_HPP_

#include <Identifiers.hpp>
#include <Util/QueryState.hpp>
#include <Util/QueryStateHistory.hpp>
#include <memory>

namespace NES {

class QuerySubPlanMetaData;
using QuerySubPlanMetaDataPtr = std::shared_ptr<QuerySubPlanMetaData>;

/**
 * This class stores metadata about a query sub plan.
 */
class QuerySubPlanMetaData {

  public:
    static QuerySubPlanMetaDataPtr create(DecomposedQueryPlanId querySubPlanId, QueryState subQueryStatus, uint64_t workerId);

    /**
     * Update the status of the qub query
     * @param queryStatus : new status
     */
    void updateStatus(QueryState queryStatus);

    /**
     * Update the meta information
     * @param metaInformation : information to update
     */
    void updateMetaInformation(const std::string& metaInformation);

    /**
     * Get status of query sub plan
     * @return status
     */
    QueryState getQuerySubPlanStatus();

    DecomposedQueryPlanId getQuerySubPlanId() const;

    QueryState getSubQueryStatus() const;

    uint64_t getWorkerId() const;

    const std::string& getMetaInformation() const;

    /** @brief Retrieve a timestamped history of query status changes. */
    const QueryStateHistory& getHistory() const;

    QuerySubPlanMetaData(DecomposedQueryPlanId querySubPlanId, QueryState subQueryStatus, uint64_t workerId);

  private:
    /**
     * Id of the subquery plan
     */
    DecomposedQueryPlanId querySubPlanId;

    /**
     * status of the sub query
     */
    QueryState subQueryStatus;

    /** @brief Stores a history of QueryState updates with their timestamp in milliseconds. */
    QueryStateHistory history;

    /**
     * worker id where the sub query is deployed
     */
    uint64_t workerId;

    /**
     * Any addition meta information e.g., failure reason
     */
    std::string metaInformation;
};
}// namespace NES

#endif // NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYSUBPLANMETADATA_HPP_
