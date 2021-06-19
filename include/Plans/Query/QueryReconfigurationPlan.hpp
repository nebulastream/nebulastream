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

#ifndef NES_INCLUDE_PLANS_QUERYRECONFIGURATIONPLAN_HPP_
#define NES_INCLUDE_PLANS_QUERYRECONFIGURATIONPLAN_HPP_
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QueryReconfigurationId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <memory>
#include <ostream>
#include <set>
#include <unordered_map>
#include <vector>

namespace NES {

class QueryReconfigurationPlan;
typedef std::shared_ptr<QueryReconfigurationPlan> QueryReconfigurationPlanPtr;

class QueryReconfigurationPlan {
  public:
    QueryReconfigurationPlan(const std::vector<QuerySubPlanId> querySubPlanIdsToStart,
                             const std::vector<QuerySubPlanId> querySubPlanIdsToStop,
                             const std::unordered_map<QuerySubPlanId, QuerySubPlanId> querySubPlanIdsToReplace);

    static QueryReconfigurationPlanPtr create(const std::vector<QuerySubPlanId> querySubPlanIdsToStart,
                                              const std::vector<QuerySubPlanId> querySubPlanIdsToStop,
                                              const std::unordered_map<QuerySubPlanId, QuerySubPlanId> querySubPlanIdsToReplace);

    /**
     * @brief Get vector of QuerySubPlans to stop
     * @return vector of QuerySubPlans to stop
     */
    const std::vector<QuerySubPlanId> getQuerySubPlanIdsToStop() const;

    /**
     * @brief Get vector of QuerySubPlans to start
     * @return vector of QuerySubPlans to start
     */
    const std::vector<QuerySubPlanId> getQuerySubPlanIdsToStart() const;

    /**
     * @brief Get Map of QuerySubPlans to replace, Key is old plan ID, value is new plan ID
     * @return Map of QuerySubPlans to replace
     */
    const std::unordered_map<QuerySubPlanId, QuerySubPlanId> getQuerySubPlanIdsToReplace() const;

    /**
     * Get query ID targeted via reconfiguration
     * @return queryId
     */
    QueryId getQueryId() const;

    /**
     * Get unique ID for reconfiguration
     * @return queryReconfigurationId
     */
    QueryReconfigurationId getId() const;

    /**
     * Converts to string to transfer over network, could not use protobuf as header is not included during query compilation.
     * @return: String representation of QueryReconfigurationPlan
     */
    const std::string serializeToString();

    /**
     * Set unique ID for reconfiguration
     * @param reconfigurationId
     */
    void setId(QueryReconfigurationId reconfigurationId);

    /**
     * Set vector of QuerySubPlans to stop
     * @param querySubPlanIdsToStop
     */
    void setQuerySubPlanIdsToStop(const std::vector<QuerySubPlanId> querySubPlanIdsToStop);

    /**
     * Set vector of QuerySubPlans to start
     * @param querySubPlanIdsToStart
     */
    void setQuerySubPlanIdsToStart(const std::vector<QuerySubPlanId> querySubPlanIdsToStart);

    /**
     * Set map of querySubPlanIdsToReplace
     * @param querySubPlanIdsToReplace
     */
    void setQuerySubPlanIdsToReplace(const std::unordered_map<QuerySubPlanId, QuerySubPlanId> querySubPlanIdsToReplace);

    /**
     * Set queryId
     * @param queryId
     */
    void setQueryId(QueryId queryId);

    /**
     * Construct QueryReconfigurationPlan from string
     * @param queryReconfigurationPlanStr: String representation of QueryReconfigurationPlan
     * @returns queryReconfigurationPlanPtr
     */
    static QueryReconfigurationPlanPtr deserializeFromString(const std::string queryReconfigurationPlanStr);

    QueryReconfigurationPlan();

    std::string toString();

  private:
    QueryReconfigurationId id;
    QueryId queryId;
    std::vector<QuerySubPlanId> querySubPlanIdsToStop;
    std::vector<QuerySubPlanId> querySubPlanIdsToStart;
    std::unordered_map<QuerySubPlanId, QuerySubPlanId> querySubPlanIdsToReplace;
};
}// namespace NES
#endif//NES_INCLUDE_PLANS_QUERYRECONFIGURATIONPLAN_HPP_
